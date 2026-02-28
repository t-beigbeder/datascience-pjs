import argparse
import bz2
import hashlib
import json
import logging
import queue
import sys
import threading
import typing
import zlib

from ddpbasics import adapters, cache, s3utils
from ddpestorec import main
import grpc
import mwxml
from sfegrpc import estore_pb2, estore_pb2_grpc, stfl_pb2, stfl_pb2_grpc

from ..pamutils import (
    MW_PAM_CATEGORY,
    MW_FR_DATE,
    MW_FR_WIKI,
    MW_PAM_PACK_URLS_QUEUE,
    MW_PAM_RESULTS_QUEUE,
    PAM_PACK_CACHE_CAT,
    PamPackStats,
    new_PamPackStats,
    PamError,
    get_bool,
    pam_base_for,
)

logger = logging.getLogger("mw.pam.etl")


class _Args(typing.TypedDict):
    map_id: str
    bucket: str
    profile_name: str | None
    wiki_name: str
    wiki_date: str
    check_sh: bool
    pam_per_pack_num: int
    check_mode: bool


class _EntsProdCtx(typing.TypedDict):
    eq: queue.Queue
    args: _Args
    es_stub: estore_pb2_grpc.EstoreStub
    fl_stub: stfl_pb2_grpc.FlowStub
    ppss: list[PamPackStats]


class _EntsGenCtx(typing.TypedDict):
    eq: queue.Queue
    args: _Args
    es_stub: estore_pb2_grpc.EstoreStub
    session_uuid: typing.NotRequired[str]
    errors_num: int


class _PamData(typing.TypedDict):
    key: str
    title: str
    updated: str
    additional_content: bytes


def _stream_reader(
    bucket: str, profile_name: str | None, url: str
) -> tuple[typing.Any, bool]:
    object_path = cache.get_s3_cache_path_for(url, PAM_PACK_CACHE_CAT, ".bz2")
    exists = s3utils.exists(bucket, object_path, profile_name)
    return (
        adapters.stream_reader(
            cache.s3_cache_streamer(
                bucket,
                profile_name,
                url,
                PAM_PACK_CACHE_CAT,
                ".bz2",
                adapters.url_streamer(url),
            )
        ),
        exists,
    )


def download_pam_pack(
    url: str,
    sha1: str,
    args: _Args,
) -> typing.Any:
    bucket, profile_name, check_sh = (
        args["bucket"],
        args["profile_name"],
        args["check_sh"],
    )

    sr, exists = _stream_reader(bucket, profile_name, url)
    if exists and not check_sh:
        return sr
    dsha1 = hashlib.new("sha1")
    while True:
        bs = sr.read(128 * 1024)
        if not len(bs):
            break
        dsha1.update(bs)
    if dsha1.digest().hex() != sha1:
        err_msg = f"downloaded {dsha1.digest().hex()} != {sha1}"
        logger.error(f"process_pam_pack: {err_msg}")
        raise PamError(err_msg)
    sr, _ = _stream_reader(bucket, profile_name, url)
    return sr


def process_pam_pack(
    pack_ref: str,
    sha1: str,
    ctx: _EntsProdCtx,
) -> PamPackStats:
    eq, args = ctx["eq"], ctx["args"]
    wiki_name, wiki_date, pam_per_pack_num = (
        args["wiki_name"],
        args["wiki_date"],
        args["pam_per_pack_num"],
    )
    url = pam_base_for(wiki_name, wiki_date) + "/" + pack_ref
    sr = download_pam_pack(url, sha1, args)
    pps = new_PamPackStats(pack_ref)
    logger.info(f"process_pam_pack: starting {url}")
    with bz2.open(sr) as cf:
        dump = mwxml.Dump.from_file(cf)
        for page in dump.pages:
            if page.redirect:
                pps["redirect_count"] += 1
            for revision in page:
                if (
                    revision.text is None
                    or revision.format != "text/x-wiki"
                    or revision.model != "wikitext"
                ):
                    pps["other_count"] += 1
                    break
                zt = zlib.compress(revision.text.encode())
                ent = _PamData(
                    key=str(revision.id),
                    title=revision.page.title,
                    updated=str(revision.timestamp),
                    additional_content=zt,
                )
                eq.put(ent)
                pps["actual_count"] += 1
                pps["total_size"] += len(revision.text.encode())
                pps["total_zsize"] += len(zt)
                break
            if pam_per_pack_num > 0 and pps["actual_count"] >= pam_per_pack_num:
                break
    logger.info(f"process_pam_pack: ended {pps}")
    return pps


def process_pam_packs(ctx: _EntsProdCtx) -> None:
    logger.info("process_pam_packs: starting")
    while True:
        jsbs: stfl_pb2.ValueOrNone = ctx["fl_stub"].Get(
            stfl_pb2.Topic(topic=MW_PAM_PACK_URLS_QUEUE)
        )
        if jsbs.is_none:
            break
        us = json.loads(jsbs.value)
        ctx["ppss"].append(process_pam_pack(us["pack_ref"], us["sha1"], ctx))
    ctx["eq"].put(None)
    logger.info("process_pam_packs: ending")


def load_generator(ctx: _EntsGenCtx) -> typing.Generator[estore_pb2.Entity]:
    eq: queue.Queue = ctx["eq"]
    assert "session_uuid" in ctx
    session_uuid = ctx["session_uuid"]
    uuid_sent = False
    while True:
        ed: _PamData | None = eq.get()
        eq.task_done()
        if ed is None:
            logger.info("load_generator: no more entries, stopping")
            return
        entity = estore_pb2.Entity()
        if not uuid_sent:
            entity.session_uuid = session_uuid
            entity.category = MW_PAM_CATEGORY
            uuid_sent = True
            logger.info(f"load_generator: {entity.session_uuid}")
        entity.key = ed["key"]
        entity.additional_content = ed["additional_content"]
        ded: typing.Dict = ed  # type:ignore
        del ded["key"]
        del ded["additional_content"]
        entity.content = json.dumps(ed).encode()
        yield entity


def load_entities(ctx: _EntsGenCtx) -> None:
    stub = ctx["es_stub"]
    ss_resp: estore_pb2.UUID = stub.StartSession(estore_pb2.Empty())
    logger.info(f"load_entities: StartSession answered {ss_resp.uuid}")
    ctx["session_uuid"] = ss_resp.uuid
    _ = stub.RecordEntities(load_generator(ctx))
    logger.info("load_entities: RecordEntities done")
    _ = stub.EndSession(estore_pb2.UUID(uuid=ss_resp.uuid))
    logger.info("load_entities: EndSession done")


def check_pack_generator(ctx: _EntsGenCtx) -> typing.Generator[list[_PamData]]:
    eq: queue.Queue = ctx["eq"]
    pack: list[_PamData] = []
    while True:
        ed: _PamData | None = eq.get()
        eq.task_done()
        if ed is None:
            logger.info(
                f"check_pack_generator: no more entries, at {len(pack)} stopping"
            )
            yield pack
            return
        pack.append(ed)
        if len(pack) == 10000:
            logger.info("check_pack_generator: yielding")
            yield pack
            pack = []


def check_loaded_entities(ctx: _EntsGenCtx) -> None:
    stub = ctx["es_stub"]
    for pack in check_pack_generator(ctx):
        dpack = {de["key"]: de for de in pack}
        for e in stub.ReadEntities(
            estore_pb2.ReadEntitiesRequest(
                category=MW_PAM_CATEGORY,
                with_additional_content=True,
                keys=dpack.keys(),
            )
        ):
            if e.key not in dpack:
                logger.error(f"returned key {e.key} not requested")
                ctx["errors_num"] += 1
            elif (
                zlib.decompress(e.additional_content)
                != dpack[e.key]["additional_content"]
            ):
                logger.error(f"additional_content mismatch {e.key}")
                ctx["errors_num"] += 1


def do_run(
    args: _Args,
    es_stub: estore_pb2_grpc.EstoreStub,
    fl_stub: stfl_pb2_grpc.FlowStub,
    lgr: logging.Logger | None = None,
) -> None:
    if lgr:
        global logger
        logger = lgr
    pctx = _EntsProdCtx(
        eq=queue.Queue(8192), args=args, es_stub=es_stub, fl_stub=fl_stub, ppss=[]
    )
    pppt = threading.Thread(target=process_pam_packs, args=(pctx,), daemon=True)
    pppt.start()

    gctx = _EntsGenCtx(eq=pctx["eq"], args=args, es_stub=es_stub, errors_num=0)
    if not args["check_mode"]:
        load_entities(gctx)
    else:
        check_loaded_entities(gctx)

    pppt.join()
    logger.info(f"load_entities: ending with {pctx['ppss']}")
    fl_stub.Put(
        stfl_pb2.TopicValue(
            topic=MW_PAM_RESULTS_QUEUE,
            value=json.dumps(
                dict(ppss=pctx["ppss"], errors_num=gctx["errors_num"])
            ).encode(),
        )
    )


def run(
    host: str,
    port: str,
    args: _Args,
) -> None:
    global logger
    logger = logging.getLogger(f"mw.pam.etl.{args['map_id']}")
    logger.info("run: starting")
    with grpc.insecure_channel(f"{host}:{port}") as channel:
        es_stub = estore_pb2_grpc.EstoreStub(channel)
        fl_stub = stfl_pb2_grpc.FlowStub(channel)
        do_run(args, es_stub, fl_stub)

    logger.info("run: ended")


def _parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--map-id", default="0")
    parser.add_argument("--bucket", default="otvl-tests")
    parser.add_argument("--s3-profile", default=None)
    parser.add_argument("--wiki-name", default=MW_FR_WIKI)
    parser.add_argument("--wiki-date", default=MW_FR_DATE)
    parser.add_argument("--check-sh", default="1")
    parser.add_argument("--pam-per-pack-num", default="0")
    parser.add_argument("--check-mode", default="")


def _doer(_: logging.Logger, host: str, port: str, pargs: argparse.Namespace) -> int:
    args = _Args(
        map_id=pargs.map_id,
        bucket=pargs.bucket,
        profile_name=pargs.s3_profile,
        wiki_name=pargs.wiki_name,
        wiki_date=pargs.wiki_date,
        check_sh=get_bool(pargs.check_sh),
        pam_per_pack_num=int(pargs.pam_per_pack_num),
        check_mode=get_bool(pargs.check_mode),
    )
    run(host, port, args)
    return 0


if __name__ == "__main__":
    status = main.main(logger, _parser, _doer)
    sys.exit(status)
