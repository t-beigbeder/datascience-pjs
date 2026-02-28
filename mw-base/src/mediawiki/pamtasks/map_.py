import argparse
import logging
import sys
import typing
import json
import re

from ddpbasics import cache, adapters
from ddpestorec import main
import grpc
from sfegrpc import estore_pb2, estore_pb2_grpc, stfl_pb2, stfl_pb2_grpc

from ..pamutils import (
    MWM_BASE,
    MW_PAM_CATEGORY,
    MW_PAM_PACK_URLS_QUEUE,
    MW_PAM_RESULTS_QUEUE,
    pam_base_for,
    MW_FR_WIKI,
    MW_FR_DATE,
    PAM_INDEX_CACHE_CAT,
)

logger = logging.getLogger("mw.pam.map")


class _Args(typing.TypedDict):
    drop: bool
    bucket: str
    profile_name: str | None
    wiki_name: str
    wiki_date: str
    pack_url_num: int


def _pam_pack_urls_with_sha1s(args: _Args, mwm_base: str) -> dict[str, str]:
    bucket, profile_name, wiki_name, wiki_date = (
        args["bucket"],
        args["profile_name"],
        args["wiki_name"],
        args["wiki_date"],
    )
    js_url = f"{pam_base_for(wiki_name, wiki_date, mwm_base)}/{wiki_name}-{wiki_date}-sha1sums.json"
    jso = json.loads(
        adapters.stream_reader(
            cache.s3_cache_streamer(
                bucket,
                profile_name,
                js_url,
                PAM_INDEX_CACHE_CAT,
                ".json",
                adapters.url_streamer(js_url),
            )
        ).read()
    )
    # jso['sha1']['files']['frwiki-20251201-pages-articles-multistream4.xml-p2977215p4477214.bz2']
    ks = []
    for k in jso["sha1"]["files"]:
        if re.search(r"-pages-articles-multistream.*\.xml-p.*\.bz2", k) is None:
            continue
        ks.append(k)
    return {k: jso["sha1"]["files"][k] for k in ks}


def _fetch_and_push_pam_pack_urls(fl_stub: stfl_pb2_grpc.FlowStub, args: _Args) -> None:
    us = _pam_pack_urls_with_sha1s(args, MWM_BASE)
    pack_url_num = args["pack_url_num"]

    for i, (pack_ref, sha1) in enumerate(us.items()):
        jsbs = json.dumps(dict(pack_ref=pack_ref, sha1=sha1)).encode()
        fl_stub.Put(stfl_pb2.TopicValue(topic=MW_PAM_PACK_URLS_QUEUE, value=jsbs))
        if pack_url_num > 0 and i >= pack_url_num-1:
            break
    fl_stub.Shutdown(stfl_pb2.ShutdownRequest(topic=MW_PAM_PACK_URLS_QUEUE, immediate=False))


def do_run(
    args: _Args,
    es_stub: estore_pb2_grpc.EstoreStub,
    fl_stub: stfl_pb2_grpc.FlowStub,
) -> None:
    logger.info(f"run: create estore drop {args['drop']}")
    es_stub.Create(estore_pb2.CreateRequest(exist_ok=True, drop=args["drop"], ddl=None))
    logger.info(f"run: create category {MW_PAM_CATEGORY}")
    es_stub.CreateCategory(
        estore_pb2.CreateCategoryRequest(label=MW_PAM_CATEGORY, content="")
    )
    fl_stub.Create(
        stfl_pb2.TopicConfig(topic=MW_PAM_PACK_URLS_QUEUE, size=1024, max_value_size=1024)
    )
    fl_stub.Create(
        stfl_pb2.TopicConfig(topic=MW_PAM_RESULTS_QUEUE, size=64, max_value_size=65536)
    )
    _fetch_and_push_pam_pack_urls(fl_stub, args)


def run(
    host: str,
    port: str,
    args: _Args,
) -> None:
    logger.info("run: starting")
    with grpc.insecure_channel(f"{host}:{port}") as channel:
        es_stub = estore_pb2_grpc.EstoreStub(channel)
        fl_stub = stfl_pb2_grpc.FlowStub(channel)
        do_run(args, es_stub, fl_stub)

    logger.info("run: ended")


def _parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--drop")
    parser.add_argument("--bucket", default="otvl-tests")
    parser.add_argument("--s3-profile", default=None)
    parser.add_argument("--wiki-name", default=MW_FR_WIKI)
    parser.add_argument("--wiki-date", default=MW_FR_DATE)
    parser.add_argument("--pack-url-num", default="0")


def _doer(_: logging.Logger, host: str, port: str, pargs: argparse.Namespace) -> int:
    args = _Args(
        drop=main.is_drop_set(pargs),
        bucket=pargs.bucket,
        profile_name=pargs.s3_profile,
        wiki_name=pargs.wiki_name,
        wiki_date=pargs.wiki_date,
        pack_url_num=int(pargs.pack_url_num),
    )
    run(host, port, args)
    return 0


if __name__ == "__main__":
    status = main.main(logger, _parser, _doer)
    sys.exit(status)
