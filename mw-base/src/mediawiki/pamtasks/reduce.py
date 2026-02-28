import argparse
import json
import logging
import sys
import typing

from ddpestorec import main
import grpc
from sfegrpc import stfl_pb2, stfl_pb2_grpc

from ..pamutils import MW_PAM_RESULTS_QUEUE, PamPackStats, new_PamPackStats

logger = logging.getLogger("mw.pam.reduce")


class _Args(typing.TypedDict):
    map_num: int


def do_run(args: _Args, fl_stub: stfl_pb2_grpc.FlowStub) -> tuple[PamPackStats, int]:
    ppssl: list[list[PamPackStats]] = []
    errors_num = 0
    while True:
        jsbs: stfl_pb2.ValueOrNone = fl_stub.Get(
            stfl_pb2.Topic(topic=MW_PAM_RESULTS_QUEUE)
        )
        jso = json.loads(jsbs.value)
        ppssl.append(jso["ppss"])
        logger.info(f"do_run: reduce from {jso['ppss']}")
        errors_num += jso["errors_num"]
        if len(ppssl) >= args["map_num"]:
            break
    rpps = new_PamPackStats("")
    for ppss in ppssl:
        for pps in ppss:
            rpps["actual_count"] += pps["actual_count"]
            rpps["total_size"] += pps["total_size"]
            rpps["total_zsize"] += pps["total_zsize"]
    logger.info(f"do_run: rpps {rpps} errors {errors_num}")
    return rpps, errors_num


def run(
    host: str,
    port: str,
    args: _Args,
) -> int:
    logger.info("run: starting")
    with grpc.insecure_channel(f"{host}:{port}") as channel:
        fl_stub = stfl_pb2_grpc.FlowStub(channel)
        _, errors_num = do_run(args, fl_stub)

    logger.info("run: ended")
    return errors_num


def _parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--map-num", default="1")


def _doer(_: logging.Logger, host: str, port: str, pargs: argparse.Namespace) -> int:
    args = _Args(map_num=int(pargs.map_num))
    return run(host, port, args)


if __name__ == "__main__":
    status = main.main(logger, _parser, _doer)
    sys.exit(status)
