import logging
import threading

from ddpbasics import gllk
import pytest
from sfegrpc import stfl_pb2

from . import etl, map_, reduce
from ..pamutils import (
    MW_FR_DATE,
    MW_FR_WIKI,
    MW_PAM_PACK_URLS_QUEUE,
    MW_PAM_RESULTS_QUEUE,
)


gllk.initialize()


@pytest.fixture
def def_map_args() -> map_._Args:
    return map_._Args(
        drop=True,
        bucket="otvl-tests",
        profile_name="otvl-tests",
        wiki_name=MW_FR_WIKI,
        wiki_date=MW_FR_DATE,
        pack_url_num=0,
    )


@pytest.fixture
def def_etl_args() -> etl._Args:
    return etl._Args(
        map_id="0",
        bucket="otvl-tests",
        profile_name="otvl-tests",
        wiki_name=MW_FR_WIKI,
        wiki_date=MW_FR_DATE,
        check_sh=False,
        pam_per_pack_num=0,
        check_mode=False,
    )


@pytest.mark.parametrize(
    "pack_url_num, client_num, pam_per_pack_num, actual_count, total_size, check_mode",
    [
        (4, 2, 100, 400, 6429963, False),
        (4, 4, 100, 400, 6429963, False),
        (4, 4, 100, 400, 6429963, True),
    ],
)
def test_map_reduce(
    def_map_args,
    def_etl_args,
    fullsvc_grpc_stub,
    pack_url_num,
    client_num,
    pam_per_pack_num,
    actual_count,
    total_size,
    check_mode,
) -> None:
    es_stub, fl_stub = fullsvc_grpc_stub["es_stub"], fullsvc_grpc_stub["fl_stub"]
    map_args: map_._Args = def_map_args
    map_args["pack_url_num"] = pack_url_num
    etl_args: etl._Args = def_etl_args
    etl_args["pam_per_pack_num"] = pam_per_pack_num
    etl_args["check_mode"] = check_mode

    def _mapr():
        map_.do_run(map_args, es_stub, fl_stub)

    def _etl(ix) -> None:
        this_etl_args = etl_args
        this_etl_args["map_id"] = str(ix)
        logger = logging.getLogger(f"mw.pam.etl.{ix}")
        etl.do_run(etl_args, es_stub, fl_stub, logger)

    tcm = threading.Thread(target=_mapr, daemon=True)
    tcm.start()
    tcm.join()
    tcs = []
    for ix in range(client_num):
        tcs.append(threading.Thread(target=_etl, daemon=True, args=(ix,)))
        tcs[-1].start()
    rpps, errors_num = reduce.do_run(reduce._Args(map_num=client_num), fl_stub)
    for ix in range(client_num):
        tcs[ix].join()
        logging.getLogger().info(f"test_map_reduce: client {ix} joined")

    fl_stub.Delete(stfl_pb2.Topic(topic=MW_PAM_PACK_URLS_QUEUE))
    fl_stub.Delete(stfl_pb2.Topic(topic=MW_PAM_RESULTS_QUEUE))
    logging.getLogger().info(f"test_map_reduce: {rpps}")
    assert rpps["actual_count"] == actual_count
    assert rpps["total_size"] == total_size
    assert errors_num == 0
