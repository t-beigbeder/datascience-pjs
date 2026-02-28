import json
import logging
import threading

from ddpbasics import gllk
import pytest
from sfegrpc import stfl_pb2

from . import etl, map_
from ..pamutils import (
    MW_FR_DATE,
    MW_FR_WIKI,
    MW_PAM_PACK_URLS_QUEUE,
    MW_PAM_RESULTS_QUEUE,
    new_PamPackStats
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
        (1, 1, 100, 100, 4680457, False),
        (1, 1, 100, 100, 4680457, True),
        (2, 1, 100, 200, 5804889, False),
        (4, 2, 100, 400, 6429963, False),
        (4, 4, 100, 400, 6429963, False),
        (4, 4, 100, 400, 6429963, True),
    ],
)
def test_mapr_basic(
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
    for ix in range(client_num):
        tcs[ix].join()
    ppssl: list[list[etl.PamPackStats]] = []
    errors_num = 0
    while True:
        jsbs: stfl_pb2.ValueOrNone = fl_stub.Get(
            stfl_pb2.Topic(topic=MW_PAM_RESULTS_QUEUE)
        )
        jso = json.loads(jsbs.value)
        ppssl.append(jso["ppss"])
        errors_num += jso["errors_num"]
        if len(ppssl) >= client_num:
            break

    fl_stub.Delete(stfl_pb2.Topic(topic=MW_PAM_PACK_URLS_QUEUE))
    fl_stub.Delete(stfl_pb2.Topic(topic=MW_PAM_RESULTS_QUEUE))
    logging.getLogger().info(f"test_mapr_basic: {ppssl}")
    rpps = new_PamPackStats("")
    for ppss in ppssl:
        for pps in ppss:
            rpps["actual_count"] += pps["actual_count"]
            rpps["total_size"] += pps["total_size"]
            rpps["total_zsize"] += pps["total_zsize"]
    assert rpps["actual_count"] == actual_count
    assert rpps["total_size"] == total_size
    assert errors_num == 0
