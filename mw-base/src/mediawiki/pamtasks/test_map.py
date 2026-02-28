import logging
import threading

from ddpbasics import gllk
import pytest
from sfegrpc import stfl_pb2

from . import map_
from ..pamutils import MW_PAM_RESULTS_QUEUE, MWM_BASE, MW_FR_DATE, MW_FR_WIKI, MW_PAM_PACK_URLS_QUEUE


gllk.initialize()


@pytest.fixture
def def_args() -> map_._Args:
    return map_._Args(
        drop=True,
        bucket="otvl-tests",
        profile_name="otvl-tests",
        wiki_name=MW_FR_WIKI,
        wiki_date=MW_FR_DATE,
        pack_url_num=0,
    )


def test_pam_pack_urls_with_sha1s(def_args):
    us = map_._pam_pack_urls_with_sha1s(def_args, MWM_BASE)
    _ = us


@pytest.mark.parametrize(
    "pack_url_num, reduce_num", [(0, 15), (10, 10)]
)
def test_mapr_basic(def_args, fullsvc_grpc_stub, pack_url_num, reduce_num) -> None:
    es_stub, fl_stub = fullsvc_grpc_stub["es_stub"], fullsvc_grpc_stub["fl_stub"]
    args: map_._Args = def_args
    args["pack_url_num"] = pack_url_num

    def _mapr():
        map_.do_run(def_args, es_stub, fl_stub)

    def _simu_worker(ix, cs) -> None:
        logger = logging.getLogger(f"_simu_worker#{ix}")
        while True:
            logger.debug("get")
            val: stfl_pb2.ValueOrNone = fl_stub.Get(stfl_pb2.Topic(topic=MW_PAM_PACK_URLS_QUEUE))
            logger.debug(f"got {val}")
            if val.is_none:
                return
            cs[ix] += 1

    tcm = threading.Thread(target=_mapr, daemon=True)
    tcm.start()
    tcm.join()
    tcs = []
    cs = []
    for ix in range(4):
        cs.append(0)
        tcs.append(threading.Thread(target=_simu_worker, daemon=True, args=(ix, cs)))
        tcs[-1].start()
    count = 0
    for ix in range(4):
        tcs[ix].join()
        count += cs[ix]
    fl_stub.Delete(stfl_pb2.Topic(topic=MW_PAM_PACK_URLS_QUEUE))
    fl_stub.Delete(stfl_pb2.Topic(topic=MW_PAM_RESULTS_QUEUE))
    assert count == reduce_num
