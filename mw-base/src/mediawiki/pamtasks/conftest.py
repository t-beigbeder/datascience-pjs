from concurrent import futures
import socket
import threading

from ddpestores import stfl_server, estore_server, estoredb, s3sqix
import grpc
import pytest
from sfegrpc import stfl_pb2_grpc, estore_pb2_grpc


@pytest.fixture
def free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


@pytest.fixture
def server_port_stfl(free_port):

    def _wait_for_server():
        server.wait_for_termination()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10), options=(("grpc.so_reuseport", 0),)
    )
    state = stfl_server.State(None)
    stfl_pb2_grpc.add_StateServicer_to_server(state, server)
    flow = stfl_server.Flow(None)
    stfl_pb2_grpc.add_FlowServicer_to_server(flow, server)
    estore = estore_server.Estore()
    estore_pb2_grpc.add_EstoreServicer_to_server(estore, server)
    server.add_insecure_port(f"localhost:{free_port}")
    server.start()

    t = threading.Thread(target=_wait_for_server, daemon=True)
    t.start()
    yield server, free_port, state, flow, estore
    server.stop(None)
    t.join()


@pytest.fixture
def grpc_stub(server_port_stfl):
    server, port, state, flow, estore = server_port_stfl
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        st_stub = stfl_pb2_grpc.StateStub(channel)
        fl_stub = stfl_pb2_grpc.FlowStub(channel)
        es_stub = estore_pb2_grpc.EstoreStub(channel)
        yield dict(
            server=server,
            port=free_port,
            state=state,
            st_stub=st_stub,
            flow=flow,
            fl_stub=fl_stub,
            estore=estore,
            es_stub=es_stub,
        )


@pytest.fixture
def db_service():
    svc = estoredb.PgService("t-db-pgs", "pamtasks_pytest")
    svc.create_db(True, True)
    yield svc


@pytest.fixture
def ixf_service(tmp_path):
    td = str(tmp_path)
    ixf_service = s3sqix.S3SqixFileService(
        "otvl-tests", td, "pamtasks_pytest", "otvl-tests"
    )
    yield ixf_service


@pytest.fixture
def ixe_service():
    ixe_service = s3sqix.S3SqixEntryService("otvl-tests", "otvl-tests")
    yield ixe_service


@pytest.fixture
def fullsvc_grpc_stub(grpc_stub, db_service, ixf_service, ixe_service):
    estore = grpc_stub["estore"]
    estore.set_db_service(db_service)
    estore.set_ixf_service(ixf_service)
    estore.set_ixe_service(ixe_service)
    yield grpc_stub
