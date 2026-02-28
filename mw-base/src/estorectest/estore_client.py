import argparse
import logging
import queue
import sys
import threading

from ddpestorec import main
import grpc
from sfegrpc import estore_pb2, estore_pb2_grpc


q: queue.Queue = queue.Queue(100)
logger = logging.getLogger("estore_client")


def get_entity(uuid_: str, category: str):
    uuid_sent = False
    while True:
        entity: estore_pb2.Entity | None = q.get()
        q.task_done()
        if entity is None:
            logger.info("get_entity: no more entries, stopping")
            return
        if not uuid_sent:
            entity.session_uuid = uuid_
            entity.category = category
            uuid_sent = True
            logger.info(f"get_entity: {entity.session_uuid}")
        logger.debug(f"get_entity: {entity}")
        yield entity


def gen_entity():
    def bcont(i):
        return (f"this is a rather large message for the entity #{i}" * 400).encode()

    # background work to provide data in a queue
    for i in range(20000):
        entity = estore_pb2.Entity(
            key=str(i), content=f"entity #{i}".encode(), additional_content=bcont(i)
        )
        q.put(entity)
    q.put(None)


def run(host, port, drop, category) -> None:
    threading.Thread(target=gen_entity, daemon=True).start()
    with grpc.insecure_channel(f"{host}:{port}") as channel:
        stub = estore_pb2_grpc.EstoreStub(channel)
        stub.Create(
            estore_pb2.CreateRequest(exist_ok=True, drop=drop, ddl=None)
        )
        stub.CreateCategory(
            estore_pb2.CreateCategoryRequest(label=category, content="")
        )
        ss_resp: estore_pb2.UUID = stub.StartSession(estore_pb2.Empty())
        logger.info(f"startSession answered {ss_resp.uuid}")
        rec_resp = stub.RecordEntities(get_entity(ss_resp.uuid, category))
        logger.info(f"recordEntities {rec_resp}")
        cls_resp = stub.EndSession(estore_pb2.UUID(uuid=ss_resp.uuid))
        logger.info(f"endSession {cls_resp}")


def _parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--drop")
    parser.add_argument("-c", "--category", default="cat-default")


def _doer(
    _: logging.Logger, host: str, port: str, args: argparse.Namespace
) -> int:
    run(host, port, main.is_drop_set(args), args.category)
    return 0


if __name__ == "__main__":
    status = main.main(logger, _parser, _doer)
    sys.exit(status)
