import os
import sys
import time
from typing import List

import orjson
import trio
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.interfaces import IAsyncResult
from loguru import logger

from ErrorCodes import ErrorCodes

logger.remove()
logger.add("ZkTreeExport.log", level="INFO", rotation="200 MB")


class Icon:
    FILE = "fas fa-file"
    FOLDER = "far fa-folder"


class ZkTreeExport:
    def __init__(self, zk_root, destination):
        # the actual initialization happens in the "new" classmethod
        self.zk_client = None
        self.root = zk_root
        self.destination = destination
        self.id = 0  # subject to removal

    @classmethod
    def new(cls, host: str, zk_root: str, destination: str) -> "ZkTreeExport":
        """Initializes a ZkTreeObject, performing various tests"""
        instance = cls(zk_root, destination)
        instance.zk_client = ZkTreeExport.start_kazoo(host)
        try:
            ZkTreeExport.test_write_permission(destination)
            logger.debug("Write permission successful.")
        except IsADirectoryError as err:
            ErrorCodes.make_graceful(err)
            sys.exit(ErrorCodes.IS_A_DIRECTORY.value)
        except PermissionError as err:
            ErrorCodes.make_graceful(err)
            sys.exit(ErrorCodes.NO_WRITE_PERMISSION.value)

        return instance

    @staticmethod
    def start_kazoo(host: str) -> KazooClient:
        """Starts a connection to the Zookeeper client"""
        zk_client = KazooClient(hosts=host)
        try:
            event = zk_client.async_start()
            event.wait(timeout=10)
            logger.info("Zookeeper connection established")
        except KazooTimeoutError as err:
            ErrorCodes.make_graceful(err)
            sys.exit(ErrorCodes.KAZOO_TIMEOUT.value)
        return zk_client

    @staticmethod
    def test_write_permission(destination: str) -> None:
        if os.path.exists(destination):
            if os.path.isdir(destination):
                raise IsADirectoryError(
                    "Trying to write to a directory. Please provide a filename instead."
                )
            if not os.access(destination, os.W_OK):
                raise PermissionError("No permission to write in that file")

        else:
            parent_directory = os.path.dirname(destination)
            if not parent_directory:
                parent_directory = "."
            if not os.access(parent_directory, os.W_OK):
                raise PermissionError("Directory does not exist")

    async def recursive_traversal(self, root: str, result: List[dict]) -> None:
        async_data = self.zk_client.get_async(root)
        async_children = self.zk_client.get_async(root)

        self.id += 1
        file_id = self.id
        data = self.get_async_node_data(async_data)
        children = async_children.get()
        file_dict = ZkTreeExport.create_dict_r(root, children, data, Icon.FILE, file_id)
        if not children:  # object has no children
            result.append(file_dict)
            return None

        branches = []
        async with trio.open_nursery() as nursery:
            for child in children:
                new_root = f"{root}/{child}"
                nursery.start_soon(new_root, branches)

        file_dict["children"] = branches
        file_dict["icon"] = Icon.FOLDER
        if file_id == 1:
            file_dict["state"] = {"opened": True}
        result.append(file_dict)
        return None

    def get_async_node_data(self, async_data: IAsyncResult) -> str:
        try:
            data, _ = async_data.get()
        except NoNodeError as err:
            ErrorCodes.make_graceful(err)
            sys.exit(ErrorCodes.NO_NODE.value)
        return data.decode("utf-8").replace("\n", "<br>")

    @staticmethod
    def create_dict_r(path, children, data="", icon=Icon.FILE, id=None):
        return {
            "text": path.split("/")[-1],
            "children": children,
            "data": data,
            "icon": icon,
            "id": id,
        }

    def to_json(self):
        logger.info("Beginning tree traversal")
        tick = time.perf_counter()
        result = []
        trio.run(self.recursive_traversal(self.root, result))
        tock = time.perf_counter()
        logger.info(f"Took {(tock - tick):.3f}s to traverse {self.id} nodes")
        logger.info("Beginning JSON dumping")
        with open(self.destination, "wb") as f:
            f.write(orjson.dumps(result))
        logger.info("File successfully writen to {self.destination}")
