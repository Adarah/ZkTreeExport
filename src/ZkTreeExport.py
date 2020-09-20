import os
import time
import sys

import orjson
from kazoo.client import KazooClient
from kazoo.interfaces import IAsyncResult
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.exceptions import NoNodeError
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
    def new(cls, host: str, zk_root: str, credentials: str, destination: str) -> "ZkTreeExport":
        """Initializes a ZkTreeObject, performing various tests"""
        instance = cls(zk_root, destination)
        instance.zk_client = ZkTreeExport.start_kazoo(host, credentials)
        try:
            ZkTreeExport.test_write_permission(destination)
            logger.debug("Write permission successful.")
        except IsADirectoryError as err:
            ErrorCodes.make_graceful(err, "{destination} is a directory")
            sys.exit(ErrorCodes.IS_A_DIRECTORY.value)
        except PermissionError as err:
            ErrorCodes.make_graceful(err, "no write permission in {destination}")
            sys.exit(ErrorCodes.NO_WRITE_PERMISSION.value)

        return instance

    @staticmethod
    def start_kazoo(host: str, credentials: str) -> KazooClient:
        """Starts a connection to the Zookeeper client"""
        zk_client = KazooClient(hosts=host)
        zk_client.add_auth_async("digest", credentials)
        try:
            event = zk_client.start_async()
            event.wait(timeout=10)
            logger.info("Zookeeper connection established")
        except KazooTimeoutError as err:
            ErrorCodes.make_graceful(err, "Zookeeper server timed out")
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

    def recursive_traversal(self, root: str) -> dict:
        async_data = self.zk_client.get_async(root)
        async_children = self.zk_client.get_children_async(root)

        self.id += 1
        file_id = self.id
        data = self.get_async_node_data(async_data)
        children = async_children.get()
        file_dict = ZkTreeExport.create_dict_r(root, children, data, Icon.FILE, file_id)
        if not children:  # object has no children
            return file_dict

        branches = []
        for child in children:
            new_root = f"{root}/{child}"
            partial_result = self.recursive_traversal(new_root)
            branches.append(partial_result)

        file_dict["children"] = branches
        file_dict["icon"] = Icon.FOLDER
        if file_id == 1:
            file_dict["state"] = {"opened": True}
        return file_dict

    def get_async_node_data(self, async_data: IAsyncResult) -> str:
        try:
            data, _ = async_data.get()
        except NoNodeError as err:
            ErrorCodes.make_graceful(err, "No node found")
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
        result = self.recursive_traversal(self.root)
        tock = time.perf_counter()
        logger.info(f"Took {(tock - tick):.3f}s to traverse {self.id} nodes")
        logger.info("Beginning JSON dumping")
        with open(self.destination, "wb") as f:
            f.write(orjson.dumps(result))
        logger.info(f"File successfully writen to {self.destination}")
