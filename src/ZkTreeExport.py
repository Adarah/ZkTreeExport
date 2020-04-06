import os
import sys

from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from functools import partial
import orjson

from ErrorCodes import ErrorCodes
from loguru import logger


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
            zk_client.start()
            logger.info("Zookeeper connection established")
        except KazooTimeoutError:
            ErrorCodes.make_graceful()
            sys.exit(ErrorCodes.KAZOO_TIMEOUT.value)
        return zk_client

    @staticmethod
    def test_write_permission(destination: str) -> None:
        if os.path.exists(destination):
            if os.path.isdir(destination):
                raise IsADirectoryError(
                    "Trying to write to a directory. Please provide a file name instead."
                )
            if not os.access(destination, os.W_OK):
                raise PermissionError("No permission to write in that file")

        else:
            parent_directory = os.path.dirname(destination)
            if not parent_directory:
                parent_directory = "."
            if not os.access(parent_directory, os.W_OK):
                raise PermissionError("No permission to write in this directory")

    def recursive_traversal(self, root: str) -> dict:
        self.id += 1
        file_id = self.id
        data = self.get_node_data(root)
        children = self.zk_client.get_children(root)
        file_dict = ZkTreeExport.create_dict_r(root, children, data, Icon.FILE, file_id)
        if not children:  # object has no children
            return file_dict

        # appends root's path to children filenames
        branches = map(partial(sum, root), children)
        file_dict['children'] = branches
        file_dict['icon'] = Icon.FOLDER
        if file_id == 1:
            file_dict["state"] = {"opened": True}
        return file_dict

    def get_node_data(self, path):
        data, _ = self.zk_client.get(path)
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
        result = self.recursive_traversal(self.root)
        with open(self.destination, "wb") as f:
            f.write(orjson.dumps(result))
