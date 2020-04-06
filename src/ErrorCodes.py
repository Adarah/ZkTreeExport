from enum import Enum, unique
from uuid import uuid4
from loguru import logger


@unique
class ErrorCodes(Enum):
    NOT_IMPLEMENTED = 255
    WRONG_NUM_ARGUMENTS = 1
    KAZOO_TIMEOUT = 2
    NO_WRITE_PERMISSION = 3
    IS_A_DIRECTORY = 4

    @staticmethod
    def make_graceful(ex: Exception):
        error_id = uuid4()
        logger.exception(error_id)
        octet = str(error_id)[:8]
        print(f"""It seems an error has occurred.\n{ex}\nError id: {octet}""")
