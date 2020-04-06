import sys
from os.path import basename
from ZkTreeExport import ZkTreeExport
from ErrorCodes import ErrorCodes


def parse_zk_string(zk_string):
    """Separate a ZK connect string (e.g. zookeeper.foo.com:2181/znode1/subnode1)
    into the hostname:port and the ZK path.
    """
    idx = zk_string.find("/")
    if idx == -1:
        raise Exception("Invalid Zookeeper connect string")
    return (zk_string[:idx], zk_string[idx:])


def get_args():
    # TODO: Implement argument parsing for future options
    if len(sys.argv) != 3:
        print(f"Usage: python3 {basename(__file__)} host:port/root /path/to/export")
        raise IndexError("Wrong number of arguments")
    host, zk_path = parse_zk_string(sys.argv[1])
    destination = sys.argv[2]
    return (host, zk_path, destination)


def main():
    try:
        host, zk_root, destination_file = get_args()
        export = ZkTreeExport.new(host, zk_root, destination_file)
        export.to_json()
    except IndexError as err:
        ErrorCodes.make_graceful(err)
        sys.exit(ErrorCodes.WRONG_NUM_ARGUMENTS.value)
    except NotImplementedError as err:
        ErrorCodes.make_graceful(err)
        sys.exit(ErrorCodes.NOT_IMPLEMENTED.value)


if __name__ == "__main__":
    main()
