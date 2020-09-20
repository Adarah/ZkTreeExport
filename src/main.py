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
        print(f"\nUsage: python3 {basename(__file__)} host:port/root username:pass /path/to/export")
        raise NotADirectoryError(
            """No directory provided. Please add / after\n
the port if you wish to start from the root"""
        )
    return (zk_string[:idx], zk_string[idx:])


def get_args():
    # TODO: Implement argument parsing for future options
    if len(sys.argv) != 4:
        print(f"\nUsage: python3 {basename(__file__)} host:port/root /path/to/export")
        raise IndexError("Wrong number of arguments")

    try:
        host, zk_path = parse_zk_string(sys.argv[1])
    except NotADirectoryError as err:
        ErrorCodes.make_graceful(err, "{zk_path} is not a directory")
        sys.exit(ErrorCodes.NOT_A_DIRECTORY.value)

    credentials = sys.argv[2]
    destination = sys.argv[3]
    return (host, zk_path, credentials, destination)


def main():
    try:
        host, zk_root, credentials, destination_file = get_args()
        export = ZkTreeExport.new(host, zk_root, credentials, destination_file)
        export.to_json()
    except IndexError as err:
        ErrorCodes.make_graceful(err)
        sys.exit(ErrorCodes.WRONG_NUM_ARGUMENTS.value, "wrong number of arguments")
    except NotImplementedError as err:
        ErrorCodes.make_graceful(err)
        sys.exit(ErrorCodes.NOT_IMPLEMENTED.value, "some function was not implemented")


if __name__ == "__main__":
    main()
