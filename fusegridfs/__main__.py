import llfuse
import sys

from .argv import parser
from .fuse import GridFSOperations
from .log import setup_logging

def main(argv=sys.argv):
    args = parser.parse_args()

    setup_logging(debug=args.debug)
    operations = GridFSOperations(args.host, args.db, args.collection)

    llfuse.init(operations, args.mountpoint, ['fsname=gridfs'])

    try:
        llfuse.main(single=args.single)
    except:
        llfuse.close()
        raise
    finally:
        llfuse.close()

if __name__ == '__main__':
    main()

