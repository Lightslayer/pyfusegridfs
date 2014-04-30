import llfuse

from .argv import parser
from .fuse import GridFSOperations


args = parser.parse_args()

operations = GridFSOperations(args.host, args.db, args.collection)

llfuse.init(operations, args.mountpoint, ['fsname=gridfs'])

try:
    llfuse.main(single=True)
except:
    llfuse.close()
    raise
finally:
    llfuse.close()
