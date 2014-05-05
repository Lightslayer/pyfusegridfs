from argparse import ArgumentParser


parser = ArgumentParser(
    prog='fusegridfs',
    description='Mount MongoDB GridFS as FUSE filesystem.')

parser.add_argument(
    'mountpoint', type=str,
    help='mount point to mount GridFS on')
parser.add_argument(
    'host', type=str,
    help='host to connect to or full connection string')
parser.add_argument(
    '--db', type=str, nargs='?', default='test',
    help='db to connect to (defaults to "test")')
parser.add_argument(
    '--collection', type=str, nargs='?', default='fs',
    help='GridFS collection to use (defaults to "fs")')
parser.add_argument(
    '--debug', action='store_true')
