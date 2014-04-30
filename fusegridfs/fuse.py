from stat import (S_IFDIR, S_IRUSR, S_IWUSR, S_IXUSR, S_IRGRP, S_IXGRP,
    S_IROTH, S_IXOTH)
from time import time
import errno
import os

from gridfs import GridFS
from llfuse import Operations, FUSEError, EntryAttributes
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from bson.objectid import ObjectId
from gridfs.errors import NoFile


def logmethod(func):
    name = func.__name__

    def decorator(self, *args, **kwargs):
        print(name, args, kwargs)
        return func(self, *args, **kwargs)

    return decorator


def int2oid(num):
    return ObjectId(int.to_bytes(num, 12, 'big'))


def oid2int(oid):
    return int(str(oid), 16)


def grid2attrs(grid):
    entry = EntryAttributes()

    entry.st_ino = 2  # oid2int(grid._id)
    entry.generation = 0
    entry.entry_timeout = 0
    entry.attr_timeout = 0
    entry.st_mode = (
#             S_IFDIR |
        S_IRUSR |
        S_IWUSR |
        S_IXUSR |
        S_IRGRP |
        S_IXGRP |
        S_IROTH |
        S_IXOTH)
    entry.st_nlink = (grid.aliases and len(grid.aliases) or 0) + 1
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    entry.st_rdev = 0
    entry.st_size = grid.length
    entry.st_blksize = grid.chunk_size
    entry.st_blocks = int(grid.length / grid.chunk_size) + 1

    utime = grid.upload_date.timestamp()
    entry.st_ctime = utime
    entry.st_mtime = utime
    entry.st_atime = utime

    return entry


class GridFSOperations(Operations):

    def __init__(self, host, db_name='test', collection_name='fs'):
        self.client = MongoClient(host)
        self.db = Database(self.client, db_name)
        self.fs = GridFS(self.db, collection_name)

    @logmethod
    def init(self):
#         root_id = int2oid(1)
#         if not self.fs.exists(root_id):
#             self.fs.put(b'', _id=root_id, content_type='text/directory')
        pass

    @logmethod
    def lookup(self, parent_inode, name):

        if parent_inode != 1:
            raise FUSEError(errno.ENOENT)

        try:
            gridout = self.fs.get_last_version(filename=name)
        except NoFile:
            raise FUSEError(errno.ENOENT)

        print(grid2attrs(gridout).st_ino)
        return grid2attrs(gridout)

    @logmethod
    def create(self, inode_parent, name, mode, flags, ctx):
        gridin = self.fs.new_file(filename=name.decode())
        gridin.close()
        return (gridin._id, grid2attrs(gridin))

    @logmethod
    def getattr(self, inode):
#         if inode == 1:
#             return self.root_attrs
        return Operations.getattr(self, inode)

    @logmethod
    def access(self, inode, mode, ctx):
        return True

    @logmethod
    def destroy(self):
        Operations.destroy(self)

    @logmethod
    def flush(self, fh):
        Operations.flush(self, fh)

    @logmethod
    def forget(self, inode_list):
        Operations.forget(self, inode_list)

    @logmethod
    def fsync(self, fh, datasync):
        Operations.fsync(self, fh, datasync)

    @logmethod
    def fsyncdir(self, fh, datasync):
        Operations.fsyncdir(self, fh, datasync)

    @logmethod
    def getxattr(self, inode, name):
        Operations.getxattr(self, inode, name)

    @logmethod
    def link(self, inode, new_parent_inode, new_name):
        Operations.link(self, inode, new_parent_inode, new_name)

    @logmethod
    def listxattr(self, inode):
        Operations.listxattr(self, inode)

    @logmethod
    def mkdir(self, parent_inode, name, mode, ctx):
        Operations.mkdir(self, parent_inode, name, mode, ctx)

    @logmethod
    def mknod(self, parent_inode, name, mode, rdev, ctx):
        Operations.mknod(self, parent_inode, name, mode, rdev, ctx)

    @logmethod
    def open(self, inode, flags):
        Operations.open(self, inode, flags)

    @logmethod
    def opendir(self, inode):
        Operations.opendir(self, inode)

    @logmethod
    def read(self, fh, off, size):
        Operations.read(self, fh, off, size)

    @logmethod
    def readdir(self, fh, off):
        Operations.readdir(self, fh, off)

    @logmethod
    def readlink(self, inode):
        Operations.readlink(self, inode)

    @logmethod
    def release(self, fh):
        Operations.release(self, fh)

    @logmethod
    def releasedir(self, fh):
        Operations.releasedir(self, fh)

    @logmethod
    def removexattr(self, inode, name):
        Operations.removexattr(self, inode, name)

    @logmethod
    def rename(self, inode_parent_old, name_old, inode_parent_new, name_new):
        Operations.rename(self,
            inode_parent_old, name_old, inode_parent_new, name_new)

    @logmethod
    def rmdir(self, inode_parent, name):
        Operations.rmdir(self, inode_parent, name)

    @logmethod
    def setattr(self, inode, attr):
        Operations.setattr(self, inode, attr)

    @logmethod
    def setxattr(self, inode, name, value):
        Operations.setxattr(self, inode, name, value)

    @logmethod
    def stacktrace(self):
        Operations.stacktrace(self)

    @logmethod
    def statfs(self):
        Operations.statfs(self)

    @logmethod
    def symlink(self, inode_parent, name, target, ctx):
        Operations.symlink(self, inode_parent, name, target, ctx)

    @logmethod
    def unlink(self, parent_inode, name):
        Operations.unlink(self, parent_inode, name)

    @logmethod
    def write(self, fh, off, buf):
        Operations.write(self, fh, off, buf)
