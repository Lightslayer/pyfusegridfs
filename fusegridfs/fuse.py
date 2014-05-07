from datetime import datetime
from logging import debug
from stat import S_IRUSR, S_IWUSR, S_IRGRP, S_IROTH, S_IFREG, S_IFDIR
import errno
import os

from bidict import namedbidict
from dateutil.tz import tzutc, tzlocal
from gridfs import GridFS
from gridfs.errors import NoFile
from gridfs.grid_file import GridIn, GridOut
from llfuse import Operations, FUSEError, EntryAttributes
from pymongo.database import Database
from pymongo.mongo_client import MongoClient


def logmethod(func):
    name = func.__name__

    def decorator(self, *args, **kwargs):
        debug('>> %s %s %s', name, args, kwargs)
        result = func(self, *args, **kwargs)
        debug('<< %s %s', name, result)
        return result

    return decorator


OIDCache = namedbidict('OIDCache', 'oids', 'ints')
oid_cache = OIDCache()
grid_cache = {}


def int2oid(num):
    return oid_cache.ints[num]


def oid2int(oid):
    if not oid in oid_cache.oids:
        oid_cache.oids[oid] = len(oid_cache) + 2

    return oid_cache.oids[oid]


def grid2attrs(grid):
    entry = EntryAttributes()

    entry.st_ino = oid2int(grid._id)
    entry.generation = 0
    entry.entry_timeout = 0
    entry.attr_timeout = 0
    entry.st_mode = (
        S_IRUSR |
        S_IWUSR |
        S_IRGRP |
        S_IROTH)

    if grid.content_type == 'text/directory':
        entry.st_mode |= S_IFDIR
    else:
        entry.st_mode |= S_IFREG

    entry.st_nlink = (grid.aliases and len(grid.aliases) or 0) + 1
    entry.st_uid = os.getuid()
    entry.st_gid = os.getgid()
    entry.st_rdev = 0
    entry.st_size = grid.length
    entry.st_blksize = grid.chunk_size
    entry.st_blocks = int(grid.length / grid.chunk_size) + 1

    utime = grid.upload_date.replace(tzinfo=tzutc()).astimezone(
        tzlocal()).timestamp()
    entry.st_ctime = utime
    entry.st_mtime = utime
    entry.st_atime = utime

    return entry


class GridFSOperations(Operations):

    def __init__(self, host, db_name='test', collection_name='fs'):
        self.client = MongoClient(host)
        self.db = Database(self.client, db_name)
        self.fs = GridFS(self.db, collection_name)

    def _new_file(self, name):
        return self.fs.new_file(
            filename=name,
            aliases=[],
            length=0,
            upload_date=datetime.now())

    @logmethod
    def init(self):
        pass

    @logmethod
    def access(self, inode, mode, ctx):
        return True

    @logmethod
    def getattr(self, inode):
        if inode == 1:
            return Operations.getattr(self, inode)
        else:
            return grid2attrs(self.fs.get(int2oid(inode)))

    @logmethod
    def lookup(self, parent_inode, name):

        if parent_inode != 1:
            raise FUSEError(errno.ENOENT)

        try:
            gridout = self.fs.get_last_version(filename=name.decode())
        except NoFile:
            raise FUSEError(errno.ENOENT)

        return grid2attrs(gridout)

    @logmethod
    def create(self, inode_parent, name, mode, flags, ctx):
        gridin = self._new_file(name.decode())
        fh = oid2int(gridin._id)
        grid_cache[fh] = gridin
        return (fh, grid2attrs(gridin))

    @logmethod
    def flush(self, fh):
        grid = grid_cache[fh]
        grid.close()

    @logmethod
    def setattr(self, inode, attr):
        gridout = self.fs.get(int2oid(inode))
        return grid2attrs(gridout)

    @logmethod
    def release(self, fh):
        del grid_cache[fh]

    @logmethod
    def forget(self, inode_list):

        for inode in inode_list:
            if inode in oid_cache.ints:
                del oid_cache.ints[inode]

    @logmethod
    def destroy(self):
        self.client.close()

    @logmethod
    def open(self, inode, flags):
        gridout = self.fs.get(int2oid(inode))
        grid_cache[inode] = gridout
        return inode

    @logmethod
    def read(self, fh, off, size):
        grid = grid_cache[fh]

        if isinstance(grid, GridIn):
            grid.close()
            grid = self.fs.get(int2oid(fh))
            grid_cache[fh] = grid

        grid.seek(off)
        return grid.read(size)

    @logmethod
    def write(self, fh, off, buf):
        grid = grid_cache[fh]

        if isinstance(grid, GridOut):
            offbuf = grid.read(off)
            grid = self._new_file(name=grid.name)
            grid_cache[fh] = grid
            grid.write(offbuf)
            del offbuf

        if grid.closed:
            grid = self._new_file(name=grid.name)
            grid_cache[fh] = grid

        grid.write(buf)
        return len(buf)

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
    def opendir(self, inode):
        Operations.opendir(self, inode)

    @logmethod
    def readdir(self, fh, off):
        Operations.readdir(self, fh, off)

    @logmethod
    def readlink(self, inode):
        Operations.readlink(self, inode)

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
    def setxattr(self, inode, name, value):
        Operations.setxattr(self, inode, name, value)

    @logmethod
    def statfs(self):
        Operations.statfs(self)

    @logmethod
    def symlink(self, inode_parent, name, target, ctx):
        Operations.symlink(self, inode_parent, name, target, ctx)

    @logmethod
    def unlink(self, parent_inode, name):
        Operations.unlink(self, parent_inode, name)
