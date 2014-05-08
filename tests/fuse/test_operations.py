from datetime import datetime
from stat import S_IFREG, S_IRUSR, S_IWUSR, S_IRGRP, S_IROTH
import errno
import os

from bson.objectid import ObjectId
from gridfs import GridFS
from llfuse.pyapi import FUSEError
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
import pytest

from fusegridfs.fuse import GridFSOperations, oid2int, oid_cache, grid_cache


@pytest.fixture(scope='module')
def now():
    return datetime.now()


@pytest.fixture(scope='module')
def utcnow(now):
    return datetime.utcfromtimestamp(now.timestamp())


@pytest.fixture(scope='module')
def oid():
    return ObjectId('000000000000000000000001')


@pytest.fixture(scope='module')
def filename():
    return 'test name'


@pytest.fixture(scope='module')
def data():
    return b'test data'


@pytest.fixture
def ops(utcnow, oid, filename, data):
    oid_cache.clear()
    grid_cache.clear()
    client = MongoClient()
    db = Database(client, 'test')
    files = Collection(db, 'fs.files')
    chunks = Collection(db, 'fs.chunks')
    files.drop()
    chunks.drop()
    fs = GridFS(db)
    fs.put(data, _id=oid, filename=filename, upload_date=utcnow)
    return GridFSOperations(client.host)


@pytest.mark.parametrize('args,host,db_name,collection_name', [
    (('localhost',), 'localhost', 'test', 'fs'),
    (('127.0.0.1',), '127.0.0.1', 'test', 'fs'),
    (('localhost', 'test'), 'localhost', 'test', 'fs'),
    (('localhost', 'test_'), 'localhost', 'test_', 'fs'),
    (('localhost', 'test', 'fs'), 'localhost', 'test', 'fs'),
    (('localhost', 'test', 'testfs'), 'localhost', 'test', 'testfs'),
])
def test__init_(args, host, db_name, collection_name):
    ops = GridFSOperations(*args)
    assert ops.client.host == host
    assert ops.db.name == db_name
    assert ops.fs._GridFS__collection.name == collection_name


def test_access(ops):
    assert ops.access(None, None, None) is True


def test_getattr_1(ops):
    with pytest.raises(FUSEError) as excinfo:
        ops.getattr(1)
    assert excinfo.value.errno == errno.ENOSYS


def test_getattr_file(ops, now, oid, data):
    inode = oid2int(oid)
    entry = ops.getattr(inode)
    assert entry.st_ino == inode
    assert entry.generation == 0
    assert entry.entry_timeout == 0
    assert entry.attr_timeout == 0
    assert entry.st_mode == S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
    assert entry.st_nlink == 1
    assert entry.st_uid == os.getuid()
    assert entry.st_gid == os.getgid()
    assert entry.st_rdev == 0
    assert entry.st_size == len(data)
    assert entry.st_blksize == 255 * 1024
    assert entry.st_blocks == 1
    assert entry.st_ctime - now.timestamp() < 1
    assert entry.st_mtime - now.timestamp() < 1
    assert entry.st_atime - now.timestamp() < 1


def test_lookup_parent_not_1(ops, filename):
    with pytest.raises(FUSEError) as excinfo:
        ops.lookup(42, filename.encode())
    assert excinfo.value.errno == errno.ENOENT


def test_lookup_file(ops, filename, data, now):
    entry = ops.lookup(1, filename.encode())
    assert entry.st_ino == 2
    assert entry.generation == 0
    assert entry.entry_timeout == 0
    assert entry.attr_timeout == 0
    assert entry.st_mode == S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH
    assert entry.st_nlink == 1
    assert entry.st_uid == os.getuid()
    assert entry.st_gid == os.getgid()
    assert entry.st_rdev == 0
    assert entry.st_size == len(data)
    assert entry.st_blksize == 255 * 1024
    assert entry.st_blocks == 1
    assert entry.st_ctime - now.timestamp() < 1
    assert entry.st_mtime - now.timestamp() < 1
    assert entry.st_atime - now.timestamp() < 1


def test_lookup_no_file(ops, filename):
    with pytest.raises(FUSEError) as excinfo:
        ops.lookup(1, b'nil')
    assert excinfo.value.errno == errno.ENOENT


def test_create_new_file(ops, filename):
    new_filename = 'new ' + filename
    fh, entry = ops.create(1, new_filename.encode(), 33204, 34881, None)
    assert fh == 2
    assert entry.st_ino == 2
    assert entry.st_size == 0
    assert entry.st_blksize == 255 * 1024
    assert entry.st_blocks == 0


def test_create_same_file(ops, filename):
    fh, entry = ops.create(1, filename.encode(), 33204, 34881, None)
    assert fh == 2
    assert entry.st_ino == 2
    assert entry.st_size == 0
    assert entry.st_blksize == 255 * 1024
    assert entry.st_blocks == 0


def test_flush(ops, filename):

    class GridMock:
        closed = False

        def close(self):
            self.closed = True

    entry = ops.lookup(1, filename.encode())
    fh = ops.open(entry.st_ino, 32768)
    grid_cache[fh] = GridMock()
    ops.flush(fh)
    assert grid_cache[fh].closed
