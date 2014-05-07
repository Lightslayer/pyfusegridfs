import errno

from bson.objectid import ObjectId
from gridfs import GridFS
from llfuse.pyapi import FUSEError
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
import pytest

from fusegridfs.fuse import GridFSOperations, oid2int
from datetime import datetime
from stat import S_IFREG, S_IRUSR, S_IWUSR, S_IRGRP, S_IROTH
import os


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
def data():
    return b'test data'


@pytest.fixture
def ops(utcnow, oid, data):
    client = MongoClient()
    db = Database(client, 'test')
    files = Collection(db, 'fs.files')
    chunks = Collection(db, 'fs.chunks')
    files.drop()
    chunks.drop()
    fs = GridFS(db)
    fs.put(data, _id=oid, upload_date=utcnow)
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
