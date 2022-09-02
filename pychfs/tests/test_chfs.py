import os
import stat
import time
import cython
import random, string
import pytest
import pychfs

rootdir   = "/tmp/chfuse"
file1     = os.path.join(rootdir, "file1.txt")
byte_text = b''

@pytest.fixture(autouse=True)
def init():
    server = os.getenv("CHFS_SERVER")
    if not server:
        raise ValueError("CHFS server may not be up. Set CHFS_SERVER")
    pychfs.init(server)
    yield
    pychfs.term()

def exists(path):
    try:
        pychfs.stat(path)
    except ValueError:
        return False
    return True

@pytest.mark.dependency()
def test_pychfs_create_stat():
    fd = pychfs.create(file1, 0, 0o0777)
    st = pychfs.stat(file1)
    assert(stat.S_ISREG(st['st_mode']) == 1)
    assert(st['st_size'] == 0)
    assert(pychfs.close(fd) == 0)

@pytest.fixture
def str_data():
    def rand_str(n):
       rlist = [random.choice(string.ascii_letters + string.digits) for i in range(n)]
       return ''.join(rlist)

    return rand_str

def test_pychfs_create_write(str_data):
    size = 256

    fd = pychfs.create(file1, os.O_RDWR, stat.S_IRWXU)
    byte_text = bytearray(str_data(size).encode('ascii'))
    assert(pychfs.write(fd, byte_text, size) == size)
    buf = bytearray(b'\x00') * size
    s = pychfs.pread(fd, buf, size, 0)
    assert(s == size)
    assert(byte_text == buf)
    st = pychfs.stat(file1)
    assert(st['st_size'] == size)
    assert(pychfs.close(fd) == 0)

@pytest.mark.dependency()
def test_pychfs_rw(str_data):
    size = 256

    fd = pychfs.open(file1, 0)
    text = str_data(size)
    byte_text = bytearray(text.encode('ascii'))
    assert(pychfs.write(fd, byte_text, size) == size)
    read_buf = bytearray(b'\x00') * size
    assert(pychfs.pread(fd, read_buf, size, 0) == size)
    assert(''.join([chr(x) for x in read_buf[:]]) == text)
    assert(pychfs.close(fd) == 0)

@pytest.fixture
def offset():
    return 5

@pytest.mark.dependency(depends=['test_pychfs_rw'])
def test_pychfs_pwrite_pread(offset):
    st = pychfs.stat(file1)
    size   = st['st_size'] // 2
    sbytes = bytearray(b'Z' * size)
    fd = pychfs.open(file1, 0)
    assert(pychfs.pwrite(fd, sbytes, size, offset) == size)
    read_buf = bytearray(b'\x00') * size
    assert(pychfs.pread(fd, read_buf, size, offset) == size)
    assert(''.join([chr(x) for x in read_buf[:]]) == 'Z' * size)
    assert(pychfs.close(fd)==0)
    del st

def test_pychfs_truncate(str_data):
    size = 256

    fd = pychfs.open(file1, 0)
    byte_text = bytearray(str_data(size).encode('ascii'))
    assert(pychfs.write(fd, byte_text, size) == size)
    assert(pychfs.close(fd) == 0)
    st = pychfs.stat(file1)
    assert(st['st_size'] == size)
    pychfs.truncate(file1, 0)
    st = pychfs.stat(file1)
    assert(st['st_size'] == 0)
    pychfs.truncate(file1, size)
    st = pychfs.stat(file1)
    assert(st['st_size'] == size)

@pytest.mark.dependency(depends=['test_pychfs_create_stat'])
def test_pychfs_unlink():
    pychfs.unlink(file1)
    assert(exists(file1) == False)

@pytest.fixture
def dirpath():
    return os.path.join(rootdir, "dir1")

@pytest.mark.dependency
def test_pychfs_mkdir(dirpath):
    pychfs.mkdir(dirpath, 0o0700)
    assert exists(dirpath)

@pytest.mark.dependency(depends=['test_pychfs_mkdir'])
def test_pychfs_rmdir(dirpath):
    pychfs.rmdir(dirpath)
    time.sleep(1) # wait for rmdir completion
    assert not exists(dirpath)

@pytest.fixture
def dirpath_list():
    return [os.path.join(rootdir, "dir" + str(x)) for x in range(0, 10)]

def test_pychfs_readdir(dirpath_list):
    bufsize = 256

    for d in dirpath_list:
        pychfs.mkdir(d, 0o0700)
    fd = pychfs.create(file1, 0, 0o0777)
    buf = bytearray(b'\x55') * bufsize
    assert(pychfs.close(fd) == 0)

    dirpath_list.append(file1)
    ng_list = [os.path.join(rootdir, "file2"), os.path.join(rootdir, "dir1111")]
    result = {}
    for e in dirpath_list:
        result[e] = 0

    def filler(buf, name, stat, offset):
        if name not in result:
            result[os.path.join(rootdir, name)] = 1
        return 0

    assert(pychfs.readdir(rootdir, buf, filler) == 0)
    assert(len(result.items()) == len(dirpath_list))
    for name in dirpath_list:
        assert(result[name] == 1)
    for name in ng_list:
        assert(name not in result)

def test_pychfs_seek_seekset(str_data):
    size = 256
    fd = pychfs.create(file1, os.O_RDWR, stat.S_IRWXU)
    byte_text = bytearray(str_data(size).encode('ascii'))
    assert(pychfs.write(fd, byte_text, size) == size)
    pychfs.seek(fd, 0, os.SEEK_SET)
    buf = bytearray(b'\x55') * size
    assert(pychfs.read(fd, buf, size) == size)
    assert(pychfs.close(fd) == 0)
    assert(buf == byte_text)

def test_pychfs_seek_seekcur(str_data):
    size = 256
    fd = pychfs.create(file1, os.O_RDWR, stat.S_IRWXU)
    byte_text = bytearray(str_data(size).encode('ascii'))
    assert(pychfs.write(fd, byte_text, size) == size)
    pychfs.seek(fd, -1 * size, os.SEEK_CUR)
    buf = bytearray(b'\x55') * size
    assert(pychfs.read(fd, buf, size) == size)
    assert(pychfs.close(fd) == 0)
    assert(buf == byte_text)
