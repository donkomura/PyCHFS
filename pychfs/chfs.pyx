from pychfs cimport chfs
from pychfs cimport stat
cimport posix.stat as st
from posix.types cimport off_t, mode_t
from libc.string cimport memset
from cython cimport view
from cython.operator cimport dereference
from cpython.ref cimport PyObject
from libc.stdint cimport uintptr_t
from libc.stdlib cimport malloc, free

__codec__ = 'ascii'

def init(str s):
    return chfs.chfs_init(s.encode(__codec__))

def term():
    return chfs.chfs_term()

def set_chunk_size(size_t size):
    return chfs.chfs_set_chunk_size(size)

def create(str path, int flag, int mode):
    return chfs.chfs_create(path.encode(__codec__), flag, mode)

def create_chunk_size(str path, int flags, int mode, size_t chunk_size):
    return chfs.chfs_create_chunk_size(path.encode(__codec__), flags, mode, chunk_size)

def open(str path, int flags):
    return chfs.chfs_open(path.encode(__codec__), flags)

def close(int fd):
    return chfs.chfs_close(fd)

def pwrite(int fd, const unsigned char[:] buf, size_t size, off_t off):
    cdef const unsigned char* data = &buf[0]
    return chfs.chfs_pwrite(fd, <void*>data, size, off)

def write(int fd, const unsigned char[:] buf, size_t size):
    cdef const unsigned char* data = &buf[0]
    return chfs.chfs_write(fd, <void*>data, size)

def pread(int fd, unsigned char[:] buf, size_t size, off_t off):
    cdef unsigned char* data = &buf[0]
    return chfs.chfs_pread(fd, <void*>data, size, off)

def read(int fd, unsigned char[:] buf, size_t size):
    cdef unsigned char* data = &buf[0]
    return chfs.chfs_read(fd, <void*>data, size)

def seek(int fd, off_t off, int whence):
    return chfs.chfs_seek(fd, off, whence)

def fsync(int fd):
    return chfs.chfs_fsync(fd)

def truncate(str path, off_t ln):
    return chfs.chfs_truncate(path.encode(__codec__), ln)

def unlink(str path):
    return chfs.chfs_unlink(path.encode(__codec__))

def mkdir(str path, mode_t mode):
    return chfs.chfs_mkdir(path.encode(__codec__), mode)

def rmdir(str path):
    return chfs.chfs_rmdir(path.encode(__codec__))

def stat(str path):
    cdef view.array va = view.array(shape=(1,), itemsize=sizeof(stat.struct_stat), format='B', mode='c', allocate_buffer=False)
    va.data = <char*>malloc(sizeof(stat.struct_stat))
    va.callback_free_data = free
    cdef int ret = chfs.chfs_stat(path.encode(__codec__), <stat.struct_stat*>va.data)
    if ret is not 0:
        raise ValueError("chfs stat returns non zero value")
    return dereference(<stat.struct_stat*>va.data)

cdef int _readdir_cb(void *request, const char* path, const stat.struct_stat* st, off_t offset):
    cdef dict req = <dict>request
    cdef stat.struct_stat _st = dereference(st)
    return (<object>req["cb"])(req["buf"], path.decode(__codec__), _st, offset)

def readdir(str path, char[:] buf, filler):
    cdef dict req = {"buf": buf, "cb": filler}
    return chfs.chfs_readdir(path.encode(__codec__), <void*><PyObject*>req, _readdir_cb)

def readdir_index(str path, int index, char[:] buf, filler):
    cdef dict req = {"buf": buf, "cb": filler}
    return chfs.chfs_readdir_index(path.encode(__codec__), index, <void*><PyObject*>req, _readdir_cb)

def symlink(str target, str path):
    return chfs.chfs_symlink(target.encode(__codec__), path.encode(__codec__))

def readlink(str path, bytes buf, size_t size):
    return chfs.chfs_readlink(path.encode(__codec__), <char *>buf, size)

