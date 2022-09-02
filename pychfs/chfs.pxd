from libc.stdint cimport int32_t
from posix.types cimport off_t
from libc.string cimport strlen
from libc.stdio cimport sprintf
cimport posix.stat as st
from pychfs cimport stat

ctypedef int (*filler_t)(void *, const char *, const stat.struct_stat *, off_t)

cdef extern from "<chfs.h>":
    int chfs_init(const char *server)
    int chfs_term()
    void chfs_set_chunk_size(int chunk_size)
    int chfs_create(const char *path, int32_t flags, st.mode_t mode)
    int chfs_create_chunk_size(const char *path, int32_t flags, st.mode_t mode,\
            int chunk_size)
    int chfs_open(const char *path, int32_t flags)
    int chfs_close(int fd)
    ssize_t chfs_pwrite(int fd, const void *buf, size_t size, off_t offset)
    ssize_t chfs_write(int fd, const void *buf, size_t size)
    ssize_t chfs_pread(int fd, void *buf, size_t size, off_t offset)
    ssize_t chfs_read(int fd, void *buf, size_t size)
    off_t chfs_seek(int fd, off_t off, int whence)
    int chfs_fsync(int fd)
    int chfs_truncate(const char *path, off_t len)
    int chfs_unlink(const char *path)
    int chfs_mkdir(const char *path, st.mode_t mode)
    int chfs_rmdir(const char *path)
    int chfs_readdir(const char *path, void *buf, filler_t filler)
    int chfs_readdir_index(const char *path, int index, void *buf, filler_t filler)
    int chfs_stat(const char *path, stat.struct_stat * st)
    int chfs_symlink(const char *target, const char *path)
    int chfs_readlink(const char *path, char *buf, size_t size)
    unsigned CHFS_S_IFREP
