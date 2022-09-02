cimport posix.stat as st

cdef extern from "<sys/stat.h>" nogil:
    cdef struct struct_stat "stat":
        st.dev_t   st_dev
        st.ino_t   st_ino
        st.mode_t  st_mode
        st.nlink_t st_nlink
        st.uid_t   st_uid
        st.gid_t   st_gid
        st.dev_t   st_rdev
        st.off_t   st_size
        st.blksize_t st_blksize
        st.blkcnt_t st_blocks
        st.time_t  st_atime
        st.time_t  st_mtime
        st.time_t  st_ctime

