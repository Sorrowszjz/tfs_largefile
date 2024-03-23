#include "file_op.h"
#include "common.h"
namespace largefile
{
    FileOperation :: FileOperation(const std::string& file_name, const int flags):
    fd_(-1) , open_flags_(flags)
    {
        file_name_ = strdup(file_name.c_str());
    }
    FileOperation :: ~FileOperation()
    {
        if(fd_ > 0)
        {
            ::close(fd_);
        }
        if(NULL != file_name_)
        {
            delete file_name_;
            file_name_ = NULL;
        }
    }
    int FileOperation :: open_file()
    {
        if(fd_ > 0)
        {
            close(fd_);
            fd_ = -1;
        }
        fd_ = ::open(file_name_,open_flags_,OPEN_MODE);
        if(fd_ < 0)
        {
            return -errno;
        }
        return fd_;
    }
    void FileOperation :: close_file()
    {
        if(fd_ < 0) return;
        ::close(fd_);
        fd_ = -1;
    }
    int64_t FileOperation ::  get_file_size()
    {
        int fd = check_file();
        if(fd < 0)
        {
            return -1;
        }
        struct stat buf;
        if(fstat(fd,&buf) != 0 )
        {
            return -1;
        }
        return buf.st_size;
    }
    int FileOperation :: check_file()
    {
        if(fd_ < 0)
        {
            fd_ = open_file();
        }
        return fd_;
    }
    int FileOperation :: ftruncate_file(const int64_t length)
    {
        int fd = check_file();
        if(fd < 0)
        {
            return fd;
        }
        return ftruncate(fd , length);
    }
    int FileOperation :: seek_file(const int64_t offset)
    {
        int fd = check_file();
        if(fd < 0)
        {
            return fd;
        }
        return lseek(fd,offset,SEEK_SET);
    }
    int FileOperation :: flush_file()
    {
        if(open_flags_ & O_SYNC)
        {
            return 0;
        }
        int fd = check_file();
        if(fd < 0)
        {
            return fd;
        }
        return fsync(fd);
    }
    int FileOperation :: unlink_file()
    {
        close_file();
        return ::unlink(file_name_);
    }
    int FileOperation :: pread_file(char *buf, const int32_t nbytes, const int64_t offset)
    {
        int32_t left = nbytes;
        int64_t read_offset = offset;
        int32_t read_len = 0;
        char *p_tmp = buf;
        int i = 0;
        while(left > 0)
        {
            ++i;
            if(i >= MAX_DISK_TIMES)
            {
                break;
            }
            if(check_file() < 0)
            {
                return -errno;
            }
            read_len = ::pread64(fd_,p_tmp,left,read_offset);
            if(read_len < 0)
            {
                read_len = -errno;

                if(-read_len == EINTR || EAGAIN == -read_len)
                {
                    continue;
                }
                else if(EBADF == -read_len)
                {
                    fd_ = -1;
                    continue;
                }
                else
                {
                    return read_len;
                }
            }
            else if(0 == read_len)
            {
                break;
            }
            left -= read_len;
            p_tmp += read_len;
            read_offset += read_len;

        }
        if(left != 0)
        {
            return EXIT_DISK_OPER_INCOMPLETE;
        }
        return TFS_SUCCESS;
    }
    int FileOperation :: pwrite_file(char *buf, const int32_t nbytes, const int64_t offset)
    {
        int32_t left = nbytes;
        int64_t write_offset = offset;
        int32_t written_len = 0;
        char *p_tmp = buf;
        int i = 0;
        while(left > 0)
        {
            ++i;
            if(i >= MAX_DISK_TIMES)
            {
                break;
            }
            if(check_file() < 0)
            {
                return -errno;
            }
            written_len = ::pwrite64(fd_,p_tmp,left,write_offset);
            if(written_len < 0)
            {
                written_len = -errno;

                if(-written_len == EINTR || EAGAIN == -written_len)
                {
                    continue;
                }
                else if(EBADF == -written_len)
                {
                    fd_ = -1;
                    continue;
                }
                else
                {
                    return written_len;
                }
            }
            left -= written_len;
            p_tmp += written_len;
            write_offset += written_len;

        }
        if(left != 0)
        {
            return EXIT_DISK_OPER_INCOMPLETE;
        }
        return TFS_SUCCESS;
    }
    int FileOperation :: write_file(const char *buf, const int32_t nbytes)
    {
        int32_t left = nbytes;
        int32_t written_len = 0;
        const char *p_tmp = buf;
        int i = 0;
        while(left > 0)
        {
            ++i;
            if(i >= MAX_DISK_TIMES)
            {
                break;
            }
            if(check_file() < 0)
            {
                return -errno;
            }
            written_len = ::write(fd_,p_tmp,left);
            if(written_len < 0)
            {
                written_len = -errno;

                if(-written_len == EINTR || EAGAIN == -written_len)
                {
                    continue;
                }
                else if(EBADF == -written_len)
                {
                    fd_ = -1;
                    continue;
                }
                else
                {
                    return written_len;
                }
            }
            left -= written_len;
            p_tmp += written_len;
        }
        if(left != 0)
        {
            return EXIT_DISK_OPER_INCOMPLETE;
        }
        return TFS_SUCCESS;
    }
}