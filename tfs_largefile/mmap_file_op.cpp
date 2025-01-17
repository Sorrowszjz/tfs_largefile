#include "common.h"
#include "mmap_file_op.h"
#include "file_op.h"
#include "mmap_file.h"

static int debug = 1;

namespace largefile
{
    int MMapFileOperation :: mmap_file(const MMapOption& mmap_option)
    {
        //检查映射参数
        if(mmap_option.max_mmap_size <= 0 || mmap_option.max_mmap_size < mmap_option.first_mmap_size)
        {
            return TFS_ERROR;
        }
        //检查文件是否打开
        int fd = check_file();
        if(fd < 0)
        {
            fprintf(stderr, "MMapFileOperation :: mmap_file - checking file failed !");
            return TFS_ERROR;
        }
        //检查映射对象是否创建 ， 然后执行映射
        if(!is_mapped_)
        {
            if(map_file_)
            {
                delete(map_file_);
            }
            map_file_ = new MMapFile(mmap_option,fd);
            is_mapped_ = map_file_->map_file(1);
        }
        if(is_mapped_)
        {
            return TFS_SUCCESS;
        }
        return TFS_ERROR;
    }
    int MMapFileOperation :: munmap_file()
    {
        if(is_mapped_ && map_file_ != NULL)
        {
            delete(map_file_); //在析构的时候会执行同步和解除映射
            is_mapped_ = 0;
        }
        return TFS_SUCCESS;
    }

    void* MMapFileOperation :: get_map_data() const 
    {
        if(is_mapped_)
        {
           return map_file_->get_data();
        }
        return NULL;
    }

    int MMapFileOperation :: pread_file(char *buf, const int32_t size , const int64_t offset)
    {
        //内存已经映射
        if(is_mapped_ && (offset + size) > map_file_->get_size())
        {
            if(debug) fprintf(stdout, " MMapFileOperation :: pread_file : pread size : %d , offset : %" __PRI64_PREFIX"d, map file size : %d. need remap\n",size, offset, map_file_->get_size());
            map_file_->remap_file();
        }

        if(is_mapped_ && (offset + size) <= map_file_->get_size())
        {
            memcpy(buf, (char*)map_file_->get_data() + offset,size);
            return TFS_SUCCESS;
        }
        //要读取的数据映射不全或者没有映射
        return FileOperation :: pread_file(buf, size, offset);//从磁盘读取
    }

    int MMapFileOperation :: pwrite_file( char* buf, const int32_t size, const int64_t offset)
    {
        if(is_mapped_ && (offset + size) > map_file_->get_size())
        {
            if(debug) fprintf(stdout, " MMapFileOperation :: pwrite _file : pwrite size : %d , offset : %" __PRI64_PREFIX"d, map file size : %d. need remap\n",size, offset, map_file_->get_size());
            map_file_->remap_file();
        }
        if(is_mapped_ && (offset + size) <= map_file_->get_size())
        {
            memcpy((char*)map_file_->get_data() + offset, buf, size);
            return TFS_SUCCESS;
        }
        return FileOperation :: pwrite_file(buf,size,offset);
    }
    int MMapFileOperation :: flush_file()
    {
        if(is_mapped_)
        {
            if(map_file_->sync_file())
            {
                return TFS_SUCCESS;
            }
            return TFS_ERROR;
        }
        return FileOperation :: flush_file();
    }
}