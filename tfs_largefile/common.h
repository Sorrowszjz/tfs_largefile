#ifndef _COMMON_H_INCLUDE
#define _COMMON_H_INCLUDE
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <errno.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdint.h>
#include <cassert>
#include <sstream>
namespace largefile
{
    const int32_t TFS_SUCCESS = 0;
    const int32_t TFS_ERROR = -1;
    const int32_t EXIT_DISK_OPER_INCOMPLETE = -6755;
    const int32_t EXIT_INDEX_ALREADY_LOADED_ERROR = -6756;
    const int32_t EXIT_META_UNEXCEPT_FOUND_ERROR = -6757;//meta found in index when insert
    const int32_t EXIT_INDEX_CORRUPT_ERROR = -6758;//index file is corrupt
    const int32_t EXIT_BLOCKID_CONFLICT_ERROR = -6759;//block id conflict
    const int32_t EXIT_BUCKET_CONFIGURE_ERROR = -6760;
    const int32_t EXIT_META_NOT_FOUND_ERROR = -6761;
    const int32_t EXIT_BLOCK_ID_ZERO_ERROR = -6762;
    static const std::string INDEX_DIR_PREFIX = "/index/";
    static const std::string MAINBLOCK_DIR_PREFIX = "/mainblock/";
    static const mode_t DIR_MODE = 0775;

    enum OperType{
        C_OPER_INSERT = 1,
        C_OPER_DELETE
    };

    struct MMapOption{
        int32_t max_mmap_size;
        int32_t first_mmap_size;
        int32_t per_mmap_size;
    };
    
    struct BlockInfo
    {
        uint32_t block_id_;         //块编号
        int32_t version_;           //块当前版本号
        int32_t file_count_;        //当前已保存文件总数
        int32_t size_;            //当前已保存文件数据总大小
        int32_t del_file_count_;    //已删除的文件数量
        int32_t del_size_;          //已删除的文件数据总大小
        uint32_t seq_no_;           //下一个可分配的文件编号
        BlockInfo()
        {
            memset(this,0,sizeof(BlockInfo));
        }
        inline bool operator==(const BlockInfo&rhs) const
        {
            return block_id_ == rhs.block_id_ && version_ == rhs.version_ && file_count_ == rhs.file_count_
            && size_ == rhs.size_ && del_file_count_ == rhs.del_file_count_ && del_size_ == rhs.del_size_ && seq_no_ == rhs.seq_no_;
        }
    };

    struct MetaInfo
    {
        MetaInfo(){init();}
        MetaInfo(const int64_t file_id, const int32_t in_offset, const int32_t file_size, const int32_t next_meta_offset)
        {
            fileid_ = file_id;
            location_.inner_offset = in_offset;
            location_.size_ = file_size;
            next_meta_offset_ = next_meta_offset;
        }

        MetaInfo(const MetaInfo& metainfo)
        {
            memcpy(this,&metainfo,sizeof(MetaInfo));
        }

        MetaInfo& operator=(const MetaInfo& metainfo)
        {
            if(this != &metainfo)
            {
                return *this;
            }
            fileid_ = metainfo.fileid_;
            location_.inner_offset = metainfo.location_.inner_offset;
            location_.size_ = metainfo.location_.size_;
            next_meta_offset_ = metainfo.next_meta_offset_;
            return *this;
        }

        MetaInfo& clone(const MetaInfo& metainfo)
        {
            assert(this == &metainfo);
            fileid_ = metainfo.fileid_;
            location_.inner_offset = metainfo.location_.inner_offset;
            location_.size_ = metainfo.location_.size_;
            next_meta_offset_ = metainfo.next_meta_offset_;
            return *this;
        }
        bool operator==(const MetaInfo& rhs) const
        {
            return fileid_ == rhs.fileid_ && location_.inner_offset == rhs.location_.inner_offset
            &&location_.size_ == rhs.location_.size_ && next_meta_offset_ == rhs.next_meta_offset_;
        }
        uint64_t get_key() const
        {
            return fileid_;
        }

        void set_key(const uint64_t key)
        {
            fileid_ = key;
        }

        uint64_t get_file_id() const
        {
            return fileid_;
        }

        void set_file_id(const uint64_t file_id)
        {
            fileid_ = file_id;
        }

        int32_t get_offset()const
        {
            return location_.inner_offset;
        }

        void set_offset(const int32_t offset)
        {
            location_.inner_offset = offset;
        }
        int32_t get_size() const
        {
            return location_.size_;
        }
        void set_size(const int32_t file_size)
        {
            location_.size_ = file_size;
        }
        int32_t get_next_meta_offset()const
        {
            return next_meta_offset_;
        }
        void set_next_meta_offset(const int32_t offset)
        {
            next_meta_offset_ = offset;
        }
    private:
        uint64_t fileid_;
        struct 
        {
            int32_t inner_offset;
            int32_t size_;
        }location_;
        int32_t next_meta_offset_;
    private:
        void init()
        {
            fileid_ = 0;
            location_.inner_offset = 0;
            location_.size_ = 0;
            next_meta_offset_ = 0;
        };
    };
} // namespace largefile


#endif