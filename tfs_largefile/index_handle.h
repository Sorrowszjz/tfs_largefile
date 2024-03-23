#ifndef TFS_INDEX_HANDLE_H_
#define TFS_INDEX_HANDLE_H_
#include "common.h"
#include "mmap_file_op.h"
#include "mmap_file.h"
#include "file_op.h"

namespace largefile{
    struct IndexHeader{
        public:
            IndexHeader()
            {
                memset(this,0,sizeof(IndexHeader));
            }
            BlockInfo block_info_;  //meta block info
            int32_t bucket_size_;   //hash bucket size
            int32_t data_file_offset_;   //position to write
            int32_t index_file_size_;   //index file offset
            int32_t free_head_offset_;  //free meta node list , reuse
    };
    class IndexHandle 
    {
        public:
            IndexHandle(const std::string& base_path,const uint32_t main_block_id);
            ~IndexHandle();
            int create(const uint32_t logic_block_id,const int32_t bucket_size,const MMapOption map_option);
            int load(const uint32_t logic_block_id,const int32_t bucket_size,const MMapOption map_option);
            int remove(const uint32_t logic_block_id);
            int flush();
            IndexHeader* index_header()
            {
                return reinterpret_cast<IndexHeader*>(file_op_->get_map_data());
            }
            BlockInfo* block_info()
            {
                return reinterpret_cast<BlockInfo*>(file_op_->get_map_data());
            }
            int32_t bucket_size()const
            { 
                return reinterpret_cast<IndexHeader*>(file_op_->get_map_data())->bucket_size_;
            }
            int32_t get_block_data_offset()const
            {
                return reinterpret_cast<IndexHeader*>(file_op_->get_map_data())->data_file_offset_;
            }
            int32_t free_head_offset()const
            {
                return reinterpret_cast<IndexHeader*>(file_op_->get_map_data())->free_head_offset_;
            }
            void commit_block_data_offset(const int file_size)
            {
               ( reinterpret_cast<IndexHeader*>(file_op_->get_map_data()))->data_file_offset_ += file_size;
            }
            int update_block_info(const OperType oper_type, const uint32_t modify_size);
            int32_t write_segment_meta(const uint64_t key, MetaInfo &meta);
            int32_t read_segment_meta(const uint64_t key, MetaInfo &meta);
            int32_t delete_sement_meta(const uint64_t key);
            int32_t hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset);
            int32_t hash_insert(const uint64_t key, int32_t previos_offset, MetaInfo& meta);
            int32_t* bucket_slot()//返回hash数组
            {
                return reinterpret_cast<int32_t*>(reinterpret_cast<char *>(file_op_->get_map_data()) + sizeof(IndexHeader));
            }
        private:
            MMapFileOperation *file_op_;
            bool is_load_;
            bool hash_compare(const uint64_t left_key, const uint64_t right_key)
            {
                return (left_key == right_key);
            }
    };
}

#endif //TFS_INDEX_HANDLE_H_