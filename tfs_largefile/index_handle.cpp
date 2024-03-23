#include "common.h"
#include "index_handle.h"
static int debug = 1;
namespace largefile{
    IndexHandle::IndexHandle(const std::string& base_path, const uint32_t main_block_id)
    {
        std::stringstream tmp_stream;
        tmp_stream << base_path << INDEX_DIR_PREFIX << main_block_id;
        std::string index_path;
        tmp_stream >> index_path;
        //std::cout << "index path : " << index_path << std::endl;
        file_op_ = new MMapFileOperation(index_path,O_CREAT | O_RDWR | O_LARGEFILE);
        is_load_ = false;
    }
    IndexHandle::~IndexHandle()
    {
        if(file_op_)
        {
            delete file_op_;
            file_op_ = NULL;
        }
    }
    int IndexHandle::create(const uint32_t logic_block_id,const int32_t bucket_size,const MMapOption map_option)
    {
        if(debug)
        {
            printf("create index . block : %u , bucket size : %d,max_mmap_size : %d , first_mmap_size : %d , per_mmap_size : %d\n",
            logic_block_id,bucket_size,map_option.max_mmap_size,map_option.first_mmap_size,map_option.per_mmap_size);
        }
        int ret = TFS_SUCCESS;
        if(is_load_)
        {
            return EXIT_INDEX_ALREADY_LOADED_ERROR;
        }
        int64_t file_size = file_op_->get_file_size();
        //std::cout << "file_size : " << file_size << std::endl; 
        if(file_size < 0)
        {
            return TFS_ERROR;
        }
        else if(file_size == 0) //empty file
        {
            IndexHeader i_header;
            i_header.block_info_.block_id_ = logic_block_id;
            i_header.block_info_.seq_no_ = 1;
            i_header.bucket_size_ = bucket_size;
            i_header.index_file_size_ = sizeof(IndexHeader) + bucket_size*sizeof(int32_t);

            //index header + total bucket
            char *init_data = new char[i_header.index_file_size_];
            memcpy(init_data,&i_header,sizeof(IndexHeader));
            memset(init_data + sizeof(IndexHeader) , 0 , i_header.index_file_size_ - sizeof(IndexHeader));

            //write into index file 
            ret = file_op_->pwrite_file(init_data,i_header.index_file_size_,0);
            if(ret != TFS_SUCCESS)
            {
                return ret;
            }
            ret = file_op_->flush_file();
            if(ret != TFS_SUCCESS)
            {
                return ret;
            }
            delete[] init_data;
            init_data = NULL;
        }
        else                     //index file already exist
        {
            return EXIT_META_UNEXCEPT_FOUND_ERROR;
        }
        ret = file_op_->mmap_file(map_option);
        if(ret != TFS_SUCCESS)
        {
            return ret;
        }
        is_load_ = true;
        if(debug)
        {
            printf("create blockid : %u index successful. data file size : %d, index file size : %d , bucket size : %d, free head offset : %d , seq_no : %u , size_ : %d , file_count : %d , del_size_ : %d ,del_file_count : %d , version_ : %d\n ",
            logic_block_id,index_header()->data_file_offset_,index_header()->index_file_size_,index_header()->bucket_size_,
            index_header()->free_head_offset_,block_info()->seq_no_,block_info()->size_,block_info()->file_count_,block_info()->del_size_,
            block_info()->del_file_count_,block_info()->version_);
        } 
        return TFS_SUCCESS;
    }
    int IndexHandle::load(const uint32_t logic_block_id,const int32_t bucket_size,const MMapOption map_option)
    {
        int ret = TFS_SUCCESS;
        //1.is already loaded
        if(is_load_)    
        {
            return EXIT_INDEX_ALREADY_LOADED_ERROR;
        }
        //2.check file size 
        
        int64_t file_size = file_op_->get_file_size();
        if(file_size < 0)
        {
            return file_size;
        }else if(file_size == 0)
        {
            return EXIT_INDEX_CORRUPT_ERROR;
        }
        MMapOption tmp_map_option = map_option;
            
        if(file_size > map_option.first_mmap_size &&file_size <= map_option.max_mmap_size)
        {
            tmp_map_option.first_mmap_size = file_size;
        }
        //3. exec mmap
        ret = file_op_->mmap_file(tmp_map_option);
        if(ret != TFS_SUCCESS)
        {
            return ret;
        }
        //4.check mmapped argvs
        int32_t index_file_size = sizeof(IndexHeader) + this->bucket_size()*sizeof(int32_t);
        if(file_size < index_file_size)
        {
            return EXIT_INDEX_CORRUPT_ERROR;
        }
        if(bucket_size == 0 || block_info()->block_id_ == 0)
        {
            return EXIT_INDEX_CORRUPT_ERROR;
        }
        if(logic_block_id != block_info()->block_id_)
        {
            return EXIT_BLOCKID_CONFLICT_ERROR;
        }
        if(bucket_size != this->bucket_size())
        {
            return EXIT_BUCKET_CONFIGURE_ERROR;
        }
        is_load_ = true;
        if(debug)
        {
            printf("load blockid : %u index successful. data file size : %d, index file size : %d , bucket size : %d, free head offset : %d , seq_no : %u , size_ : %d , file_count : %d , del_size_ : %d ,del_file_count : %d , version_ : %d\n ",
            logic_block_id,index_header()->data_file_offset_,index_header()->index_file_size_,index_header()->bucket_size_,
            index_header()->free_head_offset_,block_info()->seq_no_,block_info()->size_,block_info()->file_count_,block_info()->del_size_,
            block_info()->del_file_count_,block_info()->version_);
        }
        return TFS_SUCCESS;
    }
    int IndexHandle:: remove(const uint32_t logic_block_id)
    {
        if(is_load_)
        {
            if(logic_block_id != block_info()->block_id_)
            {
                fprintf(stderr,"block id confict. blockid : %d , index block id : %d\n",logic_block_id,block_info()->block_id_);
                return EXIT_BLOCKID_CONFLICT_ERROR;
            }
        }
        int ret = file_op_->munmap_file();
        if(ret != TFS_SUCCESS)
        {
            return ret;
        }
        ret = file_op_->unlink_file();
        return ret;
    }
    int IndexHandle::flush()
    {
        int ret = file_op_->flush_file();
        if(ret != TFS_SUCCESS)
        {
            fprintf(stderr,"index flush error.%s\n",strerror(errno));
        }
        return ret;
    }
    int IndexHandle::update_block_info(const OperType oper_type, const uint32_t modify_size)
    {
        //std::cout << "modify size : " << modify_size << std::endl;
        if(block_info()->block_id_ == 0)
        {
            return EXIT_BLOCK_ID_ZERO_ERROR;
        }
        if(oper_type == C_OPER_INSERT)
        {
            ++(block_info()->version_);
            ++(block_info()->file_count_);
            ++(block_info()->seq_no_);
            block_info()->size_ += modify_size;
        }
        else if(oper_type == C_OPER_DELETE)
        {
            ++block_info()->version_;
            --block_info()->file_count_;
            block_info()->size_ -= modify_size;
            block_info()->del_size_ += modify_size;
            ++block_info()->del_file_count_;
        }
        if(debug)
        {
            printf("update block info. blockid: %u, version: %u, file count: %u, size: %u, del file count: %u, del size: %u, seq no: %u, oper type: %d\n",
            block_info()->block_id_, block_info()->version_, block_info()->file_count_, block_info()->size_,
            block_info()->del_file_count_, block_info()->del_size_, block_info()->seq_no_, oper_type);
        }
        return TFS_SUCCESS;
    }

    int32_t IndexHandle::write_segment_meta(const uint64_t key, MetaInfo &meta)
    {
        int32_t current_offset = 0;
        int32_t previous_offset = 0;
        //查找key是否存在
        int ret = hash_find(key,current_offset,previous_offset);

        if(ret == TFS_SUCCESS)
        {
            return EXIT_META_UNEXCEPT_FOUND_ERROR;
        }
        else if(ret != EXIT_META_NOT_FOUND_ERROR)
        {
            return ret;
        }
        //key 不存在，执行插入操作
        ret = hash_insert(key,previous_offset,meta);
        return ret;
    }
    int32_t IndexHandle::read_segment_meta(const uint64_t key, MetaInfo &meta)
    {
        int32_t current_offset = 0;
        int32_t previous_offset = 0;
        int ret = TFS_SUCCESS;
        ret = hash_find(key,current_offset,previous_offset);
        if(ret == TFS_SUCCESS)
        {
            ret = file_op_->pread_file(reinterpret_cast<char *>(&meta),sizeof(MetaInfo),current_offset);
            return ret;
        }
        else
        {
            return ret;
        }
    }
    int32_t IndexHandle::hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset)
    {
        int ret = TFS_SUCCESS;
        MetaInfo meta;
        current_offset = 0;
        previous_offset = 0;
        
        int32_t slot = key % bucket_size();//找到桶编号

        int32_t pos = bucket_slot()[slot];//当前桶内第一个meta节点在索引文件中的偏移

        while(pos != 0)
        {
            //读出meta
            ret = file_op_->pread_file(reinterpret_cast<char *>(&meta),sizeof(MetaInfo),pos);
            if(ret != TFS_SUCCESS)
            {
                return ret;
            }
            if(hash_compare(key,meta.get_key()))
            {
                current_offset = pos;
                return TFS_SUCCESS;
            }
            previous_offset = pos;
            pos = meta.get_next_meta_offset();
        }
        return EXIT_META_NOT_FOUND_ERROR;
    }
    int32_t IndexHandle::hash_insert(const uint64_t key, int32_t previos_offset, MetaInfo& meta)
    {
        int ret = TFS_SUCCESS;
        //get slot
        int slot = static_cast<uint32_t>(key) % bucket_size();
        MetaInfo tmp_meta_info;
        int32_t current_offset = 0;
        //write meta
        if(free_head_offset() != 0)
        {
            ret = file_op_->pread_file(reinterpret_cast<char *>(&tmp_meta_info) , sizeof(MetaInfo),free_head_offset());
            if(ret != TFS_SUCCESS)
            {
                return ret;
            }
            current_offset = free_head_offset();
            index_header()->free_head_offset_ = tmp_meta_info.get_next_meta_offset();
            if(debug)
            {
                printf("metainfo reuse.current_offset : %d\n",current_offset);
            }
        }
        else
        {
            current_offset = index_header()->index_file_size_;
            index_header()->index_file_size_ += sizeof(MetaInfo);

            meta.set_next_meta_offset(0);
            ret = file_op_->pwrite_file(reinterpret_cast<char *>(&meta),sizeof(MetaInfo),current_offset);    

            if(ret !=TFS_SUCCESS)
            {
                index_header()->index_file_size_ -= sizeof(MetaInfo);
                return ret;
            }
        }

        //link to hash slot
        //1.if hash slot is not empty
        if(previos_offset != 0)
        {

            ret = file_op_->pread_file(reinterpret_cast<char *>(&tmp_meta_info),sizeof(MetaInfo),previos_offset);
            if(ret != TFS_SUCCESS)
            {
                index_header()->index_file_size_ -= sizeof(MetaInfo);
                return ret;
            }
            tmp_meta_info.set_next_meta_offset(current_offset);
            ret = file_op_->pwrite_file(reinterpret_cast<char *>(&tmp_meta_info),sizeof(MetaInfo),previos_offset);
            if(ret != TFS_SUCCESS)
            {
                index_header()->index_file_size_ -= sizeof(MetaInfo);
                return ret;
            }
        }
        //2.hash slot is empty
        else 
        {
            bucket_slot()[slot] = current_offset;
        }
        return TFS_SUCCESS;
    }
    int32_t IndexHandle::delete_sement_meta(const uint64_t key)
    {
        //找到带删除节点的前后节点
        int32_t current_offset = 0;
        int32_t previous_offset = 0;
        MetaInfo metainfo;
        int ret = TFS_SUCCESS;
        ret = hash_find(key,current_offset,previous_offset);
        if(ret != TFS_SUCCESS)
        {
            return ret;
        }
        ret = file_op_->pread_file(reinterpret_cast<char *>(&metainfo),sizeof(MetaInfo),current_offset);
        //std::cout << "meta.get_size() : " << metainfo.get_size() << std::endl;
        if(ret != TFS_SUCCESS)
        {
            return ret;
        }
        //读出当前节点的下一个位置
        int32_t next_pos = metainfo.get_next_meta_offset();
        //如果上一个节点不是头节点，就把上一个节点的next指针指向下一个meta节点
        if(previous_offset != 0)
        {
            //read
            MetaInfo pre_meta;
            ret = file_op_->pread_file(reinterpret_cast<char *>(&pre_meta),current_offset,previous_offset);
            if(ret != TFS_SUCCESS)
            {
                return ret;
            }
            //modify
            pre_meta.set_next_meta_offset(next_pos);
            //write back
            ret = file_op_->pwrite_file(reinterpret_cast<char *>(&pre_meta),sizeof(MetaInfo),previous_offset);
            if(ret != TFS_SUCCESS)
            {
                return ret;
            }
        }
        else
        {
            int32_t slot = static_cast<uint32_t>(key)%bucket_size();
            bucket_slot()[slot] = next_pos;
        }
        //reuseable meta node 

        metainfo.set_next_meta_offset(free_head_offset());
        ret = file_op_->pwrite_file(reinterpret_cast<char *>(&metainfo),sizeof(MetaInfo),current_offset);
        if(ret != TFS_SUCCESS)
        {
            return ret;
        }
        index_header()->free_head_offset_ = current_offset;
        if(debug)
        {
            printf("delete_segment_meta-reuse_meta_info.current_offset : %d\n",current_offset);
        }

        //update
        //std::cout << "meta.get_size() : " << metainfo.get_size() << std::endl;
        update_block_info(C_OPER_DELETE,metainfo.get_size());

        return TFS_SUCCESS;
    }

}