#ifndef MMAP_FILE_H
#define MMAP_FILE_H

#include <unistd.h>
#include <sys/types.h>
#include "common.h"
namespace largefile
{

    class MMapFile{
        public:
            MMapFile();

            explicit MMapFile(const int fd);
            MMapFile(const MMapOption& mmap_option, const int fd);

            ~MMapFile();

            bool sync_file();
            bool map_file(const bool write = false);
            void* get_data()const; // 获取映射到内存数据的首地址
            int32_t get_size()const;

            bool munmap_file();
            bool remap_file(); // 重新执行映射
            void print_option();
        private:
            bool ensure_file_size(const int32_t size);
        private:    
            int32_t size_;
            int fd_;
            void* data_;

            struct MMapOption mmap_file_option_;
    };
} // namespace largefile


#endif