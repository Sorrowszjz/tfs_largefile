#include "common.h"
#include "index_handle.h"
#include "file_op.h"
#include "mmap_file_op.h"
#include "mmap_file.h"
#include <sstream>
#include <string>
#include <unistd.h>
using namespace largefile;
using namespace std;
static int debug = 1;
const static largefile::MMapOption mmap_option = {1024000,4096,4096};
const static uint32_t mainblock_size = 1024*1024*64; //64M
const static uint32_t bucket_size = 1000;
static int32_t blockid = 1;
int main()
{
    //cout << getpid() << endl;
    cout << "Input Blockid : " << endl;
    cin >> blockid;
    if(blockid < 1)
    {
        cerr << "blockid must greater or equeal than 1 , exit" << endl;
        exit(-1);
    }


    std::string mainblock_path;
    std::stringstream tmp_stream;
    tmp_stream << "." << MAINBLOCK_DIR_PREFIX << blockid;
    tmp_stream >> mainblock_path;
    cout << mainblock_path << endl;
    int32_t ret = TFS_SUCCESS;


    //加载索引文件
    largefile::IndexHandle *indexhandle = new largefile::IndexHandle(".",blockid);
    if(debug)
    {
        printf("load init...\n");
    }
    ret = indexhandle->load(blockid,bucket_size,mmap_option);
    if(ret != TFS_SUCCESS)
    {
        fprintf(stderr,"load index %d failed.\n",blockid);
        //cout << ret << endl;
        delete indexhandle;
        //delete mainblock;
        exit(-2);
    }

    //写入文件到主块文件中
    
    largefile::FileOperation *mainblock = new largefile::FileOperation(mainblock_path,O_RDWR | O_CREAT | O_LARGEFILE);
    char buffer[4096];
    memset(buffer,'6',sizeof(buffer));
    int32_t data_offset = indexhandle->get_block_data_offset();
    uint32_t file_no = indexhandle->block_info()->seq_no_;
    if((ret = mainblock->pwrite_file(buffer,sizeof(buffer),data_offset)) != TFS_SUCCESS)
    {
        fprintf(stderr,"mainblock write file failed.ret : %d , reason : %s \n",ret,strerror(errno));
        mainblock->close_file();
        delete mainblock;
        delete indexhandle;
    }

    MetaInfo meta;
    meta.set_file_id(file_no);
    meta.set_offset(data_offset);
    meta.set_size(sizeof(buffer));
    //std::cout << "meta.get_size() : " << meta.get_size() << std::endl;
    ret = indexhandle->write_segment_meta(meta.get_key(),meta);

    //cout << "------------------------" << endl;
    //cout << indexhandle->block_info()->del_size_ << endl;
    //cout << "------------------------" << endl;
    
    if(ret == TFS_SUCCESS)
    {   
        //更新索引头部信息
        indexhandle->commit_block_data_offset(sizeof(buffer));
        //更新块信息
        indexhandle->update_block_info(C_OPER_INSERT,sizeof(buffer));
        ret = indexhandle->flush();
        if(ret != TFS_SUCCESS)
        {
            fprintf(stderr,"flush file failed, blockid : %d , file_num : %u\n" , blockid,file_no);

        }
    }else{
        fprintf(stderr,"write_sement_meta failed , mainblock : %d , file_no : %u\n",blockid,file_no);
    }

    if(ret != TFS_SUCCESS)
    {
        fprintf(stderr,"write to mainblock failed , mainblock : %d , file_no : %u\n",blockid,file_no);
    }else
    {
        if(debug) printf("write successfully. file_no : %u , blockid : %d \n", file_no , blockid);
    }
    //其他操作
    mainblock->close_file();
    delete mainblock;
    delete indexhandle;
    return 0;
}
