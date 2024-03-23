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
    //cout << indexhandle->block_info()->del_size_ << endl;
    delete indexhandle;
    return 0;
}
