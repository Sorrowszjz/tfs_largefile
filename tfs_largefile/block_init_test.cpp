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


    //创建索引文件
    largefile::IndexHandle *indexhandle = new largefile::IndexHandle(".",blockid);
    if(debug)
    {
        printf("index init...\n");
    }
    ret = indexhandle->create(blockid,bucket_size,mmap_option);
    if(ret != TFS_SUCCESS)
    {
        fprintf(stderr,"create index %d failed.\n",blockid);
        cout << ret << endl;
        delete indexhandle;
        //delete mainblock;
        exit(-3);
    }

    //创建主块并设置大小
    
    largefile::FileOperation *mainblock = new largefile::FileOperation(mainblock_path,O_RDWR | O_CREAT | O_LARGEFILE);
    ret = mainblock->ftruncate_file(mainblock_size);
    if(ret != 0)
    {
        fprintf(stderr,"create mainblock failed(%s) , reason : %s \n" , mainblock_path.c_str(),strerror(errno));
        delete mainblock;
        mainblock = NULL;
        indexhandle->remove(blockid);
        delete indexhandle;
        exit(-2);
    }

    //其他操作
    mainblock->close_file();
    indexhandle->flush();
    delete mainblock;
    delete indexhandle;
    return 0;
}
