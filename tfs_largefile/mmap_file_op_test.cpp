#include "common.h"
#include "mmap_file_op.h"
#include <iostream>
const static largefile::MMapOption mmap_option = {10240000,4096,4096};
using namespace std;

int main()
{
    const char *filename = "mmap_file_op.txt";
    largefile::MMapFileOperation* mmfo = new largefile::MMapFileOperation(filename);

    int fd = mmfo->open_file();
    //cout << fd << endl;
    char buffer[5000];
    memset(buffer,'8',4098);
    int ret = mmfo->mmap_file(mmap_option);
    if(ret == largefile::TFS_ERROR)
    {
        fprintf(stderr,"mmap_file failed, reason : %s\n",strerror(errno));
        mmfo->close_file();
    }
    ret = mmfo->pwrite_file(buffer,4098,10);
    if(ret < 0)
    {
        if(ret == largefile::EXIT_DISK_OPER_INCOMPLETE)
        {
            fprintf(stderr,"pwrote_file : read length is less than required!");
        }
        else
        {
            fprintf(stderr,"pwrite file %s failed, reason : %s\n",filename, strerror(-ret));
        }
    }
    // mmfo->unlink_file();
    memset(buffer,0,4098);
    ret = mmfo->pread_file(buffer,4098,10);
    if(ret < 0)
    {
        if(ret == largefile::EXIT_DISK_OPER_INCOMPLETE)
        {
            fprintf(stderr,"pread_file : read length is less than required!");
        }
        else
        {
            fprintf(stderr,"pread file %s failed, reason : %s\n",filename, strerror(-ret));
        }
    }
    else
    {
        buffer[4098] = '\0';
        printf("read : %s\n",buffer);
    }
    ret = mmfo->flush_file();
    if(ret == largefile::TFS_ERROR)
    {
        fprintf(stderr,"flush file failed. reason : %s\n",strerror(errno));
    }
    mmfo->munmap_file();
    mmfo->close_file();
    return 0;
}