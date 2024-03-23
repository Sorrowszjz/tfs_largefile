#include "file_op.h"
#include "common.h"

using namespace std;
int main()
{
    const char *file_name = "file_op.txt";
    largefile :: FileOperation *fileOP = new largefile :: FileOperation(file_name,O_CREAT|O_LARGEFILE|O_RDWR);


    int fd = fileOP->open_file();
    if(fd < 0)
    {
        fprintf(stderr,"open file \"%s\" failed : %s \n",file_name,strerror(-fd));
        exit(-1);
    }

    char buffer[65];
    memset(buffer,'6',64);
    int ret = fileOP->pwrite_file(buffer,64,0);
    if(ret < 0)
    {
        fprintf(stderr,"pwrite file \"%s\" failed , reason : %s\n",file_name, strerror(-ret));
    }
    memset(buffer,0,64);
    ret = fileOP->pread_file(buffer,64,0);
    if(ret < 0)
    {
        if(ret == largefile :: EXIT_DISK_OPER_INCOMPLETE)
        {
            fprintf(stderr,"pread file \"%s\" failed",file_name);
        }
        else 
        {
            fprintf(stderr,"pread file \"%s\" failed , reason : %s\n",file_name, strerror(-ret));
        }
    }else
    {
        buffer[64] = '\0';
        printf("read : %s\n",buffer);
    }
    memset(buffer,'9',64);
    fileOP->write_file(buffer,64);
    fileOP->close_file();
    //fileOP->unlink_file();
    return 0;
}