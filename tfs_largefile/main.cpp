#include "common.h"
#include "mmap_file.h"


using namespace std;
using namespace largefile;
static const mode_t OPEN_MODE = 0644;
const static largefile::MMapOption mmap_option = {10240000,4096,4096};
int open_file(string file_name,int open_flags)
{
    int fd = open(file_name.c_str(), open_flags, OPEN_MODE);
    if(fd < 0)
    {
        return -errno;
    }
    return fd;
}

using std::cout;
using std::endl;
int main()
{
    // open or create a file , get fd
    const char *filename = "./mapfile_test.txt";
    int fd = open_file(filename,O_RDWR | O_CREAT | O_LARGEFILE);

    if(fd < 0)
    {
        fprintf(stderr,"open file failed. filename : %s , desc : %s\n",filename,strerror(-fd));
        return -1;
    }
    largefile::MMapFile *map_file = new largefile::MMapFile(mmap_option,fd);


    bool is_mapped = map_file->map_file(true);
    if(is_mapped)
    {
        map_file->remap_file();
        map_file->remap_file();
        map_file->remap_file();
        map_file->remap_file();
        memset(map_file->get_data(),'9',map_file->get_size());
        map_file->sync_file();
        map_file->munmap_file();
    }else
    {
        fprintf(stderr,"map file failed\n");
    }
    close(fd);
    return 0;
}