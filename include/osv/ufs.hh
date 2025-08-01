#ifndef UFS_HH
#define UFS_HH

#include <osv/mmu.hh>
#include <atomic>
#include <vector>
#include <algorithm>
#include <tuple>
#include <cassert>
#include <iostream>
#include <unistd.h>
#include <immintrin.h>
#include <cmath>
#include <cstring>
#include <bitset>
#include <fstream>
#include <sys/mman.h>
#include "drivers/poll_nvme.hh"

#include <osv/types.h>
#include <osv/sched.hh>
#include <osv/interrupt.hh>
#include <osv/percpu.hh>
#include <osv/llfree.h>
#include <osv/power.hh>

#include <ext4_inode.h>
#include <ext4_fs.h>

namespace ucache {
class ufs;
const u64 lb_size = 512;
const u64 ext4_block_size = 4096;
const bool ring = true;

class aio_req_t {
    public:
    unvme_iod_t* nvme_iods;
    size_t size;

    aio_req_t(size_t size){
        nvme_iods = (unvme_iod_t*)malloc(size*sizeof(unvme_iod_t));
    }

    ~aio_req_t(){
        free(nvme_iods);
    }
};

class ufile {
    public:
    char* name;
    u64 size; // keep track of name and size for convenience
    ufs* fs; //filesystem this file belongs to
    struct file* fp; // keep the fp for convenience

    u64 current_seek_pos;
    ext4_inode_ref *inode;

    ufile(const char* name, ufs* fs);
    void read(void* buf, u64 offset, u64 size);
    void write(void* buf, u64 offset, u64 size);
    aio_req_t* aread(void* buf, u64 offset, u64 size);
    aio_req_t* awrite(void* buf, u64 offset, u64 size);
    void poll(aio_req_t* reqs);
    void commit_io();//std::vector<int> &devices); 

    void getLBAs(std::vector<u64> &blocks, u64 offset, u64 size);
};

class ufs{
    public:
    std::vector<const unvme_ns_t*> devices;
    std::vector<ufile*> files; // keep track of opened files for convenience
    struct ext4_fs* fs;

    ufs();

    void add_device(const unvme_ns_t* ns);
    ufile* open_ufile(const char* name);
};

};
#endif
