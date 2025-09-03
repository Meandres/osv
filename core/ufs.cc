#include <osv/ucache.hh>
#include <osv/mmu.hh>
#include <osv/mempool.hh>
#include <osv/sched.hh>
#include <osv/vnode.h>
#include <osv/mount.h>

#include <vector>
#include <thread>
#include <bitset>
#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>
#include <osv/elf.hh>

using namespace std;

namespace ucache {

aio_req_t::aio_req_t(size_t s){
    this->bios = (struct bio**)calloc(s, sizeof(void*));
    this->size = s;
}
aio_req_t::~aio_req_t(){
    free(bios);
}

static struct bio* make_page_bio(int opc, void* buf, u64 lba){
    struct bio* req = alloc_bio();
    req->bio_cmd = opc;
    req->bio_offset = lba * ext4_block_size;
    req->bio_bcount = ext4_block_size; 
    req->bio_data = buf;
    return req;
}

ufs::ufs(): fs(NULL){
}

void ufs::add_device(nvme::driver* dev){
		devices.push_back(dev);
}

ufile* ufs::open_ufile(const char* name, u64 req_size){
    ufile* nuf = new ufile(name, req_size, this);
    assert_crash(nuf != NULL);
    return nuf;
}

ufile::ufile(const char* n, u64 req_size, ufs* filesys){
    fs = filesys;
    current_seek_pos = 0;
    name = (char*)malloc((strlen(n)+1)*sizeof(char));
    strcpy(name, n);
    //this->size = req_size;
    fd = open(name, O_RDWR);
    if(fd < 0){
        perror("Error");
    }
    assert_crash(fd > 0);
    struct file* fp;
    fget(fd, &fp);
    assert_crash(fp != NULL);
    struct stat stats;
    fp->stat(&stats);
    assert_crash(req_size <= (u64)stats.st_size);
    this->size = req_size;
    u64 nb_blocks = (size/ext4_block_size)+1;
    this->lbas.reserve(nb_blocks);
    for(u64 i=0; i<nb_blocks; i++){
        ioctl_req req;
        req.l_idx = i;
        req.p_idx = ~0ul;
        int ret = fp->ioctl(req_lba, &req);
        assert_crash(ret == 0);
        this->lbas.push_back(req.p_idx);
    }
}

void ufile::getLBAs(u64* blocks, u64 offset, u64 nb_blocks){
    assert_crash(offset % ext4_block_size == 0); // make sure its 4KiB aligned
    u64 lidx = offset/ext4_block_size;
    assert_crash(lidx < lbas.size());
    blocks[0] = lbas.at(lidx);

    // coalesce blocks if they are contiguous ?
    if(nb_blocks > 1){
        for(u64 i = 1; i< nb_blocks; i++){
            blocks[i] = lbas[lidx+i];
        }
    }
}

aio_req_t* ufile::async_io(int opc, void* addr, u64 offset, u64 r_size){ 
    assert_crash(r_size % ext4_block_size == 0); // make sure its 4KiB aligned
    u64 nb_blocks = r_size / ext4_block_size;
    u64 blocks[nb_blocks];
    getLBAs(&blocks[0], offset, nb_blocks);
    
    aio_req_t* reqs = new aio_req_t(nb_blocks);
    reqs->bios[0] = make_page_bio(opc, addr, blocks[0]);
		fs->devices[0]->make_async_request(reqs->bios[0]);
		if(nb_blocks > 1){
        for(size_t i = 1; i < nb_blocks; i++){
            reqs->bios[i] = make_page_bio(opc, addr+i*ext4_block_size, blocks[i]);
		        fs->devices[0]->make_async_request(reqs->bios[i]);
        }
    }
    return reqs;
}

void ufile::poll_reqs(aio_req_t* reqs){
    fs->devices[0]->poll_req(reqs->bios[0]);
    destroy_bio(reqs->bios[0]);
    if(reqs->size > 1){
        for(size_t i = 1; i < reqs->size; i++){
            fs->devices[0]->poll_req(reqs->bios[i]);
            destroy_bio(reqs->bios[i]);
        }
    }
}

void ufile::read(void* addr, u64 offset, u64 r_size){
		aio_req_t* reqs = async_io(BIO_READ, addr, offset, r_size);
    assert_crash(reqs->bios[0] != NULL);
    poll_reqs(reqs);
}

void ufile::write(void* addr, u64 offset, u64 r_size){
		aio_req_t* reqs = async_io(BIO_WRITE, addr, offset, r_size);
    poll_reqs(reqs);
}

aio_req_t* ufile::aread(void* addr, u64 offset, u64 r_size){
    return async_io(BIO_READ, addr, offset, r_size);
}

aio_req_t* ufile::awrite(void* addr, u64 offset, u64 r_size){
    return async_io(BIO_WRITE, addr, offset, r_size);
}

void ufile::poll(aio_req_t* reqs){
    poll_reqs(reqs);
}

/*void ufile::commit_io(){ //std::vector<int> &devs){
		unvme_ring_sq_doorbell(fs->devices[0], sched::cpu::current()->id);
}*/

}
