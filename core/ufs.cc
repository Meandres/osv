#include <osv/ucache.hh>
#include <osv/mmu.hh>
#include <osv/mempool.hh>
#include <osv/sched.hh>
#include <osv/vnode.h>
#include <osv/mount.h>
#include <osv/file.h>

#include <vector>
#include <thread>
#include <bitset>
#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>
#include <osv/elf.hh>
#include <unistd.h>

using namespace std;

namespace ucache {

  aio_req_t::aio_req_t(size_t s){
    this->bios = (struct bio**)calloc(s, sizeof(void*));
    this->size = s;
  }
  aio_req_t::~aio_req_t(){
    free(bios);
  }

  /*
  static struct bio* make_page_bio(int opc, void* buf, u64 lba){
    struct bio* req = alloc_bio();
    req->bio_cmd = opc;
    req->bio_offset = lba * ext4_block_size;
    req->bio_bcount = ext4_block_size; 
    req->bio_data = buf;
    return req;
  }
  */

  ufs::ufs(): fs(NULL){
  }

  void ufs::add_device(nvme::driver* dev){
    devices.push_back(dev);
  }
  /*template<class F>
    ufile* ufs::open_ufile(const char* name, u64 req_size){

      ufile* nuf = new F(name, req_size, this);
      assert_crash(nuf != NULL);
      return nuf;
    }
  template<> ufile* ufs::open_ufile<local_ufile>(const char* name, u64 req_size);
  */
  ufile::ufile(ufs* filesys): fs(filesys)
  {
  }

  local_ufile::local_ufile(const char* n, u64 req_size, ufs* filesys): ufile(filesys), current_seek_pos(0){
    name = (char*)malloc((strlen(n)+1)*sizeof(char));
    strcpy(name, n);
    fd = open(name, O_RDWR);
    if(fd < 0){
      perror("Error");
    }
    assert_crash(fd >= 0);
    struct file* fp;
    fget(fd, &fp);
    assert_crash(fp != NULL);
    struct stat stats;
    fp->stat(&stats);
    assert_crash(req_size <= (u64)stats.st_size);
    if(req_size == 0){
      this->size = (u64)stats.st_size;
    }else{
      this->size = req_size;
    }
    u64 nb_blocks = (size/ext4_block_size)+1;
    this->lbas.reserve(nb_blocks);
    for(u64 i=0; i<nb_blocks; i++){
      ioctl_req_lba req;
      req.l_idx = i;
      req.p_idx = ~0ul;
      int ret = fp->ioctl(req_lba, &req);
      assert_crash(ret == 0);
      this->lbas.push_back(req.p_idx);
    }
  }

  void local_ufile::getLBAs(u64* blocks, u64 offset, u64 nb_blocks){
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

  aio_req_t* local_ufile::async_io(int opc, void* addr, u64 offset, u64 r_size){
    assert_crash(r_size % ext4_block_size == 0); // make sure its 4KiB aligned
    u64 nb_blocks = r_size / ext4_block_size;
    u64 blocks[nb_blocks];
    getLBAs(&blocks[0], offset, nb_blocks);

    // Count the number of contiguous LBA runs so we can size the aio_req_t.
    // For a freshly-formatted ext4 with large files the common case is 1 run,
    // reducing 512 NVMe commands (one per 4 KiB) to 1 for a 2 MiB huge page.
    size_t num_runs = 1;
    for (size_t i = 1; i < nb_blocks; i++) {
        if (blocks[i] != blocks[i - 1] + 1) num_runs++;
    }

    aio_req_t* reqs = new aio_req_t(num_runs);
    size_t bio_idx = 0;
    size_t run_start = 0;

    for (size_t i = 1; i <= nb_blocks; i++) {
        bool end_of_run = (i == nb_blocks) || (blocks[i] != blocks[i - 1] + 1);
        if (end_of_run) {
            size_t run_blocks = i - run_start;
            struct bio* bio = alloc_bio();
            bio->bio_cmd  = opc;
            bio->bio_offset = blocks[run_start] * ext4_block_size;
            bio->bio_bcount = run_blocks * ext4_block_size;
            bio->bio_data   = (char*)addr + run_start * ext4_block_size;
            fs->devices[0]->make_async_request(bio);
            reqs->bios[bio_idx++] = bio;
            run_start = i;
        }
    }
    return reqs;
  }

  void local_ufile::poll_reqs(aio_req_t* reqs){
    fs->devices[0]->poll_req(reqs->bios[0]);
    destroy_bio(reqs->bios[0]);
    if(reqs->size > 1){
      for(size_t i = 1; i < reqs->size; i++){
        fs->devices[0]->poll_req(reqs->bios[i]);
        destroy_bio(reqs->bios[i]);
      }
    }
  }

  void local_ufile::read(void* addr, u64 offset, u64 r_size){
    aio_req_t* reqs = async_io(BIO_READ, addr, offset, r_size);
    assert_crash(reqs->bios[0] != NULL);
    poll_reqs(reqs);
  }

  void local_ufile::write(void* addr, u64 offset, u64 r_size){
    aio_req_t* reqs = async_io(BIO_WRITE, addr, offset, r_size);
    poll_reqs(reqs);
  }

  aio_req_t* local_ufile::aread(void* addr, u64 offset, u64 r_size, bool ring){
    return async_io(BIO_READ, addr, offset, r_size);
  }

  aio_req_t* local_ufile::awrite(void* addr, u64 offset, u64 r_size, bool ring){
    return async_io(BIO_WRITE, addr, offset, r_size);
  }

  void local_ufile::poll(aio_req_t* reqs){
    poll_reqs(reqs);
  }

  // Drain the NVMe queue owned by cpu_id until all bios in reqs are done.
  // Does NOT call destroy_bio — bio lifecycle stays with poll_reqs on the
  // original submitter's CPU.
  void local_ufile::poll_on_cpu(aio_req_t* reqs, int cpu_id){
    for(size_t i = 0; i < reqs->size; i++){
      fs->devices[0]->poll_req_on_queue(reqs->bios[i], cpu_id);
    }
  }

  /*void local_ufile::commit_io(){ //std::vector<int> &devs){
    unvme_ring_sq_doorbell(fs->devices[0], sched::cpu::current()->id);
    }*/

  void local_ufile::close(){
    fdclose(fd);
  }

}
