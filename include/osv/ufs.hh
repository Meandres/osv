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
#include "drivers/nvme.hh"

#include <osv/types.h>
#include <osv/sched.hh>
#include <osv/interrupt.hh>
#include <osv/percpu.hh>
#include <osv/llfree.h>
#include <osv/power.hh>
#include <osv/bio.h>

namespace ucache {
  class ufs;
  const u64 lb_size = 512;
  const u64 ext4_block_size = 4096;
  const bool ring = true;


#define req_create_ucache 0
#define req_close_file 1
#define req_lba 2
  struct ioctl_req_ucache{
    u64 physSize;
    u64 batch;
    std::string filename;
    u64 virtSize;
    u64 bufsize;
    void* ret;
  };

  struct ioctl_req_lba{
    u64 l_idx;
    u64 p_idx;
  };

  class aio_req_t {
    public:
      struct bio** bios;
      size_t size;

      aio_req_t(size_t s);

      ~aio_req_t();
  };

  class ufile {
    public:
      char* name;
      u64 size; // keep track of name and size for convenience
      ufs* fs; //filesystem this file belongs to
      ufile(ufs*);
      virtual void read(void* buf, u64 offset, u64 size)  = 0;
      virtual void write(void* buf, u64 offset, u64 size) = 0;
      virtual aio_req_t* aread(void* buf, u64 offset, u64 size, bool ring) = 0;
      virtual aio_req_t* awrite(void* buf, u64 offset, u64 size, bool ring) = 0;
      virtual void poll(aio_req_t* reqs) = 0;
      virtual void close() = 0;
  };

  class local_ufile: public virtual ufile {
    public:
      int fd;
      std::vector<u64> lbas;

      u64 current_seek_pos;

      local_ufile(const char* name, u64 req_size, ufs* fs);
      void read(void* buf, u64 offset, u64 size);
      void write(void* buf, u64 offset, u64 size);
      aio_req_t* aread(void* buf, u64 offset, u64 size, bool ring);
      aio_req_t* awrite(void* buf, u64 offset, u64 size, bool ring);
      void poll(aio_req_t* reqs);
      void close();
      //void commit_io();//std::vector<int> &devices); 

      void getLBAs(u64* blocks, u64 offset, u64 nb_blocks);
    private:
      aio_req_t* async_io(int opc, void* addr, u64 offset, u64 size);
      void poll_reqs(aio_req_t* reqs);
  };

  class ufs{
    public:
      std::vector<nvme::driver*> devices;
      std::vector<ufile*> files; // keep track of opened files for convenience
      struct ext4_fs* fs;

      ufs();

      void add_device(nvme::driver* drv);
      template<class F>
        ufile* open_ufile(const char* name, u64 req_size){
          ufile* nuf = new F(name, req_size, this);
          return nuf;
        }
  };
  //extern template ufile* ufs::open_ufile<local_ufile>(const char* name, u64 req_size);

};
#endif
