#include <sys/cdefs.h>

#include "drivers/nvme.hh"
#include "drivers/unvme_nvme.h"
#include "drivers/pci-device.hh"
#include "drivers/rdtsc.h"
#include <osv/interrupt.hh>
#include <osv/virt_to_phys.hh>

#include <cassert>
#include <sstream>
#include <string>
#include <string.h>
#include <map>
#include <errno.h>
#include <osv/debug.h>

#include <osv/sched.hh>
#include <osv/trace.hh>
#include <osv/aligned_new.hh>

#include "drivers/clock.hh"
#include "drivers/clockevent.hh"

#include <osv/device.h>
#include <osv/ioctl.h>
#include <osv/contiguous_alloc.hh>

using namespace memory;

#define nvme_tag "nvme"
#define nvme_d(...)   tprintf_d(nvme_tag, __VA_ARGS__)
#define nvme_i(...)   tprintf_i(nvme_tag, __VA_ARGS__)
#define nvme_w(...)   tprintf_w(nvme_tag, __VA_ARGS__)
#define nvme_e(...)   tprintf_e(nvme_tag, __VA_ARGS__)

#include "drivers/pt.cpp"
#include <sys/mman.h>

nvme_controller_reg_t* globalReg;

#include "unvme_vfio.h"

vfio_dma_t* vfio_dma_alloc(size_t size)
{
   vfio_dma_t* p = (vfio_dma_t*)malloc(sizeof(vfio_dma_t));
   p->size = size;

   if (size <= 4096) {
      p->buf = (void*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
      madvise(p->buf, size, MADV_NOHUGEPAGE);
      memset(p->buf, 0, size);
      //p->addr = mmu::virt_to_phys(p->buf) * 4096;
      //p->addr = (uintptr_t)p->buf;
      p->addr = walk(p->buf).phys * 4096;
   } else {
      assert(size <= 2*1024*1024);
      p->buf = (void*)mmap(NULL, 2*1024*1024, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
      madvise(p->buf, 2*1024*1024, MADV_HUGEPAGE);
      memset(p->buf, 0, 2*1024*1024);
      p->addr = walkHuge(p->buf).phys * 4096;
   }
   assert(p->buf);

   return p;
}

int vfio_dma_free(vfio_dma_t* dma)
{
   return 0;
}

#include "unvme_log.c"
#include "unvme.c"
#include "unvme_core.c"
#include "unvme_nvme.c"

#include <sys/time.h>
static inline double gettime(void) {
  struct timeval now_tv;
  gettimeofday (&now_tv, NULL);
  return ((double)now_tv.tv_sec) + ((double)now_tv.tv_usec)/1000000.0;
}

nvme::nvme(pci::device &dev)
    : _dev(dev)
{
   parse_pci_config();
   globalReg = (nvme_controller_reg_t*)_bar0->get_mmio();

   u16 command = dev.get_command();
   command |= 0x4 | 0x2 | 0x400;
   dev.set_command(command);

    const unvme_ns_t* ns = unvme_open();
    if (!ns) exit(1);
    printf("model: '%.40s' sn: '%.20s' fr: '%.8s' ", ns->mn, ns->sn, ns->fr);
    printf("ps=%d qc=%d/%d qs=%d/%d bc=%#lx bs=%d mbio=%d\n", ns->pagesize, ns->qcount, ns->maxqcount, ns->qsize, ns->maxqsize, ns->blockcount, ns->blocksize, ns->maxbpio);

    unsigned datasize = 4096;
    char* buf = (char*)unvme_alloc(ns, datasize);
    for (unsigned i=0; i<datasize; i++) buf[i] = i%10;
    int ret = unvme_write(ns, 0, buf, 1, 4);
    assert(ret == 0);

    {
       unsigned repeat = 10000;
       double start = gettime();
       for (unsigned i=0; i<repeat; i++) {
          ret = unvme_read(ns, 0, buf, (i*8) % 32768, 8);
          assert(ret==0);
       }
       double end = gettime();
       std::cout << (end-start)*1e6/repeat << "us" << std::endl;
    }
}

void nvme::dump_config(void)
{
   u8 B, D, F;
   _dev.get_bdf(B, D, F);

   _dev.dump_config();
   nvme_d("%s [%x:%x.%x] vid:id= %x:%x", get_name().c_str(),
          (u16)B, (u16)D, (u16)F,
          _dev.get_vendor_id(),
          _dev.get_device_id());
}

void nvme::parse_pci_config()
{
   _bar0 = _dev.get_bar(1);
   _bar0->map();
   if (_bar0 == nullptr) {
      throw std::runtime_error("BAR1 is absent");
   }
   assert(_bar0->is_mapped());
}

hw_driver* nvme::probe(hw_device* dev)
{
   printf("probing nvme\n");
   if (auto pci_dev = dynamic_cast<pci::device*>(dev)) {
      if ((pci_dev->get_base_class_code()==1) && (pci_dev->get_sub_class_code()==8) && (pci_dev->get_programming_interface()==2)) // detect NVMe device
         return aligned_new<nvme>(*pci_dev);
   }
   return nullptr;
}
