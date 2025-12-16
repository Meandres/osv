#ifndef UCACHE_HH
#define UCACHE_HH

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

#include <osv/types.h>
#include <osv/sched.hh>
#include <osv/interrupt.hh>
#include <osv/percpu.hh>
#include <osv/llfree.h>
#include <osv/power.hh>
#include <osv/ufs.hh>

static const u64 maxPageSize = mmu::huge_page_size;

namespace ucache {

  inline const u64 DEFAULT_LB_PER_STRIPE = 256;

  const bool debug = true;
  void reset_stats(int i);
  const bool batch_io_request = true;
  void print_stats();

  typedef u64 phys_addr;
  typedef u64* virt_addr;

  /// [ THREADSAFE ]
  /// Get the VMA lock for the superblock containing addr
  rwlock_t& vma_lock(const uintptr_t addr);

  /// [ THREADSAFE ]
  /// Get the Free Ranges lock for the superblock containing
  rwlock_t& free_ranges_lock(const uintptr_t addr);

  /// Get the VMA containing this address
  boost::optional<mmu::vma*> find_intersecting_vma(const uintptr_t addr);

  /// Get all VMAs from this range
  std::vector<mmu::vma*> find_intersecting_vmas(const uintptr_t addr, const u64 size);

  /// Insert a VMA into OSv's internal state
  void insert(mmu::vma* v);

  // Erase the VMA from OSv's internal state
  void erase(mmu::vma* v);

  /// [ THREADSAFE ]
  /// Test if allocating the given region complies with OSv specific policies.
  /// If this function returns positive it means allocation might succeed.
  bool validate(const uintptr_t addr, const u64 size);

  /// Allocate the given range in virtual memory. Non-validated ranges may fail
  void allocate_range(const uintptr_t addr, const u64 size);

  /// [ THREADSAFE ]
  /// Reserves a virtual memory range of the given size
  uintptr_t reserve_range(const u64 size, size_t alignment=mmu::page_size);

  /// Free the given range by returning virtual memory back to OSv
  void free_range(const uintptr_t addr, const u64 size);

  /// [ THREADSAFE ]
  /// Allocate physically contiguous memory of the specified order
  /// Returns a virtual address from the linear mapping
  void* frames_alloc(unsigned order);

  phys_addr frames_alloc_phys_addr(size_t size);
  void frames_free_phys_addr(u64 addr, size_t size);

  /// [ THREADSAFE ]
  /// Free physically contiguous memory
  /// Requires the address returned by frames_alloc
  void frames_free(void* addr, unsigned order);

  /// [ THREADSAFE ]
  /// Get amount of free physical memory
  u64 stat_free_phys_mem();

  /// [ THREADSAFE ]
  /// Get total amount of physical memory
  u64 stat_total_phys_mem();

  struct pt_elem {
    union {
      u64 word;
      struct {
        u64 present : 1; // the page is currently in memory
        u64 writable : 1; // it's allowed to write to this page
        u64 user : 1; // accessible if not set, only kernel mode code can access this page
        u64 write : 1; // through caching writes go directly to memory
        u64 disable_cache : 1; // no cache is used for this page
        u64 accessed : 1; // the CPU sets this bit when this page is used
        u64 dirty : 1; // the CPU sets this bit when a write to this page occurs
        u64 huge_page_null : 1; // must be 0 in P1 and P4, creates a 1GiB page in P3, creates a 2MiB page in P2
        u64 global_page : 1; // isn't flushed from caches on address space switch (PGE bit of CR4 register must be set)
        u64 io : 1; // currently being used by the IO driver
        u64 inserting : 1; // being inserted
        u64 evicting : 1; // being evicted
        u64 phys : 40; // physical address the page aligned 52bit physical address of the frame or the next page table
        u64 prefetcher : 11; // core id that initiated the prefetching
        u64 no_execute : 1; // forbid executing code on this page (the NXE bit in the EFER register must be set)
      };
    };

    pt_elem() {}
    pt_elem(u64 word) : word(word) {}
    static pt_elem make(u64 phys, bool huge){
      pt_elem pte;
      pte.word = 0ull;
      pte.writable = 1;
      if(huge){
        pte.huge_page_null = 1;
      }
      if(phys != 0){
        pte.present = 1;
        pte.phys = phys;
      }
      return pte;
    }
    static u64 valid_mask(){
      pt_elem pte;
      pte.word = ~(0ull);
      pte.io = 0;
      pte.inserting = 0;
      pte.evicting = 0;
      pte.prefetcher = 0;
      pte.no_execute = 0;
      pte.huge_page_null = 0;
      return ~(pte.word);
    }
  };

  typedef pt_elem PTE;

  static unsigned idx(void *virt, unsigned level)
  {
    return ((u64)virt >> (12 + level * 9)) & 511;
  }

  inline virt_addr ensure_valid_pt_elem(virt_addr parent, unsigned idx, bool with_frame, bool huge=false){
    if(pt_elem(parent[idx]).phys == 0 || pt_elem(parent[idx]).present == 0 || pt_elem(parent[idx]).writable == 0 || (parent[idx] & pt_elem::valid_mask()) != 0){ // if the children is an empty page
      u64 page_size = huge ? 2ul*1024*1024 : 4096;
      u64 frame = with_frame ? frames_alloc_phys_addr(page_size) : 0ul;
      pt_elem pte = pt_elem::make(frame, huge);
      parent[idx] = pte.word;
      if(with_frame) // initialize new page
        memset(mmu::phys_cast<u64>(pte.phys << 12), 0, 4096);
    }
    return mmu::phys_cast<u64>(pt_elem(parent[idx]).phys << 12);
  }

  inline void initialize_pte(virt_addr l1, unsigned idx, bool with_frame){
    u64 frame = with_frame ? frames_alloc_phys_addr(4096) : 0ul;
    pt_elem pte = pt_elem::make(frame, false);
    *(l1+idx) = pte.word; 
  }
  inline void ensure_valid_pte_debug(virt_addr l1, unsigned idx, bool with_frame){
    if(*(l1+idx) == 0){ // if the children is an empty page
      u64 frame = with_frame ? frames_alloc_phys_addr(4096) : 0ul;
      pt_elem pte = pt_elem::make(frame, false);
      std::cout << std::bitset<64>(pte.word) << std::endl;
      *(l1+idx) = pte.word; 
      std::cout << std::bitset<64>(*(l1+idx)) << std::endl;
    }
  }

  void allocate_pte_range(void* virt, u64 size, bool init, bool huge);

  /*
   * if we ever need wrappers around allocate_pte_range
   void allocate_pte_range_init(void* virt, u64 size){ allocate_pte_range(virt, size, true, false); }
   void allocate_pte_range_huge(void* virt, u64 size){ allocate_pte_range(virt, size, false, true); }
   void allocate_pte_range_init_huge(void* virt, u64 size) { allocate_pte_range(virt, size, true, true); }
   */

  inline std::atomic<u64>* walkRef(void* virt, bool huge = false) {
    virt_addr ptRoot = mmu::phys_cast<u64>(processor::read_cr3());
    virt_addr l3 = mmu::phys_cast<u64>(pt_elem(ptRoot[idx(virt, 3)]).phys<<12);
    virt_addr l2 = mmu::phys_cast<u64>(pt_elem(l3[idx(virt, 2)]).phys<<12);
    if(huge)
      return reinterpret_cast<std::atomic<u64>*>(l2+idx(virt, 1));
    virt_addr l1 = mmu::phys_cast<u64>(pt_elem(l2[idx(virt, 1)]).phys<<12);
    return reinterpret_cast<std::atomic<u64>*>(l1+idx(virt, 0));
  }

  inline PTE walk(void* virt) {
    PTE pte = PTE(*walkRef(virt));
    return pte;
  }

  inline PTE walkHuge(void* virt){
    PTE pte = PTE(*walkRef(virt, true));
    return pte;
  }

  inline void invalidateTLBEntry(void* addr) {
    asm volatile("invlpg (%0)" ::"r" (addr) : "memory");
  }

  enum BufferState{
    Cached, // phys != 0 and present == 1
    Inserting,
    Reading, // currently being read
    ReadyToInsert, 
    Writing, // currently being written
    Evicting, // chosen for eviction
    Uncached, // phys == 0 and present == 0
    Inconsistent, // fail
    TBD // just something to work with
  };

  struct VMA;

  struct BufferSnapshot {
    PTE *ptes;
    aio_req_t* reqs;
    BufferState state;

    BufferSnapshot(u64 nbPages){
      ptes = (PTE*)malloc(nbPages * sizeof(PTE));
      for(u64 i = 0; i<nbPages; i++)
        ptes[i] = PTE(0);
    }

    ~BufferSnapshot(){
      free(ptes);
    }
    void print_state(){
      switch(state){
        case BufferState::Cached:
          printf("Cached\n");
          break;
        case BufferState::Inserting:
          printf("Inserting\n");
          break;
        case BufferState::Reading:
          printf("Reading\n");
          break;
        case BufferState::ReadyToInsert:
          printf("ReadyToInsert\n");
          break;
        case BufferState::Writing:
          printf("Writing\n");
          break;
        case BufferState::Evicting:
          printf("Evicting\n");
          break;
        case BufferState::Uncached:
          printf("Uncached\n");
          break;
        case BufferState::Inconsistent:
          printf("Inconsistent\n");
          break;
        default:
          printf("Wtf\n");
      }
    }
  };

  struct Buffer {
    std::atomic<u64>* pteRefs;
    void* baseVirt;
    VMA* vma;
    BufferSnapshot *snap; // only used during eviction to avoid passing std::pair around

    Buffer(void* addr, u64 size, VMA* vma);
    ~Buffer(){}

    void updateSnapshot(BufferSnapshot* bs=NULL);

    u64 getPrefetcher(){
      PTE pte(*pteRefs);
      return pte.prefetcher;
    }

    void tryClearAccessed(BufferSnapshot* bs);

    bool UncachedToInserting(u64 phys, BufferSnapshot* bs);
    u64 EvictingToUncached(BufferSnapshot* bs);

    int getAccessed(BufferSnapshot* bs);

    // for non-concurrent scenarios
    void map(u64 phys);
    u64 unmap();
    void invalidateTLBEntries();

    // Transitions
    bool UncachedToPrefetching(u64 phys, BufferSnapshot* bs);
    bool CachedToEvicting(BufferSnapshot* bs);
    bool InsertingToCached(BufferSnapshot* bs);
    bool ReadyToInsertToCached(BufferSnapshot* bs);
    bool EvictingToCached(BufferSnapshot* bs);

    bool setIO();
    bool clearIO(bool dirty=false);
  };

  static const int16_t maxQueueSize = 960;
  static const int16_t blockSize = 512;
  static const u64 maxIOs = 4096;

  inline void crash_osv(){
    printf("aborting\n");
    osv::halt();
  }

  inline void assert_crash(bool cond){
    if(!cond)
      crash_osv();
  }

  // only allow 4KiB -> 64 KiB and 2MiB
  inline int computeOrder(u64 size){
    switch(size){
      case 4096:
        return 0;
      case 8192:
        return 1;
      case 16384:
        return 2;
      case 32768:
        return 3;
      case 65536:
        return 4;
      case 20971252:
        return 9;
      default:
        return -1;
    }
  }

  inline bool isSupportedPageSize(u64 size){
    return computeOrder(size) != -1;
  }

  inline void* alignPage(void* addr, u64 pageSize){
    return reinterpret_cast<void*>(reinterpret_cast<u64>(addr) & ~(pageSize-1));
  }

  typedef std::vector<Buffer*>& PrefetchList;
  typedef std::vector<Buffer*>& EvictList;

  typedef void (*alloc_func)(VMA*, void*, PrefetchList);
  typedef void (*evict_func)(VMA*, u64, EvictList);
  typedef bool (*conditional_callback) (Buffer* buf);
  typedef void (*unconditional_callback) (Buffer* buf);

  class ResidentSet{
    public:
      struct Entry {
        std::atomic<Buffer*> buf;
      };
      u64 mask;
      virtual bool insert(Buffer*) = 0;
      virtual bool remove(Buffer*) = 0;
      virtual u64 getNextBatch(u64 batchsize) = 0;
      virtual Buffer* getEntry(int) = 0;
  };

  // open addressing hash table used for second chance replacement to keep track of currently-cached pages
  class HashTableResidentSet: public ResidentSet {
    public:
      static const uintptr_t empty = ~0ull;
      static const uintptr_t tombstone = (~0ull)-1;

      Entry* ht;
      u64 count;
      //u64 mask;
      std::atomic<u64> clockPos;

      HashTableResidentSet(u64 maxCount);
      ~HashTableResidentSet();
      u64 next_pow2(u64 x);
      u64 hash(u64 k);

      bool insert(Buffer* buf) override;
      bool contains(Buffer* buf);
      bool remove(Buffer* buf) override;
      u64 getNextBatch(u64 batch) override;
      Buffer* getEntry(int) override;
  };

  struct callbacks {
    conditional_callback isDirty_implem;
    unconditional_callback clearDirty_implem;
    unconditional_callback setDirty_implem;
    conditional_callback canBeEvicted_implem;
    unconditional_callback post_EvictingToUncached_callback_implem;
    conditional_callback pre_CachedToEvicting_callback_implem;
    unconditional_callback post_EvictingToCached_callback_implem;
    unconditional_callback post_ReadyToInsertToCached_callback_implem;
    unconditional_callback misprediction_callback_implem;

    alloc_func prefetch_pol;
    evict_func evict_pol;
  };
  extern callbacks default_callbacks;

  class VMA {
    public:
      void* start;
      u64 size;
      ufile* file;
      u64 pageSize;
      u64 nbPages;
      u64 id;
      std::atomic<u64> usedPhysSize;
      std::vector<Buffer*> buffers;

      // policy related
      ResidentSet* residentSet;
      callbacks callback_implems;

      VMA(u64, u64, ResidentSet*, ufile*, callbacks);
      ~VMA(){}

      bool isValidPtr(void* addr){
        uintptr_t s = (uintptr_t)start;
        uintptr_t a = (uintptr_t)addr;
        return a >= s && a < s + size;
      }

      Buffer* getBuffer(void* addr){
        return buffers.at(((uintptr_t)addr-(uintptr_t)start)/pageSize);
      }

      bool canBeEvicted(Buffer* buf){
        return callback_implems.canBeEvicted_implem(buf);
      }

      bool isDirty(Buffer* buf){
        return callback_implems.isDirty_implem(buf);
      }

      void clearDirty(Buffer* buf){
        callback_implems.clearDirty_implem(buf);
      }

      void setDirty(Buffer* buf){
        callback_implems.setDirty_implem(buf);
      }

      void post_EvictingToUncached_callback(Buffer* buf){
        callback_implems.post_EvictingToUncached_callback_implem(buf);
      }

      bool pre_CachedToEvicting_callback(Buffer* buf){
        return callback_implems.pre_CachedToEvicting_callback_implem(buf);
      }

      void post_EvictingToCached_callback(Buffer* buf){
        callback_implems.post_EvictingToCached_callback_implem(buf);
      }

      void post_ReadyToInsertToCached_callback(Buffer* buf){
        callback_implems.post_ReadyToInsertToCached_callback_implem(buf);
      }

      void misprediction_callback(Buffer* buf){
        callback_implems.misprediction_callback_implem(buf);
      }

      void choosePrefetchingCandidates(void* addr, PrefetchList pl){
        callback_implems.prefetch_pol(this, addr, pl);
      }

      void chooseEvictionCandidates(u64 sizeToEvict, EvictList el){
        callback_implems.evict_pol(this, sizeToEvict, el);
      }

      bool addEvictionCandidate(Buffer* buf, BufferSnapshot* bs, EvictList el){
        if(bs->ptes[0].io == 1){
          return false;
        }
        if(residentSet->remove(buf)){
          if(buf->CachedToEvicting(bs)){
            if(bs->ptes[0].present == 0 && buf->snap != NULL){ // misprediction where the IO bit was cleared when polling for another request
              delete buf->snap;
              buf->snap = NULL;
            }
            assert_crash(bs->state == BufferState::Evicting);
            assert_crash(buf->snap == NULL);
            buf->snap = bs;
            el.push_back(buf);
            return true;
          }else{
            assert(residentSet->insert(buf));
            assert_crash(buf->snap == NULL);
          }
        }
        return false;
      }
  };

  struct VMACandidate {
    VMA* vma;
    u64 nbToEvict;

    VMACandidate(VMA* c, u64 nb): vma(c), nbToEvict(nb) {}
    ~VMACandidate(){}
  };

  inline bool pte_isDirty(Buffer* buf){
    int dirty = 0;
    for(size_t i=0; i<buf->vma->nbPages; i++){
      dirty += buf->snap->ptes[i].dirty;
    }
    return dirty > 0;
  }

  inline bool pte_isAccessed(Buffer* buf){
    int accessed = 0;
    for(size_t i=0; i<buf->vma->nbPages; i++){
      accessed += buf->snap->ptes[i].accessed;
    }
    return accessed > 0;
  }

  inline bool pte_canBeEvicted(Buffer* buf){
    return !pte_isAccessed(buf);
  }

  inline void pte_clearDirty(Buffer* buf){
    if(buf->snap == NULL){
      printf("shouldn't happen\n");
      BufferSnapshot* bs = new BufferSnapshot(buf->vma->nbPages);
      buf->updateSnapshot(bs);
      buf->snap = bs;
    }
    for(size_t i = 0; i < buf->vma->nbPages; i++){ // just try to 
      PTE pte = buf->snap->ptes[i];
      if(pte.dirty == 0){ // simply skip
        continue;
      }
      PTE newPTE = PTE(pte.word);
      newPTE.dirty = 0; 
      std::atomic<u64>* ref = buf->pteRefs+i;
      ref->compare_exchange_strong(pte.word, newPTE.word); // best effort 
    }
  }

  inline void empty_unconditional_callback(Buffer* buf){
    return;
  }

  inline bool empty_conditional_callback(Buffer* buf){
    return true;
  }

  // by default -> no prefetching
  inline void default_prefetch(VMA* vma, void* addr, PrefetchList pl){
    return;
  }

  void default_transparent_eviction(VMA*, u64, EvictList);

  class uCache {	
    public:
      // core
      std::map<u64, VMA*> vmas;
      u64 totalPhysSize;

      // accessory
      u64 evict_batch;
      u64 prefetch_batch;
      ufs* fs;

      // accounting
      std::atomic<u64> usedPhysSize;
      std::atomic<u64> readSize;
      std::atomic<u64> writeSize;
      std::atomic<u64> pageFaults;
      std::atomic<u64> tlbFlush;
      std::atomic<u64> mispredictions;
      std::atomic<u64> prefetchedSize;
      std::atomic<u64> poll_depth;
      std::atomic<u64> poll_depth_count;

      uCache();
      void init(u64 physSize, int batch);
      ~uCache();

      VMA* mmap(const char* name, u64 req_size, u64 pageSize=mmu::page_size, ufile* f=NULL);
      VMA* getVMA(void* addr);

      // Functions
      void ensureFreePages(u64 additionalSize);
      void readBuffer(Buffer* buf);
      void readBufferToTmp(Buffer* buf, Buffer* tmp);
      void flushBuffers(std::vector<Buffer*>& toWrite);

      bool checkPipeline(Buffer* buffer, BufferSnapshot* bs);

      void handlePageFault(VMA* vma, void* addr, exception_frame *ef); // called from the page fault handler
      void handleFault(VMA* vma, Buffer* buffer, bool newPage=false);
      void prefetch(VMA* vma, PrefetchList pl);
      void evict();
      void getVMACandidates(std::vector<VMACandidate*> *vmaCandidates);

  };

  extern uCache* uCacheManager;

  void createCache(u64 physSize, int batch);
  void initFile(const char* name, size_t size);
}; // namespace ucache

#endif
