#ifndef BUFFER_MANAGER_HH
#define BUFFER_MANAGER_HH
#include "typedefs.hh"
#include <atomic>
#ifdef OSV
#include <osv/cache.hh>
#endif
#ifdef MMAP
// note: as mmap can evict transparently pages, this only denotes the state the buffer cache wants
// and not what is actually in the mmap region.
struct PageState {
    std::atomic<u64> stateAndVersion;

    static const u64 Unlocked = 0;
    static const u64 MaxShared = 253;
    static const u64 Locked = 255;

    PageState() {}

    void init() { stateAndVersion.store(sameVersion(0, Unlocked), std::memory_order_release); }
    static inline u64 sameVersion(u64 oldStateAndVersion, u64 newState) { return ((oldStateAndVersion<<8)>>8) | newState<<56; }
    static inline u64 nextVersion(u64 oldStateAndVersion, u64 newState) { return (((oldStateAndVersion<<8)>>8)+1) | newState<<56; }

    bool tryLockX(u64 oldStateAndVersion) {
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Locked));
    }

    void unlockX() {
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
    }

    bool tryLockS(u64 oldStateAndVersion) {
        u64 s = getState(oldStateAndVersion);
        if (s<MaxShared)
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, s+1));
        return false;
    }

    void unlockS() {
        while (true) {
            u64 oldStateAndVersion = stateAndVersion.load();
            u64 state = getState(oldStateAndVersion);
            assert(state>0 && state<=MaxShared);
            if (stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, state-1)))
                return;
        }
    }

    static u64 getState(u64 v) { return v >> 56; };
    u64 getState() { return getState(stateAndVersion.load()); }

    void operator=(PageState&) = delete;
};
#endif

struct BufferManager {
   #ifdef OSV
   CacheManager *cm; 
   #endif
   #ifdef MMAP
   PageState* pageState;
   #endif
   Page* mem;
   u64 size;
   std::atomic<u64> allocCount;
   std::atomic<u64>* readCount;
   std::atomic<u64>* writeCount;
   std::atomic<u64>* pfCount;
   u64* batch;

   PageState& getPageState(PID pid) {
      #ifdef OSV
      return cm->getPageState(pid);
      #endif
      #ifdef MMAP
      return pageState[pid];
      #endif
   }

   BufferManager() {
      #ifdef OSV
      cm = createMMIORegion(NULL, envOr("VIRTGB", 16)*gb, envOr("PHYSGB", 4)*gb, 64);
      readCount = &cm->readCount;
      writeCount = &cm->writeCount;
      pfCount = &cm->pfCount;
      batch = &cm->batch;
      mem = cm->virtMem;
      size = cm->virtSize;
      #endif
      #ifdef MMAP
      if(!getenv("BLOCK")){ printf("Please specify a block device\n"); exit(0); }
      int blockfd = open(getenv("BLOCK"), O_RDWR, S_IRWXU);
      assert(blockfd != -1);
      size = envOr("VIRTGB", 16) * 1024*1024*1024;
      u64 virtCount = size / 4096;
      u64 virtAllocSize = size + (1<<16);
      mem = (Page*)mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_SHARED, blockfd, 0);
      madvise(mem, virtAllocSize, MADV_NOHUGEPAGE);
      pageState = (PageState*)mmap(NULL, virtCount * sizeof(PageState), PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
      madvise(pageState, virtCount * sizeof(PageState), MADV_HUGEPAGE);
      readCount = new std::atomic<u64>();
      writeCount = new std::atomic<u64>();
      pfCount = new std::atomic<u64>();
      batch = (u64*)malloc(sizeof(u64));
      *batch = 64;
      #endif
      allocCount = 1; // pid 0 reserved for meta data
   }

   ~BufferManager() {}

   Page* allocPage() {
      u64 pid = allocCount++;
      u64 stateAndVersion = getPageState(pid).stateAndVersion;
      bool succ = getPageState(pid).tryLockX(stateAndVersion);
      assert(succ);
      Page* newPage = mem+pid;
      faultAt(pid);
      return newPage;
   }

   void faultAt(PID pid) {
      Page* page = mem+pid;
      #ifdef OSV
      cm->handleFault(pid, NULL);
      #endif
      #ifdef MMAP
      page->dirty &= true; // faults the page without changing the state
      #endif
   }

   Page* fixX(PID pid) {
      PageState& ps = getPageState(pid);
      for (u64 repeatCounter=0; ; repeatCounter++) {
         u64 stateAndVersion = ps.stateAndVersion.load();
         switch (PageState::getState(stateAndVersion)) {
            #ifdef OSV
            case PageState::Evicted: {
               if (ps.tryLockX(stateAndVersion)) {
                  cm->fix(pid);
                  return mem + pid;
               }
               break;
            }
            case PageState::Marked: 
            #endif
            case PageState::Unlocked: {
               if (ps.tryLockX(stateAndVersion))
                  return mem + pid;
               break;
            }
         }
         yield(repeatCounter);
      }
   }

   Page* fixS(PID pid) {
      PageState& ps = getPageState(pid);
      for (u64 repeatCounter=0; ; repeatCounter++) {
         u64 stateAndVersion = ps.stateAndVersion;
         switch (PageState::getState(stateAndVersion)) {
            case PageState::Locked:
               break;
            #ifdef OSV
            case PageState::Evicted:
               if (ps.tryLockX(stateAndVersion)) {
                  cm->fix(pid);
                  ps.unlockX();
               }
               break;
            #endif
            default:
               if (ps.tryLockS(stateAndVersion))
                  return mem + pid;
         }
         yield(repeatCounter);
      }
   }

   void unfixS(PID pid) {
      getPageState(pid).unlockS();
   }

   void unfixX(PID pid) {
      getPageState(pid).unlockX();
   }

   bool isValidPtr(void* page) {
      #ifdef OSV
      return cm->isValidPtr(reinterpret_cast<uintptr_t>(page)); 
      #endif
      #ifdef MMAP
      return (void*)mem <= page && (void*)mem + size > page; 
      #endif
   }
   PID toPID(void* page) { 
      #ifdef OSV
      return cm->toPID(page); 
      #endif
      #ifdef MMAP
      return reinterpret_cast<Page*>(page) - mem;
      #endif
   }
   Page* toPtr(PID pid) { 
      #ifdef OSV
      return cm->toPtr(pid); 
      #endif
      #ifdef MMAP
      return mem+pid;
      #endif
   }
};
#endif
