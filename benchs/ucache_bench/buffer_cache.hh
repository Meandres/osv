#ifndef BUFFER_MANAGER_HH
#define BUFFER_MANAGER_HH
#include "typedefs.hh"
#include <atomic>
#ifdef OSV
#include <osv/cache.hh>
#endif

// note: as mmap can evict transparently pages, this only denotes the state the buffer cache wants
// and not what is actually in the mmap region.
//
// uCache can be made to enforce this state via custom eviction policy 
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

struct Stats{
   u64 readSize;
   u64 writeSize;
   u64 pfCount;
};

struct BufferManager {
   PageState* pageState;
   Page* mem;
   u64 size;
   std::atomic<u64> allocCount;

   void getStats(Stats* stats){
      #ifdef OSV
      stats->readSize = uCacheManager->readSize.exchange(0);
      stats->writeSize = uCacheManager->writeSize.exchange(0);
      stats->pfCount = uCacheManager->pfCount.exchange(0);
      #endif 
      #ifdef MMAP
      stats->readSize = 0;
      stats->writeSize = 0;
      stats->pfCount = 0;
      #endif
   }

   void resetStats(){
      #ifdef OSV
      uCacheManager->readSize.store(0);
      uCacheManager->writeSize.store(0);
      uCacheManager->pfCount.store(0);
      #endif
   }

   PageState& getPageState(PID pid) {
      return pageState[pid];
   }

   BufferManager() {

      #ifdef OSV
      createCache(envOr("PHYSGB", 4)*gb, 64);
      uCacheManager->addVMA(envOr("VIRTGB", 16)*gb, pageSize);
      mem = reinterpret_cast<Page*>(uCacheManager->vmaTree->vma->start);
      size = uCacheManager->vmaTree->vma->size; // TODO: to change when multiple vmas
      #endif
      #ifdef MMAP
      if(!getenv("BLOCK")){ printf("Please specify a block device\n"); exit(0); }
      int blockfd = open(getenv("BLOCK"), O_RDWR, S_IRWXU);
      assert(blockfd != -1);
      size = envOr("VIRTGB", 16) * gb;
      u64 virtAllocSize = size + (1<<16);
      mem = (Page*)mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_SHARED, blockfd, 0);
      madvise(mem, virtAllocSize, MADV_NOHUGEPAGE);
      #endif
      u64 virtCount = size / pageSize;
      pageState = (PageState*)mmap(NULL, virtCount * sizeof(PageState), PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
      madvise(pageState, virtCount * sizeof(PageState), MADV_HUGEPAGE);
      for(int i=0; i<virtCount; i++){
         pageState[i].init();
      }
      allocCount = 1; // pid 0 reserved for meta data
   }

   ~BufferManager() {}

   Page* allocPage() {
      u64 pid = allocCount++;
      u64 stateAndVersion = getPageState(pid).stateAndVersion;
      bool succ = getPageState(pid).tryLockX(stateAndVersion);
      assert(succ);
      Page* newPage = offset(mem, pid);
      faultAt(pid);
      return newPage;
   }

   void faultAt(PID pid) {
      Page* page = offset(mem, pid);
      #ifdef OSV
      uCacheManager->handleFault(reinterpret_cast<void*>(page), NULL);
      #endif
      #ifdef MMAP
      page->dirty &= true; // faults the page without changing the state
      #endif
   }

   Page* fixX(PID pid) {
      PageState& ps = getPageState(pid);
      for (u64 repeatCounter=0; ; repeatCounter++) {
         u64 stateAndVersion = ps.stateAndVersion.load();
         if(PageState::getState(stateAndVersion) == PageState::Unlocked){
            if (ps.tryLockX(stateAndVersion))
               return offset(mem, pid);
            break;
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
            default:
               if (ps.tryLockS(stateAndVersion))
                  return offset(mem, pid);
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

   /*bool isValidPtr(void* page) {
      return (void*)mem <= page && (void*)mem + size > page; 
   }*/
   PID toPID(void* page) { 
      return divise(mem, (Page*)page);
   }
   Page* toPtr(PID pid) { 
      return offset(mem, pid);
   }
};
#endif
