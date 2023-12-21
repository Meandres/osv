#ifndef VMCACHE_HH
#define VMCACHE_HH

#include <atomic>
#include <algorithm>
#include <cassert>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <thread>
#include <vector>
//#include "drivers/span.hh"
#include <span>

#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <osv/types.h>
#include <unistd.h>
#include <immintrin.h>

#include <cstring>
#include "drivers/nvme.hh"
#include "drivers/rdtsc.h"
#include "osv/trace.hh"
#include <osv/clock.hh>
#include "drivers/ymap.hh"

#include <chrono>
#include <mutex>

typedef std::chrono::duration<int64_t, std::ratio<1, 1000000000>> elapsed_time;
enum parts{
	allocpage,
	readpage,
	//readpage_copy,
	evictpage,
	evictpagem0,
	evictpagem1,
	evictpagem2,
	evictpagem3,
	evictpagem4,
	evictpagem5,
	btreeinsert
};
static const char * partsStrings[] = { "BufferManager::allocPage()", "BufferManager::readPage()", "BufferManager::evict()", "BufferManager::evict() -> find candidates", "BufferManager::evict() -> write dirty pages", "BufferManager::evict() -> try to lock clean page candidates", "BufferManager::evict() -> try to update lock for dirty page candidates", "BufferManager::evict() -> remove from page table", "BufferManager::evict() -> remove from hash table and unlock", "BTree::insert()" };
const unsigned parts_num = 10;

std::mutex thread_mutex;
extern __thread elapsed_time parts_time[parts_num];
extern __thread uint64_t parts_count[parts_num];
extern elapsed_time thread_aggregate_time[parts_num];
extern uint64_t thread_aggregate_count[parts_num];

void add_thread_results(){
	const std::lock_guard<std::mutex> lock(thread_mutex);
	for(unsigned i=0; i<parts_num; i++){
		thread_aggregate_time[i] += parts_time[i];
		thread_aggregate_count[i] += parts_count[i];
	}
}

void print_aggregate_avg(){
	std::cout << "Results of the profiling :"<<std::endl;
	for(unsigned i=0; i<parts_num; i++){
		std::cout <<"\t" << partsStrings[i] << " : " << double(std::chrono::duration_cast<std::chrono::microseconds>(thread_aggregate_time[i]).count())/thread_aggregate_count[i] << "µs on avg over " << thread_aggregate_count[i] << " calls" << std::endl;
		//std::cout << "\t\t Total duration: " << double(std::chrono::duration_cast<std::chrono::milliseconds>(thread_aggregate_time[i]).count()) << " ms" <<std::endl;
	}	
}

typedef u64 PID; // page id type

//static const u64 pageSize = 4096;

/*struct alignas(4096) Page {
   bool dirty;
};*/

static const int16_t maxWorkerThreads = 32;
static const int16_t maxQueues = 32;
static const int16_t maxQueueSize = 8192;
static const int16_t blockSize=512;
static const u64 maxIOs = 256;

// allocate memory using huge pages
void* allocHuge(size_t size);

// use when lock is not free
void yield(u64 counter);

struct PageState {
   std::atomic<u64> stateAndVersion;

   static const u64 Unlocked = 0;
   static const u64 MaxShared = 252;
   static const u64 Locked = 253;
   static const u64 Marked = 254;
   static const u64 Evicted = 255;

   PageState() {}

   void init() { stateAndVersion.store(sameVersion(0, Evicted), std::memory_order_release); }
   static inline u64 sameVersion(u64 oldStateAndVersion, u64 newState) { return ((oldStateAndVersion<<8)>>8) | newState<<56; }
   static inline u64 nextVersion(u64 oldStateAndVersion, u64 newState) { return (((oldStateAndVersion<<8)>>8)+1) | newState<<56; }

   bool tryLockX(u64 oldStateAndVersion) {
      return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Locked));
   }

   void unlockX() {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
   }

   void unlockXEvicted() {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), Evicted), std::memory_order_release);
   }

   void downgradeLock() {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), 1), std::memory_order_release);
   }

   bool tryLockS(u64 oldStateAndVersion) {
      u64 s = getState(oldStateAndVersion);
      if (s<MaxShared)
         return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, s+1));
      if (s==Marked)
         return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, 1));
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

   bool tryMark(u64 oldStateAndVersion) {
      assert(getState(oldStateAndVersion)==Unlocked);
      return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Marked));
   }

   static u64 getState(u64 v) { return v >> 56; };
   u64 getState() { return getState(stateAndVersion.load()); }

   void operator=(PageState&) = delete;
};

// open addressing hash table used for second chance replacement to keep track of currently-cached pages
struct ResidentPageSet {
   static const u64 empty = ~0ull;
   static const u64 tombstone = (~0ull)-1;

   struct Entry {
      std::atomic<u64> pid;
   };

   Entry* ht;
   u64 count;
   u64 mask;
   std::atomic<u64> clockPos;

   ResidentPageSet();
   void init(u64 maxCount);
   ~ResidentPageSet();
   u64 next_pow2(u64 x);
   u64 hash(u64 k);
   
   void insert(u64 pid);
   bool remove(u64 pid);
   template<class Fn> void iterateClockBatch(u64 batch, Fn fn);
};

struct OSvAIOInterface {
   static const u64 maxIOs = 256;
   
   const unvme_ns_t* ns;
   int qid;
   //Page* virtMem;
   void* buffer[maxIOs];
   unvme_iod_t io_desc_array[maxIOs];

   //OSvAIOInterface(Page* virtMem, int workerThread, const unvme_ns_t* ns);
   //void writePages(const std::vector<PID>& pages); 
   OSvAIOInterface(int workerThread, const unvme_ns_t* ns) : ns(ns), qid(workerThread%maxQueues){ //, virtMem(virtMem) {
      for(u64 i=0; i<maxIOs; i++){
         buffer[i]=unvme_alloc(ns, pageSize);
	 assert(buffer[i] != NULL);
      }
   }

   void writePages(Page* virtMem, const std::vector<PID>& pages) {
        assert(pages.size() < maxIOs);
        for (u64 i=0; i<pages.size(); i++) {
                PID pid = pages[i];
		//std::cout << i << " : " << virtMem+pid << " into " << buffer[i] <<  std::endl;
                virtMem[pid].dirty = false;
		//std::cout << i << " : " << virtMem+pid << " into " << buffer[i] <<  std::endl;
                memcpy(buffer[i], virtMem+pid, pageSize);
                io_desc_array[i] = unvme_awrite(ns, qid, buffer[i], (pageSize*pid)/blockSize, pageSize/blockSize);
                assert(io_desc_array[i]!=NULL);
        }
        for(u64 i=0; i<pages.size(); i++){
                int ret=unvme_apoll(io_desc_array[i], UNVME_TIMEOUT);
                assert(ret==0);
        }
   }
};

extern __thread uint16_t wtid;

struct BufferManager {
   static const u64 mb = 1024ull * 1024;
   static const u64 gb = 1024ull * 1024 * 1024;
   u64 virtSize;
   u64 physSize;
   u64 virtCount;
   u64 physCount;
   int n_threads;
   //std::vector<OSvAIOInterface*> osvaioInterfaces;
   unvme_iod_t io_desc_array[maxWorkerThreads][maxIOs];

   std::atomic<u64> physUsedCount;
   ResidentPageSet residentSet;
   std::atomic<u64> allocCount;

   std::atomic<u64> readCount;
   std::atomic<u64> writeCount;

   Page* virtMem;
   PageState* pageState;
   u64 batch;

   bool initialized=false;

   const unvme_ns_t* ns; //NVMe thingies
   std::vector<void*> dma_read_buffers;

   PageState& getPageState(PID pid) {
      return pageState[pid];
   }

   BufferManager();
   void init();
   ~BufferManager() {}

   Page* fixX(PID pid);
   void unfixX(PID pid);
   Page* fixS(PID pid);
   void unfixS(PID pid);

   bool isValidPtr(void* page) { return (page >= virtMem) && (page < (virtMem + virtSize + 16)); }
   PID toPID(void* page) { return reinterpret_cast<Page*>(page) - virtMem; }
   Page* toPtr(PID pid) { return virtMem + pid; }

   void ensureFreePages();
   Page* allocPage();
   void handleFault(PID pid);
   void readPage(PID pid);
   void evict();
};

struct OLCRestartException{};

extern BufferManager bm;

void handle_segfault(int signo, siginfo_t* info, void* extra){
	void* page = info->si_addr;
	if(bm.isValidPtr(page)){
		std::cerr << "Segfault restart " << bm.toPID(page) << std::endl;
		throw OLCRestartException();
	}else{
		std::cerr << "Real segfault " << page << std::endl;
		exit(1);
	}
}

/*static BufferManager* getBM(){
	return &bm;
}*/

template<class T>
struct GuardO {
   PID pid;
   T* ptr;
   u64 version;
   //BufferManager *bm;
   static const u64 moved = ~0ull;

   // constructor
   explicit GuardO(u64 pid) : pid(pid), ptr(reinterpret_cast<T*>(bm.toPtr(pid))) {
      init();
   }

   template<class T2>
   GuardO(u64 pid, GuardO<T2>& parent)  {
      parent.checkVersionAndRestart();
      this->pid = pid;
      ptr = reinterpret_cast<T*>(bm.toPtr(pid));
      init();
   }

   GuardO(GuardO&& other) {
      pid = other.pid;
      ptr = other.ptr;
      version = other.version;
   }

   void init() {
      assert(pid != moved);
      PageState& ps = bm.getPageState(pid);
      for (u64 repeatCounter=0; ; repeatCounter++) {
         u64 v = ps.stateAndVersion.load();
         switch (PageState::getState(v)) {
            case PageState::Marked: {
               u64 newV = PageState::sameVersion(v, PageState::Unlocked);
               if (ps.stateAndVersion.compare_exchange_weak(v, newV)) {
                  version = newV;
                  return;
               }
               break;
            }
            case PageState::Locked:
               break;
            case PageState::Evicted:
               if (ps.tryLockX(v)) {
                  bm.handleFault(pid);
                  bm.unfixX(pid);
               }
               break;
            default:
               version = v;
               return;
         }
         yield(repeatCounter);
      }
   }

   // move assignment operator
   GuardO& operator=(GuardO&& other) {
      if (pid != moved)
         checkVersionAndRestart();
      pid = other.pid;
      ptr = other.ptr;
      version = other.version;
      other.pid = moved;
      other.ptr = nullptr;
      return *this;
   }

   // assignment operator
   GuardO& operator=(const GuardO&) = delete;

   // copy constructor
   GuardO(const GuardO&) = delete;

   void checkVersionAndRestart() {
      if (pid != moved) {
         PageState& ps = bm.getPageState(pid);
         u64 stateAndVersion = ps.stateAndVersion.load();
         if (version == stateAndVersion) // fast path, nothing changed
            return;
         if ((stateAndVersion<<8) == (version<<8)) { // same version
            u64 state = PageState::getState(stateAndVersion);
            if (state <= PageState::MaxShared)
               return; // ignore shared locks
            if (state == PageState::Marked)
               if (ps.stateAndVersion.compare_exchange_weak(stateAndVersion, PageState::sameVersion(stateAndVersion, PageState::Unlocked)))
                  return; // mark cleared
         }
         if (std::uncaught_exceptions()==0)
            throw OLCRestartException();
      }
   }

   // destructor
   ~GuardO() noexcept(false) {
      checkVersionAndRestart();
   }

   T* operator->() {
      assert(pid != moved);
      return ptr;
   }

   void release() {
      checkVersionAndRestart();
      pid = moved;
      ptr = nullptr;
   }
};

template<class T>
struct GuardX {
   PID pid;
   T* ptr;
   static const u64 moved = ~0ull;

   // constructor
   GuardX(): pid(moved), ptr(nullptr) {}

   // constructor
   explicit GuardX(u64 pid) : pid(pid) {
      ptr = reinterpret_cast<T*>(bm.fixX(pid));
      ptr->dirty = true;
   }

   inline explicit GuardX(GuardO<T>&& other) {
      assert(other.pid != moved);
      for (u64 repeatCounter=0; ; repeatCounter++) {
         PageState& ps = bm.getPageState(other.pid);
         u64 stateAndVersion = ps.stateAndVersion;
         if ((stateAndVersion<<8) != (other.version<<8))
            throw OLCRestartException();
         u64 state = PageState::getState(stateAndVersion);
         if ((state == PageState::Unlocked) || (state == PageState::Marked)) {
            if (ps.tryLockX(stateAndVersion)) {
               pid = other.pid;
               ptr = other.ptr;
               ptr->dirty = true;
               other.pid = moved;
               other.ptr = nullptr;
               return;
            }
         }
         yield(repeatCounter);
      }
   }

   // assignment operator
   GuardX& operator=(const GuardX&) = delete;

   // move assignment operator
   GuardX& operator=(GuardX&& other) {
      if (pid != moved) {
         bm.unfixX(pid);
      }
      pid = other.pid;
      ptr = other.ptr;
      other.pid = moved;
      other.ptr = nullptr;
      return *this;
   }

   // copy constructor
   GuardX(const GuardX&) = delete;

   // destructor
   ~GuardX() {
      if (pid != moved)
         bm.unfixX(pid);
   }

   T* operator->() {
      assert(pid != moved);
      return ptr;
   }

   void release() {
      if (pid != moved) {
         bm.unfixX(pid);
         pid = moved;
      }
   }
};

template<class T>
struct GuardS {
   PID pid;
   T* ptr;
   static const u64 moved = ~0ull;

   // constructor
   explicit GuardS(u64 pid) : pid(pid) {
      ptr = reinterpret_cast<T*>(bm.fixS(pid));
   }

   GuardS(GuardO<T>&& other) {
      assert(other.pid != moved);
      if (bm.getPageState(other.pid).tryLockS(other.version)) { // XXX: optimize?
         pid = other.pid;
         ptr = other.ptr;
         other.pid = moved;
         other.ptr = nullptr;
      } else {
         throw OLCRestartException();
      }
   }

   GuardS(GuardS&& other) {
      if (pid != moved)
         bm.unfixS(pid);
      pid = other.pid;
      ptr = other.ptr;
      other.pid = moved;
      other.ptr = nullptr;
   }

   // assignment operator
   GuardS& operator=(const GuardS&) = delete;

   // move assignment operator
   inline GuardS& operator=(GuardS&& other) {
      if (pid != moved)
         bm.unfixS(pid);
      pid = other.pid;
      ptr = other.ptr;
      other.pid = moved;
      other.ptr = nullptr;
      return *this;
   }

   // copy constructor
   GuardS(const GuardS&) = delete;

   // destructor
   ~GuardS() {
      if (pid != moved)
         bm.unfixS(pid);
   }

   T* operator->() {
      assert(pid != moved);
      return ptr;
   }

   void release() {
      if (pid != moved) {
         bm.unfixS(pid);
         pid = moved;
      }
   }
};

//---------------------------------------------------------------------------

struct BTreeNode;

struct BTreeNodeHeader {
   static const unsigned underFullSize = pageSize / 4;  // merge nodes below this size
   static const u64 noNeighbour = ~0ull;

   struct FenceKeySlot {
      u16 offset;
      u16 len;
   };

   bool dirty;
   union {
      PID upperInnerNode; // inner
      PID nextLeafNode = noNeighbour; // leaf
   };

   bool hasRightNeighbour() { return nextLeafNode != noNeighbour; }

   FenceKeySlot lowerFence = {0, 0};  // exclusive
   FenceKeySlot upperFence = {0, 0};  // inclusive

   bool hasLowerFence() { return !!lowerFence.len; };

   u16 count = 0;
   bool isLeaf;
   u16 spaceUsed = 0;
   u16 dataOffset = static_cast<u16>(pageSize);
   u16 prefixLen = 0;

   static const unsigned hintCount = 16;
   u32 hint[hintCount];
   u32 padding;

   BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
   ~BTreeNodeHeader() {}
};

static unsigned min(unsigned a, unsigned b){
	return a < b ? a : b;
}
template <class T> static T loadUnaligned(void* p);

struct BTreeNode : public BTreeNodeHeader {
   struct Slot {
      u16 offset;
      u16 keyLen;
      u16 payloadLen;
      union {
         u32 head;
         u8 headBytes[4];
      };
   } __attribute__((packed));
   union {
      Slot slot[(pageSize - sizeof(BTreeNodeHeader)) / sizeof(Slot)];  // grows from front
      u8 heap[pageSize - sizeof(BTreeNodeHeader)];                // grows from back
   };

   static constexpr unsigned maxKVSize = ((pageSize - sizeof(BTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

   BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) { dirty = true; }

   u8* ptr() { return reinterpret_cast<u8*>(this); }
   bool isInner() { return !isLeaf; }
   std::span<u8> getLowerFence() { return { ptr() + lowerFence.offset, lowerFence.len}; }
   std::span<u8> getUpperFence() { return { ptr() + upperFence.offset, upperFence.len}; }
   u8* getPrefix() { return ptr() + lowerFence.offset; } // any key on page is ok

   unsigned freeSpace() { return dataOffset - (reinterpret_cast<u8*>(slot + count) - ptr()); }
   unsigned freeSpaceAfterCompaction() { return pageSize - (reinterpret_cast<u8*>(slot + count) - ptr()) - spaceUsed; }

   bool hasSpaceFor(unsigned keyLen, unsigned payloadLen)
   {
      return spaceNeeded(keyLen, payloadLen) <= freeSpaceAfterCompaction();
   }

   u8* getKey(unsigned slotId) { return ptr() + slot[slotId].offset; }
   std::span<u8> getPayload(unsigned slotId) { return {ptr() + slot[slotId].offset + slot[slotId].keyLen, slot[slotId].payloadLen}; }

   PID getChild(unsigned slotId) { return loadUnaligned<PID>(getPayload(slotId).data()); }

   // How much space would inserting a new key of len "keyLen" require?
   unsigned spaceNeeded(unsigned keyLen, unsigned payloadLen) {
      return sizeof(Slot) + (keyLen - prefixLen) + payloadLen;
   }

   void makeHint();
   void updateHint(unsigned slotId);
   void searchHint(u32 keyHead, u16& lowerOut, u16& upperOut);
   u16 lowerBound(std::span<u8> skey, bool& foundExactOut);
   u16 lowerBound(std::span<u8> key);
   void insertInPage(std::span<u8> key, std::span<u8> payload);
   bool removeSlot(unsigned slotId);
   bool removeInPage(std::span<u8> key);
   void copyNode(BTreeNodeHeader* dst, BTreeNodeHeader* src);
   void compactify();
   bool mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right);
   void storeKeyValue(u16 slotId, std::span<u8> skey, std::span<u8> payload);
   void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, unsigned srcCount);
   void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot);
   void insertFence(FenceKeySlot& fk, std::span<u8> key);
   void setFences(std::span<u8> lower, std::span<u8> upper);
   void splitNode(BTreeNode* parent, unsigned sepSlot, std::span<u8> sep);

   struct SeparatorInfo {
      unsigned len;      // len of new separator
      unsigned slot;     // slot at which we split
      bool isTruncated;  // if true, we truncate the separator taking len bytes from slot+1
   };

   unsigned commonPrefix(unsigned slotA, unsigned slotB)
   {
      assert(slotA < count);
      unsigned limit = min(slot[slotA].keyLen, slot[slotB].keyLen);
      u8 *a = getKey(slotA), *b = getKey(slotB);
      unsigned i;
      for (i = 0; i < limit; i++)
         if (a[i] != b[i])
            break;
      return i;
   }

   SeparatorInfo findSeparator(bool splitOrdered)
   {
      assert(count > 1);
      if (isInner()) {
         // inner nodes are split in the middle
         unsigned slotId = count / 2;
         return SeparatorInfo{static_cast<unsigned>(prefixLen + slot[slotId].keyLen), slotId, false};
      }

      // find good separator slot
      unsigned bestPrefixLen, bestSlot;

      if (splitOrdered) {
         bestSlot = count - 2;
      } else if (count > 16) {
         unsigned lower = (count / 2) - (count / 16);
         unsigned upper = (count / 2);

         bestPrefixLen = commonPrefix(lower, 0);
         bestSlot = lower;

         if (bestPrefixLen != commonPrefix(upper - 1, 0))
            for (bestSlot = lower + 1; (bestSlot < upper) && (commonPrefix(bestSlot, 0) == bestPrefixLen); bestSlot++)
               ;
      } else {
         bestSlot = (count-1) / 2;
      }


      // try to truncate separator
      unsigned common = commonPrefix(bestSlot, bestSlot + 1);
      if ((bestSlot + 1 < count) && (slot[bestSlot].keyLen > common) && (slot[bestSlot + 1].keyLen > (common + 1)))
         return SeparatorInfo{prefixLen + common + 1, bestSlot, true};

      return SeparatorInfo{static_cast<unsigned>(prefixLen + slot[bestSlot].keyLen), bestSlot, false};
   }

   void getSep(u8* sepKeyOut, SeparatorInfo info)
   {
      memcpy(sepKeyOut, getPrefix(), prefixLen);
      memcpy(sepKeyOut + prefixLen, getKey(info.slot + info.isTruncated), info.len - prefixLen);
   }

   PID lookupInner(std::span<u8> key)
   {
      unsigned pos = lowerBound(key);
      if (pos == count)
         return upperInnerNode;
      return getChild(pos);
   }
};

static const u64 metadataPageId = 0;

struct MetaDataPage {
   bool dirty;
   PID roots[(pageSize-8)/8];

   PID getRoot(unsigned slot) { return roots[slot]; }
};


static unsigned btreeslotcounter=0;

struct BTree {
   private:

   void trySplit(GuardX<BTreeNode>&& node, GuardX<BTreeNode>&& parent, std::span<u8> key, unsigned payloadLen);
   void ensureSpace(BTreeNode* toSplit, std::span<u8> key, unsigned payloadLen);


   public:
   unsigned slotId;
   std::atomic<bool> splitOrdered;

   BTree();
   ~BTree();
   
   GuardO<BTreeNode> findLeafO(std::span<u8> key);
   int lookup(std::span<u8> key, u8* payloadOut, unsigned payloadOutSize, u16 tid);
   template<class Fn> inline bool lookup(std::span<u8> key, Fn fn, u16 tid){
	   wtid = tid;
	   for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<BTreeNode> node = findLeafO(key);
            bool found;
            unsigned pos = node->lowerBound(key, found);
            if (!found)
               return false;

            // key found
            fn(node->getPayload(pos));
            return true;
         } catch(const OLCRestartException&) {}
      }
   }
   //bool lookup(std::span<u8> key, void(*fn)(std::span<u8>));

   void insert(std::span<u8> key, std::span<u8> payload, u16 tid);
   bool remove(std::span<u8> key, u16 tid);

   template<class Fn> inline bool updateInPlace(std::span<u8> key, Fn fn, u16 tid) {
      wtid = tid;
      for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<BTreeNode> node = findLeafO(key);
            bool found;
            unsigned pos = node->lowerBound(key, found);
            if (!found)
               return false;

            {
               	GuardX<BTreeNode> nodeLocked(std::move(node));
               	fn(nodeLocked->getPayload(pos));
               	return true;
            }
         } catch(const OLCRestartException&) {}
      }
   }

   GuardS<BTreeNode> findLeafS(std::span<u8> key, u16 tid);
   template<class Fn> inline void scanAsc(std::span<u8> key, Fn fn, u16 tid) {
      wtid = tid;
      GuardS<BTreeNode> node = findLeafS(key, tid);
      bool found;
      unsigned pos = node->lowerBound(key, found);
      for (u64 repeatCounter=0; ; repeatCounter++) {
         if (pos<node->count) {
            if (!fn(*node.ptr, pos))
               return;
            pos++;
         } else {
            if (!node->hasRightNeighbour())
               return;
            pos = 0;
            node = GuardS<BTreeNode>(node->nextLeafNode);
         }
      }
   }

   template<class Fn>inline void scanDesc(std::span<u8> key, Fn fn, u16 tid) {
	wtid = tid;
	GuardS<BTreeNode> node = findLeafS(key, tid);
	bool exactMatch;
    int pos = node->lowerBound(key, exactMatch);
    if (pos == node->count) {
        pos--;
        exactMatch = true; // XXX:
    }
    for (u64 repeatCounter=0; ; repeatCounter++) {
        while (pos>=0) {
        	if (!fn(*node.ptr, pos, exactMatch))
               return;
            pos--;
        }
    	if (!node->hasLowerFence())
        	return;
        node = findLeafS(node->getLowerFence(), tid);
        pos = node->count-1;
    }
}
};
#endif
