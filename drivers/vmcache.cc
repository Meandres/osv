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
#include <span>

#include <libaio.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <immintrin.h>

#include "drivers/vmcache.hh"
#include <osv/mmu.hh>
//#include "drivers/nvme.hh"

using namespace std;

#define die(msg) do { perror(msg); exit(EXIT_FAILURE); } while(0)

// allocate memory using huge pages
void* allocHuge(size_t size) {
   void* p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_HUGEPAGE);
   return p;
}

// use when lock is not free
void yield(u64 counter) {
   _mm_pause();
}

// OSv tracepoints
TRACEPOINT(trace_vmcache_btree_insert, "");
TRACEPOINT(trace_vmcache_btree_insert_ret, "");
TRACEPOINT(trace_vmcache_btree_remove, "");
TRACEPOINT(trace_vmcache_btree_remove_ret, "");
/*TRACEPOINT(trace_vmcache_guardO_constr, "");
TRACEPOINT(trace_vmcache_guardO_constr_ret, "");
TRACEPOINT(trace_vmcache_guardX_constr, "");
TRACEPOINT(trace_vmcache_guardX_constr_ret, "");*/
TRACEPOINT(trace_vmcache_try_split, "");
TRACEPOINT(trace_vmcache_try_split_ret, "");
TRACEPOINT(trace_vmcache_insert_in_page, "");
TRACEPOINT(trace_vmcache_insert_in_page_ret, "");

__thread elapsed_time parts_time[parts_num] = { };
__thread uint64_t parts_retry[parts_num] = { };
__thread uint64_t parts_count[parts_num] = { };
elapsed_time thread_aggregate_time[parts_num] = { };
uint64_t thread_aggregate_retry[parts_num] = { };
uint64_t thread_aggregate_count[parts_num] = { };

ResidentPageSet::ResidentPageSet(){}

void ResidentPageSet::init(u64 maxCount){
	//count(next_pow2(maxCount * 1.5)), mask(count - 1), clockPos(0) {
	count = next_pow2(maxCount * 1.5);
	mask = count-1;
	clockPos = 0;
	ht = (Entry*)allocHuge(count * sizeof(Entry));
	memset((void*)ht, 0xFF, count * sizeof(Entry));
}

ResidentPageSet::~ResidentPageSet() {
    munmap(ht, count * sizeof(u64));
}

u64 ResidentPageSet::next_pow2(u64 x) {
	return 1<<(64-__builtin_clzl(x-1));
}

u64 ResidentPageSet::hash(u64 k) {
    const u64 m = 0xc6a4a7935bd1e995;
    const int r = 47;
    u64 h = 0x8445d61a4e774912 ^ (8*m);
    k *= m;
    k ^= k >> r;
    k *= m;
	h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

void ResidentPageSet::insert(u64 pid) {
      u64 pos = hash(pid) & mask;
      while (true) {
         u64 curr = ht[pos].pid.load();
         assert(curr != pid);
         if ((curr == empty) || (curr == tombstone))
            if (ht[pos].pid.compare_exchange_strong(curr, pid))
               return;

         pos = (pos + 1) & mask;
      }
   }

bool ResidentPageSet::remove(u64 pid) {
      u64 pos = hash(pid) & mask;
      while (true) {
         u64 curr = ht[pos].pid.load();
         if (curr == empty)
            return false;

         if (curr == pid)
            if (ht[pos].pid.compare_exchange_strong(curr, tombstone))
               return true;

         pos = (pos + 1) & mask;
      }
   }

template<class Fn> void ResidentPageSet::iterateClockBatch(u64 batch, Fn fn) {
      u64 pos, newPos;
      do {
         pos = clockPos.load();
         newPos = (pos+batch) % count;
      } while (!clockPos.compare_exchange_strong(pos, newPos));

      for (u64 i=0; i<batch; i++) {
         u64 curr = ht[pos].pid.load();
         if ((curr != tombstone) && (curr != empty))
            fn(curr);
         pos = (pos + 1) & mask;
      }
   }

/*OSvAIOInterface::OSvAIOInterface(Page* virtMem, int workerThread, const unvme_ns_t* ns) : ns(ns), qid(workerThread%maxQueues), virtMem(virtMem) {
      for(u64 i=0; i<maxIOs; i++){
         buffer[i]=unvme_alloc(ns, pageSize);
      }
   }

void OSvAIOInterface::writePages(const vector<PID>& pages) {
   	assert(pages.size() < maxIOs);
   	for (u64 i=0; i<pages.size(); i++) {
       		PID pid = pages[i];
       		virtMem[pid].dirty = false;
       		memcpy(buffer[i], &virtMem[pid], pageSize);  
		io_desc_array[i] = unvme_awrite(ns, qid, buffer[i], (pageSize*pid)/blockSize, pageSize/blockSize);
		assert(io_desc_array[i]!=NULL);
	}
   	for(u64 i=0; i<pages.size(); i++){
       		int ret=unvme_apoll(io_desc_array[i], UNVME_TIMEOUT);
       		assert(ret==0);
	}
}*/
__thread uint16_t wtid = 0;
BufferManager bm;

template<class T>
struct AllocGuard : public GuardX<T> {	
   template <typename ...Params>
   AllocGuard(Params&&... params) {
      GuardX<T>::ptr = reinterpret_cast<T*>(bm.allocPage());
      new (GuardX<T>::ptr) T(std::forward<Params>(params)...);
      GuardX<T>::pid = bm.toPID(GuardX<T>::ptr);
   }
};

u64 envOr(const char* env, u64 value) {
   if (getenv(env))
      return atof(getenv(env));
   return value;
}

BufferManager::BufferManager(){}
	//: virtSize(envOr("VIRTGB", 16)*gb), physSize(envOr("PHYSGB", 4)*gb), virtCount(virtSize / pageSize), physCount(physSize / pageSize), residentSet(physCount) {
void BufferManager::init(){
   n_threads = envOr("THREADS", 1);
   assert(n_threads<=maxWorkerThreads);
   virtSize = envOr("VIRTGB", 16)*gb;
   physSize = envOr("PHYSGB", 4)*gb;
   virtCount = virtSize / pageSize;
   physCount = physSize / pageSize;
   residentSet.init(physCount);
   assert(virtSize>=physSize);
   u64 virtAllocSize = virtSize + (1<<16); // we allocate 64KB extra to prevent segfaults during optimistic reads

   // Initialise phys mappers
#ifdef VMCACHE_YMAP
	createYmapInterfaces(physCount, n_threads);
#endif //VMCACHE_YMAP

   pageState = (PageState*)allocHuge(virtCount * sizeof(PageState));
   for (u64 i=0; i<virtCount; i++)
      pageState[i].init();

   ns = unvme_open();
   /*dma_read_buffers.reserve(n_threads);
   for(int i=0; i<n_threads; i++)
      dma_read_buffers.push_back(unvme_alloc(ns, pageSize));
   osvaioInterfaces.reserve(n_threads);
   for(unsigned i=0; i<n_threads; i++)
      osvaioInterfaces.emplace_back(new OSvAIOInterface(i, ns));*/

   // Initialize virtual pages
   virtMem = (Page*) mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   madvise(virtMem, virtAllocSize, MADV_NOHUGEPAGE);
   //std::cout << "virtMem "<< virtMem << " - " << virtMem+virtCount << std::endl;
   if (virtMem == MAP_FAILED)
      cerr << "mmap failed" << endl;
   for(u64 i=0; i<virtCount; i++){
	//if(i%512 == 0){
   	//	madvise(virtMem+1, pageSize*512, MADV_NOHUGEPAGE);
	//}
	// page fault to create page table levels
	// do we need this ?
	bool b = virtMem[i].dirty;
	assert(!b);
	
	madvise(virtMem+i, pageSize, MADV_DONTNEED);
	// install zeroPages
	atomic<u64>* ptePtr = walkRef(virtMem+i);
	ptePtr->store(0ull);
   }
   invalidateTLB();
   
   //u64 phys = PTE(*walkRef(virtMem+512)).phys;
      
   batch = envOr("BATCH", 64);
   physUsedCount = 0;
   allocCount = 1; // pid 0 reserved for meta data
   readCount = 0;
   writeCount = 0;

   cerr << "vmcache " << " virtgb:" << virtSize/gb << " physgb:" << physSize/gb << endl;
   initialized=true;
}

void BufferManager::ensureFreePages() {
   if (physUsedCount >= physCount*0.95)
      evict();
}

// allocated new page and fix it
Page* BufferManager::allocPage() {
   auto start = osv::clock::uptime::now();
   physUsedCount++;
   ensureFreePages();
   u64 pid = allocCount++;
   if (pid >= virtCount) {
      cerr << "VIRTGB is too low" << endl;
      exit(EXIT_FAILURE);
   }
   //std::cout << "alloc " << pid << std::endl;
   u64 stateAndVersion = getPageState(pid).stateAndVersion;
   bool succ = getPageState(pid).tryLockX(stateAndVersion);
   assert(succ);
#ifdef VMCACHE_YMAP
   ymapInterfaces[wtid]->mapPhysPage(virtMem+pid);
#endif
   residentSet.insert(pid);
   virtMem[pid].dirty = true;
   
   auto end = osv::clock::uptime::now();
   auto elapsed = end - start;
   parts_time[allocpage] += elapsed;
   parts_count[allocpage]++;

   return virtMem + pid;
}

void BufferManager::handleFault(PID pid) {
   physUsedCount++;
   ensureFreePages();
#ifdef VMCACHE_YMAP
   ymapInterfaces[wtid]->mapPhysPage(virtMem+pid);
#endif
   readPage(pid);
   residentSet.insert(pid);
}

Page* BufferManager::fixX(PID pid) {
   PageState& ps = getPageState(pid);
   for (u64 repeatCounter=0; ; repeatCounter++) {
      u64 stateAndVersion = ps.stateAndVersion.load();
      switch (PageState::getState(stateAndVersion)) {
         case PageState::Evicted: {
            if (ps.tryLockX(stateAndVersion)) {
               handleFault(pid);
               return virtMem + pid;
            }
            break;
         }
         case PageState::Marked: case PageState::Unlocked: {
            if (ps.tryLockX(stateAndVersion))
               return virtMem + pid;
            break;
         }
      }
      yield(repeatCounter);
   }
}

Page* BufferManager::fixS(PID pid) {
   PageState& ps = getPageState(pid);
   for (u64 repeatCounter=0; ; repeatCounter++) {
      u64 stateAndVersion = ps.stateAndVersion;
      switch (PageState::getState(stateAndVersion)) {
         case PageState::Locked: {
            break;
         } case PageState::Evicted: {
            if (ps.tryLockX(stateAndVersion)) {
               handleFault(pid);
               ps.unlockX();
            }
            break;
         }
         default: {
            if (ps.tryLockS(stateAndVersion))
               return virtMem + pid;
         }
      }
      yield(repeatCounter);
   }
}

void BufferManager::unfixS(PID pid) {
   getPageState(pid).unlockS();
}

void BufferManager::unfixX(PID pid) {
   getPageState(pid).unlockX();
}

void BufferManager::readPage(PID pid) {
      auto start = osv::clock::uptime::now();
      int ret = unvme_read(ns, wtid%maxQueues, virtMem+pid, pid*(pageSize/blockSize), pageSize/blockSize);
      assert(ret==0);
      /*int ret = unvme_read(ns, wtid%maxQueues, dma_read_buffers[wtid], (pid*pageSize)/blockSize, pageSize/blockSize);
      assert(ret==0);
      auto mid = osv::clock::uptime::now();
      memcpy(virtMem+pid, dma_read_buffers[wtid], pageSize);*/
      readCount++;
      auto end = osv::clock::uptime::now();
      auto elapsed = end - start;
      /*auto elapsed_mid = mid - start;
      parts_time[readpage_copy] += elapsed_mid;
      parts_count[readpage_copy]++;*/
      parts_time[readpage] += elapsed;
      parts_count[readpage]++;
   }

void BufferManager::evict() {
   auto start = osv::clock::uptime::now();
   vector<PID> toEvict;
   vector<void*> toEvictAddresses;
   toEvict.reserve(batch);
   toEvictAddresses.reserve(batch);
   vector<PID> toWrite;
   toWrite.reserve(batch);

   // 0. find candidates, lock dirty ones in shared mode
   while (toEvict.size()+toWrite.size() < batch) {
      residentSet.iterateClockBatch(batch, [&](PID pid) {
         PageState& ps = getPageState(pid);
         u64 v = ps.stateAndVersion;
         switch (PageState::getState(v)) {
            case PageState::Marked:
               if (virtMem[pid].dirty) {
                  if (ps.tryLockS(v))
                     toWrite.push_back(pid);
               } else {
                  toEvict.push_back(pid);
               }
               break;
            case PageState::Unlocked:
               ps.tryMark(v);
               break;
            default:
               break; // skip
         };
      });
   }

   // 1. write dirty pages
   //osvaioInterfaces[wtid]->writePages(virtMem, toWrite);
   //std::cout << "Writing" << wtid%maxQueues << std::endl;
   assert(toWrite.size() <= maxIOs);
   for (u64 i=0; i<toWrite.size(); i++) {
	PID pid = toWrite[i];
	virtMem[pid].dirty = false;
	io_desc_array[wtid][i] = unvme_awrite(ns, wtid%maxQueues, virtMem+pid, pid*(pageSize/blockSize), pageSize/blockSize);
	assert(io_desc_array[wtid][i]!=NULL);
   }
   for(u64 i=0; i<toWrite.size(); i++){
	int ret=unvme_apoll(io_desc_array[wtid][i], 3);
	if(ret!=0){
		std::cout << "Error ret " << ret << ", i " << i << ", pid " << toWrite[i] << std::endl;
	}
	assert(ret==0);
   }
   writeCount += toWrite.size();
   
   // 2. try to lock clean page candidates
   toEvict.erase(std::remove_if(toEvict.begin(), toEvict.end(), [&](PID pid) {
      PageState& ps = getPageState(pid);
      u64 v = ps.stateAndVersion;
      return (PageState::getState(v) != PageState::Marked) || !ps.tryLockX(v);
   }), toEvict.end());
   

   // 3. try to upgrade lock for dirty page candidates
   for (auto& pid : toWrite) {
      PageState& ps = getPageState(pid);
      u64 v = ps.stateAndVersion;
      if ((PageState::getState(v) == 1) && ps.stateAndVersion.compare_exchange_weak(v, PageState::sameVersion(v, PageState::Locked)))
         toEvict.push_back(pid);
      else
         ps.unlockS();
   }
   
   // 4. remove from page table
    for (u64& pid : toEvict){
#ifdef VMCACHE_YMAP
	    ymapInterfaces[wtid]->unmapPhysPage(virtMem + pid);
	    toEvictAddresses.push_back(virtMem+pid);
    }
    mmu::invlpg_tlb_all(toEvictAddresses);
    //ymapInterfaces[wtid]->unmapBatch(virtMem, toEvict);
#else
	    madvise(virtMem + pid, pageSize, MADV_DONTNEED);
    }
#endif
   
   // 5. remove from hash table and unlock
   for (u64& pid : toEvict) {
      bool succ = residentSet.remove(pid);
      assert(succ);
      getPageState(pid).unlockXEvicted();
   }

   physUsedCount -= toEvict.size();
   auto end = osv::clock::uptime::now();
   auto elapsed = end - start;
   parts_time[evictpage] += elapsed;
   parts_count[evictpage]++;
}

//---------------------------------------------------------------------------

template <class T>
static T loadUnaligned(void* p)
{
   T x;
   memcpy(&x, p, sizeof(T));
   return x;
}

// Get order-preserving head of key (assuming little endian)
static u32 head(u8* key, unsigned keyLen)
{
   switch (keyLen) {
      case 0:
         return 0;
      case 1:
         return static_cast<u32>(key[0]) << 24;
      case 2:
         return static_cast<u32>(__builtin_bswap16(loadUnaligned<u16>(key))) << 16;
      case 3:
         return (static_cast<u32>(__builtin_bswap16(loadUnaligned<u16>(key))) << 16) | (static_cast<u32>(key[2]) << 8);
      default:
         return __builtin_bswap32(loadUnaligned<u32>(key));
   }
}

   void BTreeNode::makeHint()
   {
      unsigned dist = count / (hintCount + 1);
      for (unsigned i = 0; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].head;
   }

   void BTreeNode::updateHint(unsigned slotId)
   {
      unsigned dist = count / (hintCount + 1);
      unsigned begin = 0;
      if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
         begin = (slotId / dist) - 1;
      for (unsigned i = begin; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].head;
   }

   void BTreeNode::searchHint(u32 keyHead, u16& lowerOut, u16& upperOut)
   {
      if (count > hintCount * 2) {
         u16 dist = upperOut / (hintCount + 1);
         u16 pos, pos2;
         for (pos = 0; pos < hintCount; pos++)
            if (hint[pos] >= keyHead)
               break;
         for (pos2 = pos; pos2 < hintCount; pos2++)
            if (hint[pos2] != keyHead)
               break;
         lowerOut = pos * dist;
         if (pos2 < hintCount)
            upperOut = (pos2 + 1) * dist;
      }
   }

   // lower bound search, foundExactOut indicates if there is an exact match, returns slotId
   u16 BTreeNode::lowerBound(span<u8> skey, bool& foundExactOut)
   {
      foundExactOut = false;

      // check prefix
      int cmp = memcmp(skey.data(), getPrefix(), min(skey.size(), prefixLen));
      if (cmp < 0) // key is less than prefix
         return 0;
      if (cmp > 0) // key is greater than prefix
         return count;
      if (skey.size() < prefixLen) // key is equal but shorter than prefix
         return 0;
      u8* key = skey.data() + prefixLen;
      unsigned keyLen = skey.size() - prefixLen;

      // check hint
      u16 lower = 0;
      u16 upper = count;
      u32 keyHead = head(key, keyLen);
      searchHint(keyHead, lower, upper);

      // binary search on remaining range
      while (lower < upper) {
         u16 mid = ((upper - lower) / 2) + lower;
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
         } else { // head is equal, check full key
            int cmp = memcmp(key, getKey(mid), min(keyLen, slot[mid].keyLen));
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               if (keyLen < slot[mid].keyLen) { // key is shorter
                  upper = mid;
               } else if (keyLen > slot[mid].keyLen) { // key is longer
                  lower = mid + 1;
               } else {
                  foundExactOut = true;
                  return mid;
               }
            }
         }
      }
      return lower;
   }

   // lowerBound wrapper ignoring exact match argument (for convenience)
   u16 BTreeNode::lowerBound(span<u8> key)
   {
      bool ignore;
      return lowerBound(key, ignore);
   }

   // insert key/value pair
   void BTreeNode::insertInPage(span<u8> key, span<u8> payload)
   {
      trace_vmcache_insert_in_page();
      unsigned needed = spaceNeeded(key.size(), payload.size());
      if (needed > freeSpace()) {
         assert(needed <= freeSpaceAfterCompaction());
         compactify();
      }
      unsigned slotId = lowerBound(key);
      memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
      storeKeyValue(slotId, key, payload);
      count++;
      updateHint(slotId);
      trace_vmcache_insert_in_page_ret();
   }

   bool BTreeNode::removeSlot(unsigned slotId)
   {
      spaceUsed -= slot[slotId].keyLen;
      spaceUsed -= slot[slotId].payloadLen;
      memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
      count--;
      makeHint();
      return true;
   }

   bool BTreeNode::removeInPage(span<u8> key)
   {
      bool found;
      unsigned slotId = lowerBound(key, found);
      if (!found)
         return false;
      return removeSlot(slotId);
   }

   void BTreeNode::copyNode(BTreeNodeHeader* dst, BTreeNodeHeader* src) {
      u64 ofs = offsetof(BTreeNodeHeader, upperInnerNode);
      memcpy(reinterpret_cast<u8*>(dst)+ofs, reinterpret_cast<u8*>(src)+ofs, sizeof(BTreeNode)-ofs);
   }

   void BTreeNode::compactify()
   {
      unsigned should = freeSpaceAfterCompaction();
      static_cast<void>(should);
      BTreeNode tmp(isLeaf);
      tmp.setFences(getLowerFence(), getUpperFence());
      copyKeyValueRange(&tmp, 0, 0, count);
      tmp.upperInnerNode = upperInnerNode;
      copyNode(this, &tmp);
      makeHint();
      assert(freeSpace() == should);
   }

   // merge right node into this node
   bool BTreeNode::mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right)
   {
      if (!isLeaf)
         // TODO: implement inner merge
         return true;

      assert(right->isLeaf);
      assert(parent->isInner());
      BTreeNode tmp(isLeaf);
      tmp.setFences(getLowerFence(), right->getUpperFence());
      unsigned leftGrow = (prefixLen - tmp.prefixLen) * count;
      unsigned rightGrow = (right->prefixLen - tmp.prefixLen) * right->count;
      unsigned spaceUpperBound =
         spaceUsed + right->spaceUsed + (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
      if (spaceUpperBound > pageSize)
         return false;
      copyKeyValueRange(&tmp, 0, 0, count);
      right->copyKeyValueRange(&tmp, count, 0, right->count);
      PID pid = bm.toPID(this);
      memcpy(parent->getPayload(slotId+1).data(), &pid, sizeof(PID));
      parent->removeSlot(slotId);
      tmp.makeHint();
      tmp.nextLeafNode = right->nextLeafNode;

      copyNode(this, &tmp);
      return true;
   }

   // store key/value pair at slotId
   void BTreeNode::storeKeyValue(u16 slotId, span<u8> skey, span<u8> payload)
   {
      // slot
      u8* key = skey.data() + prefixLen;
      unsigned keyLen = skey.size() - prefixLen;
      slot[slotId].head = head(key, keyLen);
      slot[slotId].keyLen = keyLen;
      slot[slotId].payloadLen = payload.size();
      // key
      unsigned space = keyLen + payload.size();
      dataOffset -= space;
      spaceUsed += space;
      slot[slotId].offset = dataOffset;
      assert(getKey(slotId) >= reinterpret_cast<u8*>(&slot[slotId]));
      memcpy(getKey(slotId), key, keyLen);
      memcpy(getPayload(slotId).data(), payload.data(), payload.size());
   }

   void BTreeNode::copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, unsigned srcCount)
   {
      if (prefixLen <= dst->prefixLen) {  // prefix grows
         unsigned diff = dst->prefixLen - prefixLen;
         for (unsigned i = 0; i < srcCount; i++) {
            unsigned newKeyLen = slot[srcSlot + i].keyLen - diff;
            unsigned space = newKeyLen + slot[srcSlot + i].payloadLen;
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot + i].offset = dst->dataOffset;
            u8* key = getKey(srcSlot + i) + diff;
            memcpy(dst->getKey(dstSlot + i), key, space);
            dst->slot[dstSlot + i].head = head(key, newKeyLen);
            dst->slot[dstSlot + i].keyLen = newKeyLen;
            dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
         }
      } else {
         for (unsigned i = 0; i < srcCount; i++)
            copyKeyValue(srcSlot + i, dst, dstSlot + i);
      }
      dst->count += srcCount;
      assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<u8*>(dst->slot + dst->count));
   }

   void BTreeNode::copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot)
   {
      unsigned fullLen = slot[srcSlot].keyLen + prefixLen;
      u8 key[fullLen];
      memcpy(key, getPrefix(), prefixLen);
      memcpy(key+prefixLen, getKey(srcSlot), slot[srcSlot].keyLen);
      dst->storeKeyValue(dstSlot, {key, fullLen}, getPayload(srcSlot));
   }

   void BTreeNode::insertFence(FenceKeySlot& fk, span<u8> key)
   {
      assert(freeSpace() >= key.size());
      dataOffset -= key.size();
      spaceUsed += key.size();
      fk.offset = dataOffset;
      fk.len = key.size();
      memcpy(ptr() + dataOffset, key.data(), key.size());
   }

   void BTreeNode::setFences(span<u8> lower, span<u8> upper)
   {
      insertFence(lowerFence, lower);
      insertFence(upperFence, upper);
      for (prefixLen = 0; (prefixLen < min(lower.size(), upper.size())) && (lower[prefixLen] == upper[prefixLen]); prefixLen++)
         ;
   }

void BTreeNode::splitNode(BTreeNode* parent, unsigned sepSlot, span<u8> sep)
   {
      assert(sepSlot > 0);
      assert(sepSlot < (pageSize / sizeof(PID)));

      BTreeNode tmp(isLeaf);
      BTreeNode* nodeLeft = &tmp;

      AllocGuard<BTreeNode> newNode(isLeaf);
      BTreeNode* nodeRight = newNode.ptr;

      nodeLeft->setFences(getLowerFence(), sep);
      nodeRight->setFences(sep, getUpperFence());

      PID leftPID = bm.toPID(this);
      u16 oldParentSlot = parent->lowerBound(sep);
      if (oldParentSlot == parent->count) {
	 if(parent->upperInnerNode != leftPID){
		 std::cout << "leftPID " << leftPID << std::endl;
	 }
         assert(parent->upperInnerNode == leftPID);
         parent->upperInnerNode = newNode.pid;
      } else {
         assert(parent->getChild(oldParentSlot) == leftPID);
         memcpy(parent->getPayload(oldParentSlot).data(), &newNode.pid, sizeof(PID));
      }
      parent->insertInPage(sep, {reinterpret_cast<u8*>(&leftPID), sizeof(PID)});

      if (isLeaf) {
         copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
         copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
         nodeLeft->nextLeafNode = newNode.pid;
         nodeRight->nextLeafNode = this->nextLeafNode;
      } else {
         // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
         copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
         copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
         nodeLeft->upperInnerNode = getChild(nodeLeft->count);
         nodeRight->upperInnerNode = upperInnerNode;
      }
      nodeLeft->makeHint();
      nodeRight->makeHint();
      copyNode(this, nodeLeft);
   }

static_assert(sizeof(BTreeNode) == pageSize, "btree node size problem");


GuardO<BTreeNode> BTree::findLeafO(span<u8> key) {
	auto start = osv::clock::uptime::now();
	GuardO<MetaDataPage> meta(metadataPageId);
	GuardO<BTreeNode> node(meta->getRoot(slotId), meta);
	meta.release();

      while (node->isInner())
         node = GuardO<BTreeNode>(node->lookupInner(key), node);
      auto elapsed = osv::clock::uptime::now() - start;
      parts_time[findleafO] += elapsed;
      parts_count[findleafO]++;
      return node;
   }

   // point lookup, returns payload len on success, or -1 on failure
int BTree::lookup(span<u8> key, u8* payloadOut, unsigned payloadOutSize, u16 tid) {
      wtid = tid;
      for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<BTreeNode> node = findLeafO(key);
            bool found;
            unsigned pos = node->lowerBound(key, found);
            if (!found)
               return -1;

            // key found, copy payload
            memcpy(payloadOut, node->getPayload(pos).data(), min(node->slot[pos].payloadLen, payloadOutSize));
            return node->slot[pos].payloadLen;
         } catch(const OLCRestartException&) {}
      }
   }

/*template<class Fn> bool BTree::updateInPlace(span<u8> key, Fn fn) {
      for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<BTreeNode> node = findLeafO(key);
            bool found;
            unsigned pos = node->lowerBound(key, found);
            if (!found)
               return false;

            {
               GuardX<BTreeNode> nodeLocked(move(node));
               fn(nodeLocked->getPayload(pos));
               return true;
            }
         } catch(const OLCRestartException&) {}
      }
   }
*/
GuardS<BTreeNode> BTree::findLeafS(span<u8> key, u16 tid) {
      wtid = tid;
      for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<MetaDataPage> meta(metadataPageId);
            GuardO<BTreeNode> node(meta->getRoot(slotId), meta);
            meta.release();

            while (node->isInner())
               node = GuardO<BTreeNode>(node->lookupInner(key), node);

            return GuardS<BTreeNode>(move(node));
         } catch(const OLCRestartException&) {}
      }
   }

/*template<class Fn> void BTree::scanAsc(span<u8> key, Fn fn) {
      GuardS<BTreeNode> node = findLeafS(key);
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

template<class Fn>void BTree::scanDesc(span<u8> key, Fn fn) {
	GuardS<BTreeNode> node = findLeafS(key);
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
        node = findLeafS(node->getLowerFence());
        pos = node->count-1;
    }
}
*/


BTree::BTree() : splitOrdered(false) {
   if(!bm.initialized)
	bm.init();
   //print_pf = true;
   GuardX<MetaDataPage> page(metadataPageId);
   AllocGuard<BTreeNode> rootNode(true);
   slotId = btreeslotcounter++;
   page->roots[slotId] = rootNode.pid;
   //std::cout << "End of Btree init" << std::endl;
}

BTree::~BTree() {}

void BTree::trySplit(GuardX<BTreeNode>&& node, GuardX<BTreeNode>&& parent, span<u8> key, unsigned payloadLen)
{
   trace_vmcache_try_split();
   // create new root if necessary
   if (parent.pid == metadataPageId) {
      MetaDataPage* metaData = reinterpret_cast<MetaDataPage*>(parent.ptr);
      AllocGuard<BTreeNode> newRoot(false);
      newRoot->upperInnerNode = node.pid;
      metaData->roots[slotId] = newRoot.pid;
      parent = move(newRoot);
   }

   // split
   BTreeNode::SeparatorInfo sepInfo = node->findSeparator(splitOrdered.load());
   u8 sepKey[sepInfo.len];
   node->getSep(sepKey, sepInfo);

   if (parent->hasSpaceFor(sepInfo.len, sizeof(PID))) {  // is there enough space in the parent for the separator?
      node->splitNode(parent.ptr, sepInfo.slot, {sepKey, sepInfo.len});
      trace_vmcache_try_split_ret();
      return;
   }

   // must split parent to make space for separator, restart from root to do this
   node.release();
   parent.release();
   ensureSpace(parent.ptr, {sepKey, sepInfo.len}, sizeof(PID));
   trace_vmcache_try_split_ret();
}

void BTree::ensureSpace(BTreeNode* toSplit, span<u8> key, unsigned payloadLen)
{
   for (u64 repeatCounter=0; ; repeatCounter++) {
      try {
         GuardO<BTreeNode> parent(metadataPageId);
         GuardO<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId), parent);

         while (node->isInner() && (node.ptr != toSplit)) {
            parent = move(node);
            node = GuardO<BTreeNode>(parent->lookupInner(key), parent);
         }
         if (node.ptr == toSplit) {
            if (node->hasSpaceFor(key.size(), payloadLen))
               return; // someone else did split concurrently
            GuardX<BTreeNode> parentLocked(move(parent));
            GuardX<BTreeNode> nodeLocked(move(node));
            trySplit(move(nodeLocked), move(parentLocked), key, payloadLen);
         }
         return;
      } catch(const OLCRestartException&) {}
   }
}

void BTree::insert(span<u8> key, span<u8> payload, u16 tid)
{
   wtid = tid;
   assert((key.size()+payload.size()) <= BTreeNode::maxKVSize);
   //std::cout << "BTree::insert" << std::endl;
   trace_vmcache_btree_insert();
   auto start = osv::clock::uptime::now(); 
   for (u64 repeatCounter=0; ; repeatCounter++) {
      try {
         GuardO<BTreeNode> parent(metadataPageId);
         GuardO<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId), parent);

         while (node->isInner()) {
            parent = move(node);
            node = GuardO<BTreeNode>(parent->lookupInner(key), parent);
         }

         if (node->hasSpaceFor(key.size(), payload.size())) {
            // only lock leaf
            GuardX<BTreeNode> nodeLocked(move(node));
	    assert(walk(nodeLocked.ptr).phys!=0ull);
            parent.release();
            nodeLocked->insertInPage(key, payload);
   	    trace_vmcache_btree_insert_ret();
	    auto elapsed = osv::clock::uptime::now() - start;
	    parts_time[btreeinsert] += elapsed;
	    parts_retry[btreeinsert] += repeatCounter;
	    parts_count[btreeinsert]++;
            return; // success
         }

         // lock parent and leaf
         GuardX<BTreeNode> parentLocked(move(parent));
         GuardX<BTreeNode> nodeLocked(move(node));
	 assert(walk(nodeLocked.ptr).phys!=0ull);
	 assert(walk(parentLocked.ptr).phys!=0ull);
         trySplit(move(nodeLocked), move(parentLocked), key, payload.size());
         // insert hasn't happened, restart from root
      } catch(const OLCRestartException&) {}
   }
}

bool BTree::remove(span<u8> key, u16 tid)
{
   wtid = tid;
   trace_vmcache_btree_remove();
   auto start = osv::clock::uptime::now(); 
   for (u64 repeatCounter=0; ; repeatCounter++) {
      try {
         GuardO<BTreeNode> parent(metadataPageId);
         GuardO<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId), parent);

         u16 pos;
         while (node->isInner()) {
            pos = node->lowerBound(key);
            PID nextPage = (pos == node->count) ? node->upperInnerNode : node->getChild(pos);
            parent = move(node);
            node = GuardO<BTreeNode>(nextPage, parent);
         }

         bool found;
         unsigned slotId = node->lowerBound(key, found);
         if (!found){
   	    trace_vmcache_btree_remove_ret();
	    auto elapsed = osv::clock::uptime::now() - start;
	    parts_time[btreeremove] += elapsed;
	    parts_retry[btreeremove] += repeatCounter;
	    parts_count[btreeremove]++;
            return false;
	 }

         unsigned sizeEntry = node->slot[slotId].keyLen + node->slot[slotId].payloadLen;
         if ((node->freeSpaceAfterCompaction()+sizeEntry >= BTreeNodeHeader::underFullSize) && (parent.pid != metadataPageId) && (parent->count >= 2) && ((pos + 1) < parent->count)) {
            // underfull
            GuardX<BTreeNode> parentLocked(move(parent));
            GuardX<BTreeNode> nodeLocked(move(node));
            GuardX<BTreeNode> rightLocked(parentLocked->getChild(pos + 1));
            nodeLocked->removeSlot(slotId);
            if (rightLocked->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
               if (nodeLocked->mergeNodes(pos, parentLocked.ptr, rightLocked.ptr)) {
               }
            }
         } else {
            GuardX<BTreeNode> nodeLocked(move(node));
            parent.release();
            nodeLocked->removeSlot(slotId);
         }
   	 trace_vmcache_btree_remove_ret();
	 auto elapsed = osv::clock::uptime::now() - start;
	 parts_time[btreeremove] += elapsed;
	 parts_retry[btreeremove] += repeatCounter;
	 parts_count[btreeremove]++;
         return true;
      } catch(const OLCRestartException&) {}
   }
}
