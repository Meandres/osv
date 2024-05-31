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
#include <random>

#include <errno.h>
//#include <libaio.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <immintrin.h>

//#include "exmap.h"
#include <osv/cache.hh>
#include <osv/sampler.hh>
//#include <osv/memcmp.hh>

//__thread uint16_t workerThreadId = 0;
//__thread int32_t tpcchistorycounter = 0;
#include "tpcc/TPCCWorkload.hpp"

using namespace std;

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

u64 acc_time = 0;
u64 acc_count=0;
u64 diff_time=0, dt_1=0, dt_2=0, dt_3=0, dt_4=0, dt_5=0, c_1=0, c_2=0, c_3=0, c_4=0, c_5=0; 

#include <immintrin.h>
#define likely(condition) __builtin_expect(condition, 1)
#define unlikely(condition) __builtin_expect(condition, 0)

/**
 * Compare bytes between two locations. The locations must not overlap.
 *
 * @param src_1
 *   Pointer to the first source of the data.
 * @param src_2
 *   Pointer to the second source of the data.
 * @param n
 *   Number of bytes to compare.
 * @return
 *   zero if src_1 equal src_2
 *   -ve if src_1 less than src_2
 *   +ve if src_1 greater than src_2
 */
static inline int
rte_memcmp(const void *src_1, const void *src,
        size_t n) __attribute__((always_inline));

/**
 * Find the first different bit for comparison.
 */
static inline int
rte_cmpffd (uint32_t x, uint32_t y)
{
    int i;
    int pos = x ^ y;
    for (i = 0; i < 32; i++)
        if (pos & (1<<i))
            return i;
    return -1;
}

/**
 * Find the first different byte for comparison.
 */
static inline int
rte_cmpffdb (const uint8_t *x, const uint8_t *y, size_t n)
{
    size_t i;
    for (i = 0; i < n; i++)
        if (x[i] != y[i])
            return x[i] - y[i];
    return 0;
}

/**
 * Compare 16 bytes between two locations.
 * locations should not overlap.
 */
static inline int
rte_cmp16(const void *src_1, const void *src_2)
{
    __m128i xmm0, xmm1, xmm2;

    xmm0 = _mm_lddqu_si128((const __m128i *)src_1);
    xmm1 = _mm_lddqu_si128((const __m128i *)src_2);
    xmm2 = _mm_xor_si128(xmm0, xmm1);

    if (unlikely(!_mm_testz_si128(xmm2, xmm2))) {

        uint64_t mm11 = _mm_extract_epi64(xmm0, 0);
        uint64_t mm12 = _mm_extract_epi64(xmm0, 1);

        uint64_t mm21 = _mm_extract_epi64(xmm1, 0);
        uint64_t mm22 = _mm_extract_epi64(xmm1, 1);

        if (mm11 == mm21)
            return rte_cmpffdb((const uint8_t *)&mm12,
                    (const uint8_t *)&mm22, 8);
        else
            return rte_cmpffdb((const uint8_t *)&mm11,
                    (const uint8_t *)&mm21, 8);
    }

    return 0;
}

/**
 * Compare 0 to 15 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_memcmp_regular(const uint8_t *src_1u, const uint8_t *src_2u, size_t n)
{
    int ret = 1;

    /**
     * Compare less than 16 bytes
     */
    if (n & 0x08) {
        ret = (*(const uint64_t *)src_1u ==
                *(const uint64_t *)src_2u);

        if ((ret != 1))
            goto exit_8;

        n -= 0x8;
        src_1u += 0x8;
        src_2u += 0x8;
    }

    if (n & 0x04) {
        ret = (*(const uint32_t *)src_1u ==
                *(const uint32_t *)src_2u);

        if ((ret != 1))
            goto exit_4;

        n -= 0x4;
        src_1u += 0x4;
        src_2u += 0x4;
    }

    if (n & 0x02) {
        ret = (*(const uint16_t *)src_1u ==
                *(const uint16_t *)src_2u);

        if ((ret != 1))
            goto exit_2;

        n -= 0x2;
        src_1u += 0x2;
        src_2u += 0x2;
    }

    if (n & 0x01) {
        ret = (*(const uint8_t *)src_1u ==
                *(const uint8_t *)src_2u);

        if ((ret != 1))
            goto exit_1;

        n -= 0x1;
        src_1u += 0x1;
        src_2u += 0x1;
    }

    return !ret;

exit_8:
    return rte_cmpffdb(src_1u, src_2u, 8);
exit_4:
    return rte_cmpffdb(src_1u, src_2u, 4);
exit_2:
    return rte_cmpffdb(src_1u, src_2u, 2);
exit_1:
    return rte_cmpffdb(src_1u, src_2u, 1);
}

/**
 * AVX2 implementation below
 */

/**
 * Compare 32 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp32(const void *src_1, const void *src_2)
{
    const __m128i* src1 = (const __m128i*)src_1;
    const __m128i* src2 = (const __m128i*)src_2;
    const uint8_t *s1, *s2;

    __m128i mm11 = _mm_lddqu_si128(src1);
    __m128i mm12 = _mm_lddqu_si128(src1 + 1);
    __m128i mm21 = _mm_lddqu_si128(src2);
    __m128i mm22 = _mm_lddqu_si128(src2 + 1);

    __m128i mm1 = _mm_xor_si128(mm11, mm21);
    __m128i mm2 = _mm_xor_si128(mm12, mm22);
    __m128i mm = _mm_or_si128(mm1, mm2);

    if (unlikely(!_mm_testz_si128(mm, mm))) {

        /*
         * Find out which of the two 16-byte blocks
         * are different.
         */
        if (_mm_testz_si128(mm1, mm1)) {
            mm11 = mm12;
            mm21 = mm22;
            mm1 = mm2;
            s1 = (const uint8_t *)(src1 + 1);
            s2 = (const uint8_t *)(src2 + 1);
        } else {
            s1 = (const uint8_t *)src1;
            s2 = (const uint8_t *)src2;
        }

        // Produce the comparison result
        __m128i mm_cmp = _mm_cmpgt_epi8(mm11, mm21);
        __m128i mm_rcmp = _mm_cmpgt_epi8(mm21, mm11);
        mm_cmp = _mm_xor_si128(mm1, mm_cmp);
        mm_rcmp = _mm_xor_si128(mm1, mm_rcmp);

        uint32_t cmp = _mm_movemask_epi8(mm_cmp);
        uint32_t rcmp = _mm_movemask_epi8(mm_rcmp);

        int cmp_b = rte_cmpffd(cmp, rcmp);

        int ret = (cmp_b == -1) ? 0 : (s1[cmp_b] - s2[cmp_b]);
        return ret;
    }

    return 0;
}

/**
 * Compare 48 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp48(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp32((const uint8_t *)src_1 + 0 * 32,
            (const uint8_t *)src_2 + 0 * 32);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp16((const uint8_t *)src_1 + 1 * 32,
            (const uint8_t *)src_2 + 1 * 32);
    return ret;
}

/**
 * Compare 64 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp64 (const void* src_1, const void* src_2)
{
    const __m256i* src1 = (const __m256i*)src_1;
    const __m256i* src2 = (const __m256i*)src_2;
    const uint8_t *s1, *s2;

    __m256i mm11 = _mm256_lddqu_si256(src1);
    __m256i mm12 = _mm256_lddqu_si256(src1 + 1);
    __m256i mm21 = _mm256_lddqu_si256(src2);
    __m256i mm22 = _mm256_lddqu_si256(src2 + 1);

    __m256i mm1 = _mm256_xor_si256(mm11, mm21);
    __m256i mm2 = _mm256_xor_si256(mm12, mm22);
    __m256i mm = _mm256_or_si256(mm1, mm2);

    if (unlikely(!_mm256_testz_si256(mm, mm))) {
        /*
         * Find out which of the two 32-byte blocks
         * are different.
         */
        if (_mm256_testz_si256(mm1, mm1)) {
            mm11 = mm12;
            mm21 = mm22;
            mm1 = mm2;
            s1 = (const uint8_t *)(src1 + 1);
            s2 = (const uint8_t *)(src2 + 1);
        } else {
            s1 = (const uint8_t *)src1;
            s2 = (const uint8_t *)src2;
        }

        // Produce the comparison result
        __m256i mm_cmp = _mm256_cmpgt_epi8(mm11, mm21);
        __m256i mm_rcmp = _mm256_cmpgt_epi8(mm21, mm11);
        mm_cmp = _mm256_xor_si256(mm1, mm_cmp);
        mm_rcmp = _mm256_xor_si256(mm1, mm_rcmp);

        uint32_t cmp = _mm256_movemask_epi8(mm_cmp);
        uint32_t rcmp = _mm256_movemask_epi8(mm_rcmp);

        int cmp_b = rte_cmpffd(cmp, rcmp);

        int ret = (cmp_b == -1) ? 0 : (s1[cmp_b] - s2[cmp_b]);
        return ret;
    }

    return 0;
}

/**
 * Compare 128 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp128(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 0 * 64,
            (const uint8_t *)src_2 + 0 * 64);

    if (unlikely(ret != 0))
        return ret;

    return rte_cmp64((const uint8_t *)src_1 + 1 * 64,
            (const uint8_t *)src_2 + 1 * 64);
}

/**
 * Compare 256 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp256(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 0 * 64,
            (const uint8_t *)src_2 + 0 * 64);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 1 * 64,
            (const uint8_t *)src_2 + 1 * 64);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 2 * 64,
            (const uint8_t *)src_2 + 2 * 64);

    if (unlikely(ret != 0))
        return ret;

    return rte_cmp64((const uint8_t *)src_1 + 3 * 64,
            (const uint8_t *)src_2 + 3 * 64);
}

/**
 * Compare bytes between two locations. The locations must not overlap.
 *
 * @param src_1
 *   Pointer to the first source of the data.
 * @param src_2
 *   Pointer to the second source of the data.
 * @param n
 *   Number of bytes to compare.
 * @return
 *   zero if src_1 equal src_2
 *   -ve if src_1 less than src_2
 *   +ve if src_1 greater than src_2
 */
static inline int
rte_memcmp(const void *_src_1, const void *_src_2, size_t n)
{
    const uint8_t *src_1 = (const uint8_t *)_src_1;
    const uint8_t *src_2 = (const uint8_t *)_src_2;
    int ret = 0;

    if (n < 16)
        return rte_memcmp_regular(src_1, src_2, n);

    if (n <= 32) {
        ret = rte_cmp16(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 48) {
        ret = rte_cmp32(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 64) {
        ret = rte_cmp32(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 32, src_2 + 32);

        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 96) {
        ret = rte_cmp64(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 64, src_2 + 64);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 128) {
        ret = rte_cmp64(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp32(src_1 + 64, src_2 + 64);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 96, src_2 + 96);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

CMP_BLOCK_LESS_THAN_512:
    if (n <= 512) {
        if (n >= 256) {
            ret = rte_cmp256(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 256;
            src_2 = src_2 + 256;
            n -= 256;
        }
        if (n >= 128) {
            ret = rte_cmp128(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 128;
            src_2 = src_2 + 128;
            n -= 128;
        }
        if (n >= 64) {
            n -= 64;
            ret = rte_cmp64(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 64;
            src_2 = src_2 + 64;
        }
        if (n > 32) {
            ret = rte_cmp32(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            ret = rte_cmp32(src_1 - 32 + n, src_2 - 32 + n);
            return ret;
        }
        if (n > 0)
            ret = rte_cmp32(src_1 - 32 + n, src_2 - 32 + n);

        return ret;
    }

    while (n > 512) {
        ret = rte_cmp256(src_1 + 0 * 256, src_2 + 0 * 256);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp256(src_1 + 1 * 256, src_2 + 1 * 256);
        if (unlikely(ret != 0))
            return ret;

        src_1 = src_1 + 512;
        src_2 = src_2 + 512;
        n -= 512;
    }
    goto CMP_BLOCK_LESS_THAN_512;
}

inline int custom_memcmp(const void *_src_1, const void *_src_2, size_t n){
    //u64 start = rdtsc();
    int ret = rte_memcmp(_src_1, _src_2, n);
    //int ret = memcmp(_src_1, _src_2, n);
    //acc_time += (rdtsc() - start);
    //acc_count++;
    return ret;
}

inline int lowerBound_custom_memcmp(const void *_src_1, const void *_src_2, size_t n){
    //u64 start = rdtsc();
    int ret = rte_memcmp(_src_1, _src_2, n);
    //int ret = memcmp(_src_1, _src_2, n);
    //acc_time += (rdtsc() - start);
    //acc_count++;
    return ret;
}

/*typedef u64 PID; // page id type

static const u64 pageSize = 4096;

struct alignas(4096) Page {
   bool dirty;
};*/

//static const int16_t maxWorkerThreads = 128;

#define die(msg) do { perror(msg); exit(EXIT_FAILURE); } while(0)

/*uint64_t rdtsc() {
   uint32_t hi, lo;
   __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
   return static_cast<uint64_t>(lo)|(static_cast<uint64_t>(hi)<<32);
}

// exmap helper function
static int exmapAction(int exmapfd, exmap_opcode op, u16 len) {
   struct exmap_action_params params_free = { .interface = workerThreadId, .iov_len = len, .opcode = (u16)op, };
   return ioctl(exmapfd, EXMAP_IOCTL_ACTION, &params_free);
}

// allocate memory using huge pages
void* allocHuge(size_t size) {
   void* p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_HUGEPAGE);
   return p;
}*/

// use when lock is not free
void yield(u64 counter) {
   _mm_pause();
}

/*struct PageState {
   atomic<u64> stateAndVersion;

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
      atomic<u64> pid;
   };

   Entry* ht;
   u64 count;
   u64 mask;
   atomic<u64> clockPos;

   ResidentPageSet(u64 maxCount) : count(next_pow2(maxCount * 1.5)), mask(count - 1), clockPos(0) {
      ht = (Entry*)allocHuge(count * sizeof(Entry));
      memset((void*)ht, 0xFF, count * sizeof(Entry));
   }

   ~ResidentPageSet() {
      munmap(ht, count * sizeof(u64));
   }

   u64 next_pow2(u64 x) {
      return 1<<(64-__builtin_clzl(x-1));
   }

   u64 hash(u64 k) {
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

   void insert(u64 pid) {
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

   bool remove(u64 pid) {
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

   template<class Fn>
   void iterateClockBatch(u64 batch, Fn fn) {
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
};

// libaio interface used to write batches of pages
struct LibaioInterface {
   static const u64 maxIOs = 256;

   int blockfd;
   Page* virtMem;
   io_context_t ctx;
   iocb cb[maxIOs];
   iocb* cbPtr[maxIOs];
   io_event events[maxIOs];

   LibaioInterface(int blockfd, Page* virtMem) : blockfd(blockfd), virtMem(virtMem) {
      memset(&ctx, 0, sizeof(io_context_t));
      int ret = io_setup(maxIOs, &ctx);
      if (ret != 0) {
         std::cerr << "libaio io_setup error: " << ret << " ";
         switch (-ret) {
            case EAGAIN: std::cerr << "EAGAIN"; break;
            case EFAULT: std::cerr << "EFAULT"; break;
            case EINVAL: std::cerr << "EINVAL"; break;
            case ENOMEM: std::cerr << "ENOMEM"; break;
            case ENOSYS: std::cerr << "ENOSYS"; break;
         };
         exit(EXIT_FAILURE);
      }
   }

   void writePages(const vector<PID>& pages) {
      assert(pages.size() < maxIOs);
      for (u64 i=0; i<pages.size(); i++) {
         PID pid = pages[i];
         virtMem[pid].dirty = false;
         cbPtr[i] = &cb[i];
         io_prep_pwrite(cb+i, blockfd, &virtMem[pid], pageSize, pageSize*pid);
      }
      int cnt = io_subm->t(ctx, pages.size(), cbPtr);
      assert(cnt == pages.size());
      cnt = io_getevents(ctx, pages.size(), pages.size(), events, nullptr);
      assert(cnt == pages.size());
   }
};

struct BufferManager {
   static const u64 mb = 1024ull * 1024;
   static const u64 gb = 1024ull * 1024 * 1024;
   u64 virtSize;
   u64 physSize;
   u64 virtCount;
   u64 physCount;
   struct exmap_user_interface* exmapInterface[maxWorkerThreads];
   vector<LibaioInterface> libaioInterface;

   bool useExmap;
   int blockfd;
   int exmapfd;

   atomic<u64> physUsedCount;
   ResidentPageSet residentSet;
   atomic<u64> allocCount;

   atomic<u64> readCount;
   atomic<u64> writeCount;

   Page* virtMem;
   PageState* pageState;
   u64 batch;

   PageState& getPageState(PID pid) {
      return pageState[pid];
   }

   BufferManager();
   ~BufferManager() {}//munlockall();}

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
};*/

u64 envOr(const char* env, u64 value) {
   if (getenv(env))
      return atof(getenv(env));
   return value;
}

bool check_expl_ctrl(const char* env){
    if(getenv(env))
        if(strcmp(getenv(env), "TRUE"))
            return true;
    return false;
}

CacheManager *bm= createMMIORegion(NULL, envOr("VIRTGB", 16)*gb, envOr("PHYSGB", 4)*gb, envOr("THREADS", 1), 64, check_expl_ctrl("EXPLICIT"));

struct OLCRestartException {};

template<class T>
struct GuardO {
   PID pid;
   T* ptr;
   u64 version;
   static const u64 moved = ~0ull;

   // constructor
   explicit GuardO(u64 pid, int tid) : pid(pid), ptr(reinterpret_cast<T*>(bm->toPtr(pid))) {
      init(tid);
   }

   template<class T2>
   GuardO(u64 pid, GuardO<T2>& parent, int tid)  {
      parent.checkVersionAndRestart();
      this->pid = pid;
      ptr = reinterpret_cast<T*>(bm->toPtr(pid));
      init(tid);
   }

   GuardO(GuardO&& other) {
      pid = other.pid;
      ptr = other.ptr;
      version = other.version;
   }

   void init(int tid) {
      assert(pid != moved);
      PageState& ps = bm->getPageState(pid);
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
                  bm->handleFault(pid);
                  bm->unfixX(pid);
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
         PageState& ps = bm->getPageState(pid);
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
   explicit GuardX(u64 pid, int tid) : pid(pid) {
      ptr = reinterpret_cast<T*>(bm->fixX(pid));
      ptr->dirty = true;
   }

   explicit GuardX(GuardO<T>&& other) {
      assert(other.pid != moved);
      for (u64 repeatCounter=0; ; repeatCounter++) {
         PageState& ps = bm->getPageState(other.pid);
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
         bm->unfixX(pid);
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
         bm->unfixX(pid);
   }

   T* operator->() {
      assert(pid != moved);
      return ptr;
   }

   void release() {
      if (pid != moved) {
         bm->unfixX(pid);
         pid = moved;
      }
   }
};

template<class T>
struct AllocGuard : public GuardX<T> {
   template <typename ...Params>
   AllocGuard(int tid, Params&&... params) {
      GuardX<T>::ptr = reinterpret_cast<T*>(bm->allocPage());
      new (GuardX<T>::ptr) T(std::forward<Params>(params)...);
      GuardX<T>::pid = bm->toPID(GuardX<T>::ptr);
   }
};

template<class T>
struct GuardS {
   PID pid;
   T* ptr;
   static const u64 moved = ~0ull;

   // constructor
   explicit GuardS(u64 pid, int tid) : pid(pid) {
      ptr = reinterpret_cast<T*>(bm->fixS(pid));
   }

   GuardS(GuardO<T>&& other) {
      assert(other.pid != moved);
      if (bm->getPageState(other.pid).tryLockS(other.version)) { // XXX: optimize?
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
         bm->unfixS(pid);
      pid = other.pid;
      ptr = other.ptr;
      other.pid = moved;
      other.ptr = nullptr;
   }

   // assignment operator
   GuardS& operator=(const GuardS&) = delete;

   // move assignment operator
   GuardS& operator=(GuardS&& other) {
      if (pid != moved)
         bm->unfixS(pid);
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
         bm->unfixS(pid);
   }

   T* operator->() {
      assert(pid != moved);
      return ptr;
   }

   void release() {
      if (pid != moved) {
         bm->unfixS(pid);
         pid = moved;
      }
   }
};



/*BufferManager::BufferManager() : virtSize(envOr("VIRTGB", 16)*gb), physSize(envOr("PHYSGB", 4)*gb), virtCount(virtSize / pageSize), physCount(physSize / pageSize), residentSet(physCount) {
   assert(virtSize>=physSize);
   const char* path = getenv("BLOCK") ? getenv("BLOCK") : "/tmp/bm->;
   blockfd = open(path, O_RDWR | O_DIRECT, S_IRWXU);
   if (blockfd == -1) {
      cerr << "cannot open BLOCK device '" << path << "'" << endl;
      exit(EXIT_FAILURE);
   }
   u64 virtAllocSize = virtSize + (1<<16); // we allocate 64KB extra to prevent segfaults during optimistic reads

   useExmap = envOr("EXMAP", 0);
   if (useExmap) {
      exmapfd = open("/dev/exmap", O_RDWR);
      if (exmapfd < 0) die("open exmap");

      struct exmap_ioctl_setup buffer;
      buffer.fd             = blockfd;
      buffer.max_interfaces = maxWorkerThreads;
      buffer.buffer_size    = physCount;
      buffer.flags          = 0;
      if (ioctl(exmapfd, EXMAP_IOCTL_SETUP, &buffer) < 0)
         die("ioctl: exmap_setup");

      for (unsigned i=0; i<maxWorkerThreads; i++) {
         exmapInterface[i] = (struct exmap_user_interface *) mmap(NULL, pageSize, PROT_READ|PROT_WRITE, MAP_SHARED, exmapfd, EXMAP_OFF_INTERFACE(i));
         if (exmapInterface[i] == MAP_FAILED)
            die("setup exmapInterface");
      }

      virtMem = (Page*)mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_SHARED, exmapfd, 0);
   } else {
      virtMem = (Page*)mmap(NULL, virtAllocSize, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
      madvise(virtMem, virtAllocSize, MADV_NOHUGEPAGE);
   }

   pageState = (PageState*)allocHuge(virtCount * sizeof(PageState));
   for (u64 i=0; i<virtCount; i++)
      pageState[i].init();
   if (virtMem == MAP_FAILED)
      die("mmap failed");

   libaioInterface.reserve(maxWorkerThreads);
   for (unsigned i=0; i<maxWorkerThreads; i++)
      libaioInterface.emplace_back(LibaioInterface(blockfd, virtMem));

   physUsedCount = 0;
   allocCount = 1; // pid 0 reserved for meta data
   readCount = 0;
   writeCount = 0;
   batch = envOr("BATCH", 64);

   cerr << "vmcache " << "blk:" << path << " virtgb:" << virtSize/gb << " physgb:" << physSize/gb << " exmap:" << useExmap << endl;
}

void BufferManager::ensureFreePages() {
   if (physUsedCount >= physCount*0.95)
      evict();
}

// allocated new page and fix it
Page* BufferManager::allocPage() {
   physUsedCount++;
   ensureFreePages();
   u64 pid = allocCount++;
   if (pid >= virtCount) {
      cerr << "VIRTGB is too low" << endl;
      exit(EXIT_FAILURE);
   }
   u64 stateAndVersion = getPageState(pid).stateAndVersion;
   bool succ = getPageState(pid).tryLockX(stateAndVersion);
   assert(succ);
   residentSet.insert(pid);

   if (useExmap) {
      exmapInterface[workerThreadId]->iov[0].page = pid;
      exmapInterface[workerThreadId]->iov[0].len = 1;
      while (exmapAction(exmapfd, EXMAP_OP_ALLOC, 1) < 0) {
         cerr << "allocPage errno: " << errno << " pid: " << pid << " workerId: " << workerThreadId << endl;
         ensureFreePages();
      }
   }
   virtMem[pid].dirty = true;

   return virtMem + pid;
}

void BufferManager::handleFault(PID pid) {
   physUsedCount++;
   ensureFreePages();
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
   if (useExmap) {
      for (u64 repeatCounter=0; ; repeatCounter++) {
         int ret = pread(exmapfd, virtMem+pid, pageSize, workerThreadId);
         if (ret == pageSize) {
            assert(ret == pageSize);
            readCount++;
            return;
         }
         cerr << "readPage errno: " << errno << " pid: " << pid << " workerId: " << workerThreadId << endl;
         ensureFreePages();
      }
   } else {
      int ret = pread(blockfd, virtMem+pid, pageSize, pid*pageSize);
      assert(ret==pageSize);
      readCount++;
   }
}

void BufferManager::evict() {
   vector<PID> toEvict;
   toEvict.reserve(batch);
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
   libaioInterface[workerThreadId].writePages(toWrite);
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
   if (useExmap) {
      for (u64 i=0; i<toEvict.size(); i++) {
         exmapInterface[workerThreadId]->iov[i].page = toEvict[i];
         exmapInterface[workerThreadId]->iov[i].len = 1;
      }
      if (exmapAction(exmapfd, EXMAP_OP_FREE, toEvict.size()) < 0)
         die("ioctl: EXMAP_OP_FREE");
   } else {
      for (u64& pid : toEvict)
         madvise(virtMem + pid, pageSize, MADV_DONTNEED);
   }

   // 5. remove from hash table and unlock
   for (u64& pid : toEvict) {
      bool succ = residentSet.remove(pid);
      assert(succ);
      getPageState(pid).unlockXEvicted();
   }

   physUsedCount -= toEvict.size();
}*/

//---------------------------------------------------------------------------

struct BTreeNode;

struct BTreeNodeHeader {
   static const unsigned underFullSize = (pageSize/2) + (pageSize/4);  // merge nodes more empty
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

static unsigned min(unsigned a, unsigned b)
{
   return a < b ? a : b;
}

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
   span<u8> getLowerFence() { return { ptr() + lowerFence.offset, lowerFence.len}; }
   span<u8> getUpperFence() { return { ptr() + upperFence.offset, upperFence.len}; }
   u8* getPrefix() { return ptr() + lowerFence.offset; } // any key on page is ok

   unsigned freeSpace() { return dataOffset - (reinterpret_cast<u8*>(slot + count) - ptr()); }
   unsigned freeSpaceAfterCompaction() { return pageSize - (reinterpret_cast<u8*>(slot + count) - ptr()) - spaceUsed; }

   bool hasSpaceFor(unsigned keyLen, unsigned payloadLen)
   {
      return spaceNeeded(keyLen, payloadLen) <= freeSpaceAfterCompaction();
   }

   u8* getKey(unsigned slotId) { return ptr() + slot[slotId].offset; }
   span<u8> getPayload(unsigned slotId) { return {ptr() + slot[slotId].offset + slot[slotId].keyLen, slot[slotId].payloadLen}; }

   PID getChild(unsigned slotId) { return loadUnaligned<PID>(getPayload(slotId).data()); }

   // How much space would inserting a new key of len "keyLen" require?
   unsigned spaceNeeded(unsigned keyLen, unsigned payloadLen) {
      return sizeof(Slot) + (keyLen - prefixLen) + payloadLen;
   }

   void makeHint()
   {
      unsigned dist = count / (hintCount + 1);
      for (unsigned i = 0; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].head;
   }

   void updateHint(unsigned slotId)
   {
      unsigned dist = count / (hintCount + 1);
      unsigned begin = 0;
      if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
         begin = (slotId / dist) - 1;
      for (unsigned i = begin; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].head;
   }

   void searchHint(u32 keyHead, u16& lowerOut, u16& upperOut)
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
   u16 lowerBound(span<u8> skey, bool& foundExactOut)
   {
      foundExactOut = false;
      //c_1++;
      //u64 m0 = rdtsc();

      // check prefix
      int cmp = custom_memcmp(skey.data(), getPrefix(), min(skey.size(), prefixLen));
      //u64 m1 = rdtsc();
      if (cmp < 0) // key is less than prefix
         return 0;
      if (cmp > 0) // key is greater than prefix
         return count;
      if (skey.size() < prefixLen) // key is equal but shorter than prefix
         return 0;
      u8* key = skey.data() + prefixLen;
      unsigned keyLen = skey.size() - prefixLen;
      //u64 m2 = rdtsc();

      // check hint
      u16 lower = 0;
      u16 upper = count;
      u32 keyHead = head(key, keyLen);
      searchHint(keyHead, lower, upper);
      //u64 m3 = rdtsc();

      u64 count=0;
      u16 mid;
      // binary search on remaining range
      while (lower < upper) {
         count++;
         mid = ((upper - lower) / 2) + lower;
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
         } else { // head is equal, check full key
            int cmp = lowerBound_custom_memcmp(key, getKey(mid), min(keyLen, slot[mid].keyLen));
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
                  //break;
               }
            }
         }
      }
      /*u64 m4 = rdtsc();
      dt_1 += (m1-m0);
      dt_2 += (m2-m1);
      dt_3 += (m3-m2);
      dt_4 += (m4-m3);
      c_2 += count;
      if(foundExactOut){
        return mid;
      }else{*/
        return lower;
      //}
   }

   // lowerBound wrapper ignoring exact match argument (for convenience)
   u16 lowerBound(span<u8> key)
   {
      bool ignore;
      return lowerBound(key, ignore);
   }

   // insert key/value pair
   void insertInPage(span<u8> key, span<u8> payload)
   {
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
   }

   bool removeSlot(unsigned slotId)
   {
      spaceUsed -= slot[slotId].keyLen;
      spaceUsed -= slot[slotId].payloadLen;
      memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
      count--;
      makeHint();
      return true;
   }

   bool removeInPage(span<u8> key)
   {
      bool found;
      unsigned slotId = lowerBound(key, found);
      if (!found)
         return false;
      return removeSlot(slotId);
   }

   void copyNode(BTreeNodeHeader* dst, BTreeNodeHeader* src) {
      u64 ofs = offsetof(BTreeNodeHeader, upperInnerNode);
      memcpy(reinterpret_cast<u8*>(dst)+ofs, reinterpret_cast<u8*>(src)+ofs, sizeof(BTreeNode)-ofs);
   }

   void compactify()
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
   bool mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right)
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
      PID pid = bm->toPID(this);
      memcpy(parent->getPayload(slotId+1).data(), &pid, sizeof(PID));
      parent->removeSlot(slotId);
      tmp.makeHint();
      tmp.nextLeafNode = right->nextLeafNode;

      copyNode(this, &tmp);
      return true;
   }

   // store key/value pair at slotId
   void storeKeyValue(u16 slotId, span<u8> skey, span<u8> payload)
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

   void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, unsigned srcCount)
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

   void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot)
   {
      unsigned fullLen = slot[srcSlot].keyLen + prefixLen;
      u8 key[fullLen];
      memcpy(key, getPrefix(), prefixLen);
      memcpy(key+prefixLen, getKey(srcSlot), slot[srcSlot].keyLen);
      dst->storeKeyValue(dstSlot, {key, fullLen}, getPayload(srcSlot));
   }

   void insertFence(FenceKeySlot& fk, span<u8> key)
   {
      assert(freeSpace() >= key.size());
      dataOffset -= key.size();
      spaceUsed += key.size();
      fk.offset = dataOffset;
      fk.len = key.size();
      memcpy(ptr() + dataOffset, key.data(), key.size());
   }

   void setFences(span<u8> lower, span<u8> upper)
   {
      insertFence(lowerFence, lower);
      insertFence(upperFence, upper);
      for (prefixLen = 0; (prefixLen < min(lower.size(), upper.size())) && (lower[prefixLen] == upper[prefixLen]); prefixLen++)
         ;
   }

   void splitNode(BTreeNode* parent, unsigned sepSlot, span<u8> sep, int tid)
   {
      assert(sepSlot > 0);
      assert(sepSlot < (pageSize / sizeof(PID)));

      BTreeNode tmp(isLeaf);
      BTreeNode* nodeLeft = &tmp;

      AllocGuard<BTreeNode> newNode(tid, isLeaf);
      BTreeNode* nodeRight = newNode.ptr;

      nodeLeft->setFences(getLowerFence(), sep);
      nodeRight->setFences(sep, getUpperFence());

      PID leftPID = bm->toPID(this);
      u16 oldParentSlot = parent->lowerBound(sep);
      if (oldParentSlot == parent->count) {
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

   PID lookupInner(span<u8> key)
   {
      unsigned pos = lowerBound(key);
      if (pos == count)
         return upperInnerNode;
      return getChild(pos);
   }
};

static_assert(sizeof(BTreeNode) == pageSize, "btree node size problem");

static const u64 metadataPageId = 0;

struct MetaDataPage {
   bool dirty;
   PID roots[(pageSize-8)/8];

   PID getRoot(unsigned slot) { return roots[slot]; }
};

struct BTree {
   private:

   void trySplit(GuardX<BTreeNode>&& node, GuardX<BTreeNode>&& parent, span<u8> key, unsigned payloadLen, int tid);
   void ensureSpace(BTreeNode* toSplit, span<u8> key, unsigned payloadLen, int tid);

   public:
   unsigned slotId;
   atomic<bool> splitOrdered;

   BTree();
   ~BTree();

   GuardO<BTreeNode> findLeafO(span<u8> key, int tid) {
      GuardO<MetaDataPage> meta(metadataPageId, tid);
      GuardO<BTreeNode> node(meta->getRoot(slotId), meta, tid);
      meta.release();

      while (node->isInner())
         node = GuardO<BTreeNode>(node->lookupInner(key), node, tid);
      return node;
   }

   // point lookup, returns payload len on success, or -1 on failure
   int lookup(span<u8> key, u8* payloadOut, unsigned payloadOutSize, int tid) {
      for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<BTreeNode> node = findLeafO(key, tid);
            bool found;
            unsigned pos = node->lowerBound(key, found);
            if (!found)
               return -1;

            // key found, copy payload
            memcpy(payloadOut, node->getPayload(pos).data(), min(node->slot[pos].payloadLen, payloadOutSize));
            return node->slot[pos].payloadLen;
         } catch(const OLCRestartException&) { yield(repeatCounter); }
      }
   }

   template<class Fn>
   bool lookup(span<u8> key, Fn fn, int tid) {
      for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<BTreeNode> node = findLeafO(key, tid);
            bool found;
            unsigned pos = node->lowerBound(key, found);
            if (!found)
               return false;

            // key found
            fn(node->getPayload(pos));
            return true;
         } catch(const OLCRestartException&) { yield(repeatCounter); }
      }
   }

   void insert(span<u8> key, span<u8> payload, int tid);
   bool remove(span<u8> key, int tid);

   template<class Fn>
   bool updateInPlace(span<u8> key, Fn fn, int tid) {
      for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<BTreeNode> node = findLeafO(key, tid);
            bool found;
            unsigned pos = node->lowerBound(key, found);
            if (!found)
               return false;

            {
               GuardX<BTreeNode> nodeLocked(move(node));
               fn(nodeLocked->getPayload(pos));
               return true;
            }
         } catch(const OLCRestartException&) { yield(repeatCounter); }
      }
   }

   GuardS<BTreeNode> findLeafS(span<u8> key, int tid) {
      for (u64 repeatCounter=0; ; repeatCounter++) {
         try {
            GuardO<MetaDataPage> meta(metadataPageId, tid);
            GuardO<BTreeNode> node(meta->getRoot(slotId), meta, tid);
            meta.release();

            while (node->isInner())
               node = GuardO<BTreeNode>(node->lookupInner(key), node, tid);

            return GuardS<BTreeNode>(move(node));
         } catch(const OLCRestartException&) { yield(repeatCounter); }
      }
   }

   template<class Fn>
   void scanAsc(span<u8> key, Fn fn, int tid) {
      GuardS<BTreeNode> node = findLeafS(key, tid);
      bool found;
      unsigned pos = node->lowerBound(key, found);
      for (u64 repeatCounter=0; ; repeatCounter++) { // XXX
         if (pos<node->count) {
            if (!fn(*node.ptr, pos))
               return;
            pos++;
         } else {
            if (!node->hasRightNeighbour())
               return;
            pos = 0;
            node = GuardS<BTreeNode>(node->nextLeafNode, tid);
         }
      }
   }

   template<class Fn>
   void scanDesc(span<u8> key, Fn fn, int tid) {
      GuardS<BTreeNode> node = findLeafS(key, tid);
      bool exactMatch;
      int pos = node->lowerBound(key, exactMatch);
      if (pos == node->count) {
         pos--;
         exactMatch = true; // XXX:
      }
      for (u64 repeatCounter=0; ; repeatCounter++) { // XXX
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

static unsigned btreeslotcounter = 0;

BTree::BTree() : splitOrdered(false) {
   GuardX<MetaDataPage> page(metadataPageId, 0);
   AllocGuard<BTreeNode> rootNode(0, true);
   slotId = btreeslotcounter++;
   page->roots[slotId] = rootNode.pid;
}

BTree::~BTree() {}

void BTree::trySplit(GuardX<BTreeNode>&& node, GuardX<BTreeNode>&& parent, span<u8> key, unsigned payloadLen, int tid)
{

   // create new root if necessary
   if (parent.pid == metadataPageId) {
      MetaDataPage* metaData = reinterpret_cast<MetaDataPage*>(parent.ptr);
      AllocGuard<BTreeNode> newRoot(tid, false);
      newRoot->upperInnerNode = node.pid;
      metaData->roots[slotId] = newRoot.pid;
      parent = move(newRoot);
   }

   // split
   BTreeNode::SeparatorInfo sepInfo = node->findSeparator(splitOrdered.load());
   u8 sepKey[sepInfo.len];
   node->getSep(sepKey, sepInfo);

   if (parent->hasSpaceFor(sepInfo.len, sizeof(PID))) {  // is there enough space in the parent for the separator?
      node->splitNode(parent.ptr, sepInfo.slot, {sepKey, sepInfo.len}, tid);
      return;
   }

   // must split parent to make space for separator, restart from root to do this
   node.release();
   parent.release();
   ensureSpace(parent.ptr, {sepKey, sepInfo.len}, sizeof(PID), tid);
}

void BTree::ensureSpace(BTreeNode* toSplit, span<u8> key, unsigned payloadLen, int tid)
{
   for (u64 repeatCounter=0; ; repeatCounter++) {
      try {
         GuardO<BTreeNode> parent(metadataPageId, tid);
         GuardO<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId), parent, tid);

         while (node->isInner() && (node.ptr != toSplit)) {
            parent = move(node);
            node = GuardO<BTreeNode>(parent->lookupInner(key), parent, tid);
         }
         if (node.ptr == toSplit) {
            if (node->hasSpaceFor(key.size(), payloadLen))
               return; // someone else did split concurrently
            GuardX<BTreeNode> parentLocked(move(parent));
            GuardX<BTreeNode> nodeLocked(move(node));
            trySplit(move(nodeLocked), move(parentLocked), key, payloadLen, tid);
         }
         return;
      } catch(const OLCRestartException&) { yield(repeatCounter); }
   }
}

void BTree::insert(span<u8> key, span<u8> payload, int tid)
{
   assert((key.size()+payload.size()) <= BTreeNode::maxKVSize);

   for (u64 repeatCounter=0; ; repeatCounter++) {
      try {
         GuardO<BTreeNode> parent(metadataPageId, tid);
         GuardO<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId), parent, tid);

         while (node->isInner()) {
            parent = move(node);
            node = GuardO<BTreeNode>(parent->lookupInner(key), parent, tid);
         }

         if (node->hasSpaceFor(key.size(), payload.size())) {
            // only lock leaf
            GuardX<BTreeNode> nodeLocked(move(node));
            parent.release();
            nodeLocked->insertInPage(key, payload);
            return; // success
         }

         // lock parent and leaf
         GuardX<BTreeNode> parentLocked(move(parent));
         GuardX<BTreeNode> nodeLocked(move(node));
         trySplit(move(nodeLocked), move(parentLocked), key, payload.size(), tid);
         // insert hasn't happened, restart from root
      } catch(const OLCRestartException&) { yield(repeatCounter); }
   }
}

bool BTree::remove(span<u8> key, int tid)
{
   for (u64 repeatCounter=0; ; repeatCounter++) {
      try {
         GuardO<BTreeNode> parent(metadataPageId, tid);
         GuardO<BTreeNode> node(reinterpret_cast<MetaDataPage*>(parent.ptr)->getRoot(slotId), parent, tid);

         u16 pos;
         while (node->isInner()) {
            pos = node->lowerBound(key);
            PID nextPage = (pos == node->count) ? node->upperInnerNode : node->getChild(pos);
            parent = move(node);
            node = GuardO<BTreeNode>(nextPage, parent, tid);
         }

         bool found;
         unsigned slotId = node->lowerBound(key, found);
         if (!found)
            return false;

         unsigned sizeEntry = node->slot[slotId].keyLen + node->slot[slotId].payloadLen;
         if ((node->freeSpaceAfterCompaction()+sizeEntry >= BTreeNodeHeader::underFullSize) && (parent.pid != metadataPageId) && (parent->count >= 2) && ((pos + 1) < parent->count)) {
            // underfull
            GuardX<BTreeNode> parentLocked(move(parent));
            GuardX<BTreeNode> nodeLocked(move(node));
            GuardX<BTreeNode> rightLocked(parentLocked->getChild(pos + 1), tid);
            nodeLocked->removeSlot(slotId);
            if (rightLocked->freeSpaceAfterCompaction() >= (pageSize-BTreeNodeHeader::underFullSize)) {
               if (nodeLocked->mergeNodes(pos, parentLocked.ptr, rightLocked.ptr)) {
                  // XXX: should reuse page Id
               }
            }
         } else {
            GuardX<BTreeNode> nodeLocked(move(node));
            parent.release();
            nodeLocked->removeSlot(slotId);
         }
         return true;
      } catch(const OLCRestartException&) { yield(repeatCounter); }
   }
}

typedef u64 KeyType;

void handleSEGFAULT(int signo, siginfo_t* info, void* extra) {
   void* page = info->si_addr;
   if (bm->isValidPtr(page)) {
      cerr << "segfault restart " << bm->toPID(page) << endl;
      throw OLCRestartException();
   } else {
      cerr << "segfault " << page << endl;
      _exit(1);
   }
}

template <class Record>
struct vmcacheAdapter
{
   BTree tree;

   public:
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb, int tid) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      u8 kk[Record::maxFoldLength()];
      tree.scanAsc({k, l}, [&](BTreeNode& node, unsigned slot) {
         memcpy(kk, node.getPrefix(), node.prefixLen);
         memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
         typename Record::Key typedKey;
         Record::unfoldKey(kk, typedKey);
         return found_record_cb(typedKey, *reinterpret_cast<const Record*>(node.getPayload(slot).data()));
      }, tid);
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb, int tid) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      u8 kk[Record::maxFoldLength()];
      bool first = true;
      tree.scanDesc({k, l}, [&](BTreeNode& node, unsigned slot, bool exactMatch) {
         if (first) { // XXX: hack
            first = false;
            if (!exactMatch)
               return true;
         }
         memcpy(kk, node.getPrefix(), node.prefixLen);
         memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
         typename Record::Key typedKey;
         Record::unfoldKey(kk, typedKey);
         return found_record_cb(typedKey, *reinterpret_cast<const Record*>(node.getPayload(slot).data()));
      }, tid);
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record, int tid) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      tree.insert({k, l}, {(u8*)(&record), sizeof(Record)}, tid);
   }
   // -------------------------------------------------------------------------------------
   template<class Fn>
   void lookup1(const typename Record::Key& key, Fn fn, int tid) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      bool succ = tree.lookup({k, l}, [&](span<u8> payload) {
         fn(*reinterpret_cast<const Record*>(payload.data()));
      }, tid);
      assert(succ);
   }
   // -------------------------------------------------------------------------------------
   template<class Fn>
   void update1(const typename Record::Key& key, Fn fn, int tid) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      tree.updateInPlace({k, l}, [&](span<u8> payload) {
         fn(*reinterpret_cast<Record*>(payload.data()));
      }, tid);
   }
   // -------------------------------------------------------------------------------------
   // Returns false if the record was not found
   bool erase(const typename Record::Key& key, int tid) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      return tree.remove({k, l}, tid);
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f, int tid) {
      Field value;
      lookup1(key, [&](const Record& r) { value = r.*f; }, tid);
      return value;
   }

   u64 count(int tid) {
      u64 cnt = 0;
      tree.scanAsc({(u8*)nullptr, 0}, [&](BTreeNode& node, unsigned slot) { cnt++; return true; } , tid);
      return cnt;
   }

   u64 countw(Integer w_id, int tid) {
      u8 k[sizeof(Integer)];
      fold(k, w_id);
      u64 cnt = 0;
      u8 kk[Record::maxFoldLength()];
      tree.scanAsc({k, sizeof(Integer)}, [&](BTreeNode& node, unsigned slot) {
         memcpy(kk, node.getPrefix(), node.prefixLen);
         memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
         if (custom_memcmp(k, kk, sizeof(Integer))!=0)
            return false;
         cnt++;
         return true;
      }, tid);
      return cnt;
   }
};

int stick_this_thread_to_core(int core_id) {
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

template<class Fn>
void parallel_for(uint64_t begin, uint64_t end, uint64_t nthreads, Fn fn) {
   std::vector<std::thread> threads;
   uint64_t n = end-begin;
   if (n<nthreads)
      nthreads = n;
   uint64_t perThread = n/nthreads;
   for (unsigned i=0; i<nthreads; i++) {
      threads.emplace_back([&,i]() {
         stick_this_thread_to_core(i);
         uint64_t b = (perThread*i) + begin;
         uint64_t e = (i==(nthreads-1)) ? end : ((b+perThread) + begin);
         fn(i, b, e);
      });
   }
   for (auto& t : threads)
      t.join();
}

int main(int argc, char** argv) {
   /*if (bm->useExmap) {
      struct sigaction action;
      action.sa_flags = SA_SIGINFO;
      action.sa_sigaction = handleSEGFAULT;
      if (sigaction(SIGSEGV, &action, NULL) == -1) {
         perror("sigusr: sigaction");
         exit(1);
      }
   }*/

   unsigned nthreads = envOr("THREADS", 1);
   initRNG(nthreads);
   /*constexpr u64 N = 200000;
    auto start = chrono::high_resolution_clock::now();
    parallel_for(0, N*nthreads, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
        workerThreadId = worker;
        for (u64 i=begin; i<end; i++) {
            bm->fixX(i);
        }
    });
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> sec = end-start;
    cout << "linux,explicit_control," << nthreads << "," << (sec.count()/ (N*nthreads) / 1e-9)<< endl;
    return 0;*/
   u64 n = envOr("DATASIZE", 10);
   u64 runForSec = envOr("RUNFOR", 30);
   bool isRndread = envOr("RNDREAD", 0);

   u64 statDiff = 1e8;
   atomic<u64> txProgress(0);
   atomic<bool> keepRunning(true);
   auto systemName = "osv";

   auto statFn = [&]() {
      cout << "ts,tx,rmb,wmb,system,threads,datasize,workload,batch" << endl;
      u64 cnt = 0;
      for (uint64_t i=0; i<runForSec; i++) {
         sleep(1);
         float rmb = (bm->readCount.exchange(0)*pageSize)/(1024.0*1024);
         float wmb = (bm->writeCount.exchange(0)*pageSize)/(1024.0*1024);
         u64 prog = txProgress.exchange(0);
         cout << cnt++ << "," << prog << "," << rmb << "," << wmb << "," << systemName << "," << nthreads << "," << n << "," << (isRndread?"rndread":"tpcc") << "," << bm->batch << endl;//avg_memcpy << "," << avg_memcmp << endl;
      }  
      keepRunning = false;
   };


   if (isRndread) {
      BTree bt;
      bt.splitOrdered = true;

      {
         // insert
         parallel_for(0, n, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
            bm->registerThread();
            //workerThreadId = worker;
            array<u8, 120> payload;
            for (u64 i=begin; i<end; i++) {
               union { u64 v1; u8 k1[sizeof(u64)]; };
               v1 = __builtin_bswap64(i);
               memcpy(payload.data(), k1, sizeof(u64));
               bt.insert({k1, sizeof(KeyType)}, payload, worker);
            }
            bm->forgetThread();
         });
      }
      cerr << "space: " << (bm->allocCount.load()*pageSize)/(float)gb << " GB " << endl;
      /*u64 c = 10'000'000'000;
      vector<u64> v;
      u64 start = rdtsc();
      for(u64 cnt=0; cnt<c; cnt++){
          union { u64 v1; u8 k1[sizeof(u64)]; };
          v1 = __builtin_bswap64(cnt%n);
          //v.push_back(RandomGenerator::getRand<u64>(0, n));
          
          array<u8, 120> payload;
          bool succ = bt.lookup({k1, sizeof(u64)}, [&](span<u8> p) {
            memcpy(payload.data(), p.data(), p.size());
          }, 0);
          assert(succ);
          assert(custom_memcmp(k1, payload.data(), sizeof(u64))==0);
      }
      u64 stop = rdtsc();
      diff_time = stop-start;
      cout << "Avg cycles lookup: " << (double)diff_time / c << endl;
      return 0;*/

      bm->readCount = 0;
      bm->writeCount = 0;
      //acc_memcpy.store(0);
      //acc_memcmp.store(0);
      thread statThread(statFn);

      parallel_for(0, nthreads, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
         bm->registerThread();
         //workerThreadId = worker;
         u64 cnt = 0;
         u64 start = rdtsc();
         while (keepRunning.load()) {
            union { u64 v1; u8 k1[sizeof(u64)]; };
            v1 = __builtin_bswap64(RandomGenerator::getRand<u64>(0, n, worker));

            array<u8, 120> payload;
            bool succ = bt.lookup({k1, sizeof(u64)}, [&](span<u8> p) {
               memcpy(payload.data(), p.data(), p.size());
            }, worker);
            assert(succ);
            assert(custom_memcmp(k1, payload.data(), sizeof(u64))==0);

            cnt++;
            u64 stop = rdtsc();
            if ((stop-start) > statDiff) {
               txProgress += cnt;
               start = stop;
               cnt = 0;
            }
         }
         txProgress += cnt;
         bm->forgetThread();
      });

      statThread.join();
      return 0;
   }

   // TPC-C
   Integer warehouseCount = n;

   vmcacheAdapter<warehouse_t> warehouse;
   vmcacheAdapter<district_t> district;
   vmcacheAdapter<customer_t> customer;
   vmcacheAdapter<customer_wdl_t> customerwdl;
   vmcacheAdapter<history_t> history;
   vmcacheAdapter<neworder_t> neworder;
   vmcacheAdapter<order_t> order;
   vmcacheAdapter<order_wdc_t> order_wdc;
   vmcacheAdapter<orderline_t> orderline;
   vmcacheAdapter<item_t> item;
   vmcacheAdapter<stock_t> stock;

   TPCCWorkload<vmcacheAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock, true, warehouseCount, true);

   {
      tpcc.loadItem(0);
      tpcc.loadWarehouse(0);

      parallel_for(1, warehouseCount+1, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
         bm->registerThread();
         uint32_t tpcchistorycounter = 0;
         //workerThreadId = worker;
         for (Integer w_id=begin; w_id<end; w_id++) {
            //printf("%lu: %u\n", worker, w_id);
            tpcc.loadStock(w_id, worker);
            tpcc.loadDistrinct(w_id, worker);
            for (Integer d_id = 1; d_id <= 10; d_id++) {
               //printf("\t%u\n", d_id);
               tpcc.loadCustomer(w_id, d_id, worker, &tpcchistorycounter, worker);
               tpcc.loadOrders(w_id, d_id, worker);
            }
         }
         bm->forgetThread();
      });
   }
   cerr << "space: " << (bm->allocCount.load()*pageSize)/(float)gb << " GB " << endl;

   bm->readCount = 0;
   bm->writeCount = 0;
   thread statThread(statFn);

   //prof::config _config = { std::chrono::milliseconds(1) };
   //prof::start_sampler(_config);
   parallel_for(0, nthreads, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
      bm->registerThread();
      uint32_t tpcchistorycounter = 0;
      //workerThreadId = worker;
      u64 cnt = 0;
      u64 start = rdtsc();
      while (keepRunning.load()) {
         int w_id = tpcc.urand(1, warehouseCount, worker); // wh crossing
         tpcc.tx(w_id, worker, &tpcchistorycounter, worker);
         cnt++;
         u64 stop = rdtsc();
         if ((stop-start) > statDiff) {
            txProgress += cnt;
            start = stop;
            cnt = 0;
         }
      }
      txProgress += cnt;
      bm->forgetThread();
   });
   //prof::stop_sampler();

   statThread.join();
   //cout << "Avg cycles memcmp: " << (double)acc_time / acc_count << endl;
   cerr << "space: " << (bm->allocCount.load()*pageSize)/(float)gb << " GB " << endl;
   //print_maps();

   return 0;
}
