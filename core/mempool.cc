/*
 * Copyright (C) 2013 Cloudius Systems, Ltd.
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

#include <iostream>
#include <osv/mempool.hh>
#include <osv/ilog2.hh>
#include "arch-setup.hh"
#include <cassert>
#include <cstdint>
#include <boost/utility.hpp>
#include <string.h>
#include <lockfree/unordered-queue-mpsc.hh>
#include "arch.hh"
#include "libc/libc.hh"
#include "osv/mmu-defs.hh"
#include <osv/align.hh>
#include <osv/debug.hh>
#include <osv/kernel_config_memory_tracker.h>
#if CONF_memory_tracker
#include <osv/alloctracker.hh>
#endif
#include <atomic>
#include <osv/mmu.hh>
#include <osv/trace.hh>
#include <lockfree/ring.hh>
#include <osv/percpu-worker.hh>
#include <osv/preempt-lock.hh>
#include <osv/sched.hh>
#include <algorithm>
#include <osv/prio.hh>
#include <stdlib.h>
#include <osv/shrinker.h>
#include <osv/defer.hh>
#include <osv/dbg-alloc.hh>
#include <osv/export.h>

#include <osv/llfree.h>
#include <osv/ucache.hh>

#include <osv/kernel_config_lazy_stack.h>
#include <osv/kernel_config_lazy_stack_invariant.h>
#include <osv/kernel_config_memory_debug.h>
#include <osv/kernel_config_memory_l1_pool_size.h>
#include <osv/kernel_config_memory_page_batch_size.h>
#include <osv/kernel_config_memory_jvm_balloon.h>

// recent Boost gets confused by the "hidden" macro we add in some Musl
// header files, so need to undefine it
#undef hidden
#include <boost/dynamic_bitset.hpp>
#include <boost/lockfree/stack.hpp>
#include <boost/lockfree/policies.hpp>

TRACEPOINT(trace_memory_malloc, "buf=%p, len=%d, align=%d", void *, size_t,
           size_t);
TRACEPOINT(trace_memory_malloc_mempool, "buf=%p, req_len=%d, alloc_len=%d,"
           " align=%d", void*, size_t, size_t, size_t);
TRACEPOINT(trace_memory_malloc_large, "buf=%p, req_len=%d, alloc_len=%d,"
           " align=%d", void*, size_t, size_t, size_t);
TRACEPOINT(trace_memory_malloc_page, "buf=%p, req_len=%d, alloc_len=%d,"
           " align=%d", void*, size_t, size_t, size_t);
TRACEPOINT(trace_memory_free, "buf=%p", void *);
TRACEPOINT(trace_memory_realloc, "in=%p, newlen=%d, out=%p", void *, size_t, void *);
TRACEPOINT(trace_memory_page_alloc, "page=%p", void*);
TRACEPOINT(trace_memory_page_free, "page=%p", void*);
TRACEPOINT(trace_memory_huge_failure, "page ranges=%d", unsigned long);
TRACEPOINT(trace_memory_reclaim, "shrinker %s, target=%d, delta=%d", const char *, long, long);
TRACEPOINT(trace_memory_wait, "allocation size=%d", size_t);

namespace dbg {

static size_t object_size(void* v);

}

OSV_LIBSOLARIS_API
unsigned char *osv_reclaimer_thread;

namespace memory {

size_t phys_mem_size;

bi::set<page_range,
   bi::member_hook<page_range,
       bi::set_member_hook<>,
       &page_range::set_hook>,
   bi::constant_time_size<false>> phys_mem_ranges
    __attribute__((init_priority((int)init_prio::llfree_allocator)));

// Indicate whether malloc pools are initialized yet
bool smp_allocator{false};

bool use_linear_map{true};

// llfree page frame allocator
llf llfree_allocator
    __attribute__((init_priority((int)init_prio::llfree_allocator)));

size_t mempool_cpuid(){
    return sched::cpu::current() ? sched::cpu::current()->id : 0;
}

// A mutex serializing modifications to phys_mem_ranges.
// I.e. early allocations and initialization of page frame allocator
mutex phys_mem_ranges_lock;

#if CONF_memory_tracker
// Optionally track living allocations, and the call chain which led to each
// allocation. Don't set tracker_enabled before tracker is fully constructed.
alloc_tracker tracker;
bool tracker_enabled = false;
static inline void tracker_remember(void *addr, size_t size)
{
    // Check if tracker_enabled is true, but expect (be quicker in the case)
    // that it is false.
    if (__builtin_expect(tracker_enabled, false)) {
        tracker.remember(addr, size);
    }
}
static inline void tracker_forget(void *addr)
{
    if (__builtin_expect(tracker_enabled, false)) {
        tracker.forget(addr);
    }
}
#endif

//
// Before smp_allocator=true, threads are not yet available. malloc and free
// are used immediately after virtual memory is being initialized.
// sched::cpu::current() uses TLS which is set only later on.
//

static void garbage_collector_fn();
PCPU_WORKERITEM(garbage_collector, garbage_collector_fn);

//
// Since the small pools are managed per-cpu, malloc() always access the correct
// pool on the same CPU that it was issued from, free() on the other hand, may
// happen from different CPUs, so for each CPU, we maintain an array of
// lockless spsc rings, which combined are functioning as huge mpsc ring.
//
// A worker item is in charge of freeing the object from the original
// CPU it was allocated on.
//
// As much as the producer is concerned (cpu who did free()) -
// 1st index -> dest cpu
// 2nd index -> local cpu
//

class garbage_sink {
private:
    static const int signal_threshold = 256;
    lockfree::unordered_queue_mpsc<free_object> queue;
    int pushed_since_last_signal {};
public:
    void free(unsigned obj_cpu, free_object* obj)
    {
        queue.push(obj);
        if (++pushed_since_last_signal > signal_threshold) {
            garbage_collector.signal(sched::cpus[obj_cpu]);
            pushed_since_last_signal = 0;
        }
    }

    free_object* pop()
    {
        return queue.pop();
    }
};

static garbage_sink ***pcpu_free_list;

void pool::collect_garbage()
{
    assert(!sched::preemptable());

    unsigned cpu_id = mempool_cpuid();

    for (unsigned i = 0; i < sched::cpus.size(); i++) {
        auto sink = pcpu_free_list[cpu_id][i];
        free_object* obj;
        while ((obj = sink->pop())) {
            memory::pool::from_object(obj)->free_same_cpu(obj, cpu_id);
        }
    }
}

static void garbage_collector_fn()
{
#if CONF_lazy_stack_invariant
    assert(!sched::thread::current()->is_app());
#endif
    WITH_LOCK(preempt_lock) {
        pool::collect_garbage();
    }
}

// Memory allocation strategy
//
// Bits 44:46 of the virtual address are used to determine which memory
// allocator was used for allocation and, therefore, which one should be used
// to free the memory block.
//
// Small objects (< page size / 4) are stored in pages.  The beginning of the
// page contains a header with a pointer to a pool, consisting of all free
// objects of that size.  The pool maintains a singly linked list of free
// objects, and adds or frees pages as needed.
//
// Objects which size is in range (page size / 4, page size] are given a whole
// page from per-CPU page buffer.  Such objects don't need header they are
// known to be not larger than a single page.  Page buffer is refilled by
// allocating memory from large allocator.
//
// Large objects are rounded up to page size.  They have a header in front that
// contains the page size.  There is gap between the header and the acutal
// object to ensure proper alignment.  Unallocated page ranges are kept either
// in one of 16 doubly linked lists or in a red-black tree sorted by their
// size.  List k stores page ranges which page count is in range
// [2^k, 2^(k + 1)).  The tree stores page ranges that are too big for any of
// the lists.  Memory is allocated from the smallest, non empty list, that
// contains page ranges large enough. If there is no such list then it is a
// worst-fit allocation form the page ranges in the tree.

pool::pool(unsigned size)
    : _size(size)
    , _free()
{
    assert(size + sizeof(page_header) <= page_size);
}

pool::~pool()
{
}

const size_t pool::max_object_size = page_size / 4;
const size_t pool::min_object_size = sizeof(free_object);

pool::page_header* pool::to_header(free_object* object)
{
    return reinterpret_cast<page_header*>(
                 reinterpret_cast<std::uintptr_t>(object) & ~(page_size - 1));
}

TRACEPOINT(trace_pool_alloc, "this=%p, obj=%p", void*, void*);
TRACEPOINT(trace_pool_free, "this=%p, obj=%p", void*, void*);
TRACEPOINT(trace_pool_free_same_cpu, "this=%p, obj=%p", void*, void*);
TRACEPOINT(trace_pool_free_different_cpu, "this=%p, obj=%p, obj_cpu=%d", void*, void*, unsigned);

void* pool::alloc()
{
    void * ret = nullptr;
#if CONF_lazy_stack_invariant
    assert(sched::preemptable() && arch::irq_enabled());
#endif
#if CONF_lazy_stack
    arch::ensure_next_stack_page();
#endif
    WITH_LOCK(preempt_lock) {

        // We enable preemption because add_page() may take a Mutex.
        // this loop ensures we have at least one free page that we can
        // allocate from, in from the context of the current cpu
        while (_free->empty()) {
            DROP_LOCK(preempt_lock) {
                add_page();
            }
        }

        // We have a free page, get one object and return it to the user
        auto it = _free->begin();
        page_header *header = &(*it);
        free_object* obj = header->local_free;
        ++header->nalloc;
        header->local_free = obj->next;
        if (!header->local_free) {
            _free->erase(it);
        }
        ret = obj;
    }

    trace_pool_alloc(this, ret);
    return ret;
}

unsigned pool::get_size()
{
    return _size;
}

static inline void* untracked_alloc_page();
static inline void untracked_free_page(void *v);

void pool::add_page()
{
    // FIXME: This allocates a map from the linear mapping
    // Changing this breaks mmu's superblock_manager::free_range if
    //   * The free range can be merged with the next one and
    //   * The pool in which the node of the following range was allocated gets freed frees the page and
    //   * The page was allocated by the same core that calld free range in the beginning
    //   As this would then causes a recursive call into free_range. Is is therefore kept unmapped for now
    void* page = untracked_alloc_page();

#if CONF_lazy_stack_invariant
    assert(sched::preemptable() && arch::irq_enabled());
#endif
#if CONF_lazy_stack
    arch::ensure_next_stack_page();
#endif
    WITH_LOCK(preempt_lock) {
        page_header* header = new (page) page_header;
        header->cpu_id = mempool_cpuid();
        header->owner = this;
        header->nalloc = 0;
        header->local_free = nullptr;
        for (auto p = page + page_size - _size; p >= header + 1; p -= _size) {
            auto obj = static_cast<free_object*>(p);
            obj->next = header->local_free;
            header->local_free = obj;
        }
        _free->push_back(*header);
        if (_free->empty()) {
            /* encountered when starting to enable TLS for AArch64 in mixed
               LE / IE tls models */
            abort();
        }
    }
}

inline bool pool::have_full_pages()
{
    return !_free->empty() && _free->back().nalloc == 0;
}

void pool::free_same_cpu(free_object* obj, unsigned cpu_id)
{
    void* object = static_cast<void*>(obj);
    trace_pool_free_same_cpu(this, object);

    page_header* header = to_header(obj);
    if (!--header->nalloc && have_full_pages()) {
        if (header->local_free) {
            _free->erase(_free->iterator_to(*header));
        }
        DROP_LOCK(preempt_lock) {
            untracked_free_page(header);
        }
    } else {
        if (!header->local_free) {
            if (header->nalloc) {
                _free->push_front(*header);
            } else {
                // keep full pages on the back, so they're not fragmented
                // early, and so we find them easily in have_full_pages()
                _free->push_back(*header);
            }
        }
        obj->next = header->local_free;
        header->local_free = obj;
    }
}

void pool::free_different_cpu(free_object* obj, unsigned obj_cpu, unsigned cur_cpu)
{
    trace_pool_free_different_cpu(this, obj, obj_cpu);
    auto sink = memory::pcpu_free_list[obj_cpu][cur_cpu];
    sink->free(obj_cpu, obj);
}

void pool::free(void* object)
{
    trace_pool_free(this, object);

#if CONF_lazy_stack_invariant
    assert(sched::preemptable() && arch::irq_enabled());
#endif
#if CONF_lazy_stack
    arch::ensure_next_stack_page();
#endif
    WITH_LOCK(preempt_lock) {

        free_object* obj = static_cast<free_object*>(object);
        page_header* header = to_header(obj);
        unsigned obj_cpu = header->cpu_id;
        unsigned cur_cpu = mempool_cpuid();

        if (obj_cpu == cur_cpu) {
            // free from the same CPU this object has been allocated on.
            free_same_cpu(obj, obj_cpu);
        } else {
            // free from a different CPU. we try to hand the buffer
            // to the proper worker item that is pinned to the CPU that this buffer
            // was allocated from, so it'll free it.
            free_different_cpu(obj, obj_cpu, cur_cpu);
        }
    }
}

pool* pool::from_object(void* object)
{
    auto header = to_header(static_cast<free_object*>(object));
    return header->owner;
}

class malloc_pool : public pool {
public:
    malloc_pool();
private:
    static size_t compute_object_size(unsigned pos);
};

malloc_pool malloc_pools[ilog2_roundup_constexpr(page_size) + 1]
    __attribute__((init_priority((int)init_prio::malloc_pools)));

struct mark_smp_allocator_intialized {
    mark_smp_allocator_intialized() {
        // FIXME: Handle CPU hot-plugging.
        auto ncpus = sched::cpus.size();
        // Our malloc() is very coarse so allocate all the queues in one large buffer.
        // We allocate at least one page because current implementation of aligned_alloc()
        // is not capable of ensuring aligned allocation for small allocations.
        auto buf = aligned_alloc(alignof(garbage_sink),
                    std::max(page_size, sizeof(garbage_sink) * ncpus * ncpus));
        pcpu_free_list = new garbage_sink**[ncpus];
        for (auto i = 0U; i < ncpus; i++) {
            pcpu_free_list[i] = new garbage_sink*[ncpus];
            for (auto j = 0U; j < ncpus; j++) {
                static_assert(!(sizeof(garbage_sink) %
                        alignof(garbage_sink)), "garbage_sink align");
                auto p = pcpu_free_list[i][j] = static_cast<garbage_sink *>(
                        buf + sizeof(garbage_sink) * (i * ncpus + j));
                new (p) garbage_sink;
            }
        }
        smp_allocator = true;
    }
} s_mark_smp_alllocator_initialized __attribute__((init_priority((int)init_prio::malloc_pools)));

malloc_pool::malloc_pool()
    : pool(compute_object_size(this - malloc_pools))
{
}

size_t malloc_pool::compute_object_size(unsigned pos)
{
    size_t size = 1 << pos;
    if (size > max_object_size) {
        size = max_object_size;
    }
    return size;
}

page_range::page_range(size_t _size)
    : size(_size)
{
}

struct addr_cmp {
    bool operator()(const page_range& fpr1, const page_range& fpr2) const {
        return &fpr1 < &fpr2;
    }
};

namespace bi = boost::intrusive;

mutex free_page_ranges_lock;

// Our notion of free memory is "whatever is in the page ranges". Therefore it
// starts at 0, and increases as we add page ranges.
//
// Updates to total should be fairly rare. We only expect updates upon boot,
// and eventually hotplug in an hypothetical future
static std::atomic<size_t> total_memory(0);
static size_t watermark_lo(0);
#if CONF_memory_jvm_balloon
static std::atomic<size_t> current_jvm_heap_memory(0);
#endif

// At least two (x86) huge pages worth of size;
static size_t constexpr min_emergency_pool_size = 4 << 20;

__thread unsigned emergency_alloc_level = 0;

reclaimer_lock_type reclaimer_lock;

extern "C" OSV_LIBSOLARIS_API void thread_mark_emergency()
{
    emergency_alloc_level = 1;
}

reclaimer reclaimer_thread
    __attribute__((init_priority((int)init_prio::reclaimer)));

void wake_reclaimer()
{
    reclaimer_thread.wake();
}

static void on_new_memory(size_t mem) { total_memory.fetch_add(mem); }

namespace stats {
    size_t free() {
        if(llfree_allocator.is_ready())
            return llfree_allocator.free_memory();

        size_t ret{0};
        for(auto& r : phys_mem_ranges) {
            ret += r.size;
        }
        return ret;
    }
    size_t total() { return total_memory.load(std::memory_order_relaxed); }
}

void reclaimer::wake()
{
    _blocked.wake_one();
}

pressure reclaimer::pressure_level()
{
    assert(mutex_owned(&free_page_ranges_lock));
    if (stats::free() < watermark_lo) {
        return pressure::PRESSURE;
    }
    return pressure::NORMAL;
}

ssize_t reclaimer::bytes_until_normal(pressure curr)
{
    assert(mutex_owned(&free_page_ranges_lock));
    if (curr == pressure::PRESSURE) {
        return watermark_lo - stats::free();
    } else {
        return 0;
    }
}

void oom()
{
    abort("Out of memory: could not reclaim any further. Current memory: %d Kb", stats::free() >> 10);
}

void reclaimer::wait_for_minimum_memory()
{
    if (emergency_alloc_level) {
        return;
    }

    if (stats::free() < min_emergency_pool_size) {
        // Nothing could possibly give us memory back, might as well use up
        // everything in the hopes that we only need a tiny bit more..
        if (!_active_shrinkers) {
            return;
        }
        wait_for_memory(min_emergency_pool_size - stats::free());
    }
}

// Allocating memory here can lead to a stack overflow. That is why we need
// to use boost::intrusive for the waiting lists.
//
// Also, if the reclaimer itself reaches a point in which it needs to wait for
// memory, there is very little hope and we would might as well give up.
void reclaimer::wait_for_memory(size_t mem)
{
    // If we're asked for an impossibly large allocation, abort now instead of
    // the reclaimer thread aborting later. By aborting here, the application
    // bug will be easier for the user to debug. An allocation larger than RAM
    // can never be satisfied, because OSv doesn't do swapping.
    if (mem > memory::stats::total())
        abort("Unreasonable allocation attempt, larger than memory. Aborting.");
    trace_memory_wait(mem);
    _oom_blocked.wait(mem);
}


void shrinker::deactivate_shrinker()
{
    reclaimer_thread._active_shrinkers -= _enabled;
    _enabled = 0;
}

void shrinker::activate_shrinker()
{
    reclaimer_thread._active_shrinkers += !_enabled;
    _enabled = 1;
}

shrinker::shrinker(std::string name)
    : _name(name)
{
    // Since we already have to take that lock anyway in pretty much every
    // operation, just reuse it.
    WITH_LOCK(reclaimer_thread._shrinkers_mutex) {
        reclaimer_thread._shrinkers.push_back(this);
        reclaimer_thread._active_shrinkers += 1;
    }
}

extern "C"
void *osv_register_shrinker(const char *name,
                            size_t (*func)(size_t target, bool hard))
{
    return reinterpret_cast<void *>(new c_shrinker(name, func));
}

bool reclaimer_waiters::wake_waiters()
{
    // bool woken = false;
    // assert(mutex_owned(&free_page_ranges_lock));
    // free_page_ranges.for_each([&] (page_range& fp) {
    //     // We won't do the allocations, so simulate. Otherwise we can have
    //     // 10Mb available in the whole system, and 4 threads that wait for
    //     // it waking because they all believe that memory is available
    //     auto in_this_page_range = fp.size;
    //     // We expect less waiters than page ranges so the inner loop is one
    //     // of waiters. But we cut the whole thing short if we're out of them.
    //     if (_waiters.empty()) {
    //         woken = true;
    //         return false;
    //     }
    //
    //     auto it = _waiters.begin();
    //     while (it != _waiters.end()) {
    //         auto& wr = *it;
    //         it++;
    //
    //         if (in_this_page_range >= wr.bytes) {
    //             in_this_page_range -= wr.bytes;
    //             _waiters.erase(_waiters.iterator_to(wr));
    //             wr.owner->wake();
    //             wr.owner = nullptr;
    //             woken = true;
    //         }
    //     }
    //     return true;
    // });
    //
    // if (!_waiters.empty()) {
    //     reclaimer_thread.wake();
    // }
    // return woken;
    return true;
}

// Note for callers: Ideally, we would not only wake, but already allocate
// memory here and pass it back to the waiter. However, memory is not always
// allocated the same way (ex: refill_page_buffer is completely different from
// malloc_large) and that could be cumbersome.
//
// That means that this returning will only mean allocation may succeed, not
// that it will.  Because of that, it is of extreme importance that callers
// pass the exact amount of memory they are waiting for. So for instance, if
// your allocation is 2Mb in size + a 4k header, "bytes" below should be 2Mb +
// 4k, not 2Mb. Failing to do so could livelock the system, that would forever
// wake up believing there is enough memory, when in reality there is not.
void reclaimer_waiters::wait(size_t bytes)
{
    assert(mutex_owned(&free_page_ranges_lock));

    sched::thread *curr = sched::thread::current();

    // Wait for whom?
    if (curr == reclaimer_thread._thread.get()) {
        oom();
     }

    wait_node wr;
    wr.owner = curr;
    wr.bytes = bytes;
    _waiters.push_back(wr);

    // At this point the reclaimer thread already knows there are waiters,
    // because the _waiters_list was already updated.
    reclaimer_thread.wake();
    sched::thread::wait_until(&free_page_ranges_lock, [&] { return !wr.owner; });
}

reclaimer::reclaimer()
    : _oom_blocked(), _thread(sched::thread::make([&] { _do_reclaim(); }, sched::thread::attr().detached().name("reclaimer").stack(page_size)))
{
    osv_reclaimer_thread = reinterpret_cast<unsigned char *>(_thread.get());
    _thread->start();
}

bool reclaimer::_can_shrink()
{
    auto p = pressure_level();
    // The active fields are protected by the _shrinkers_mutex lock, but there
    // is no need to take it. Worst that can happen is that we either defer
    // this pass, or take an extra pass without need for it.
    if (p == pressure::PRESSURE) {
        return _active_shrinkers != 0;
    }
    return false;
}

void reclaimer::_shrinker_loop(size_t target, std::function<bool ()> hard)
{
    // FIXME: This simple loop works only because we have a single shrinker
    // When we have more, we need to probe them and decide how much to take from
    // each of them.
    WITH_LOCK(_shrinkers_mutex) {
        // We execute this outside the free_page_ranges lock, so the threads
        // freeing memory (or allocating, for that matter) will have the chance
        // to manipulate the free_page_ranges structure.  Executing the
        // shrinkers with the lock held would result in a deadlock.
        for (auto s : _shrinkers) {
            // FIXME: If needed, in the future we can introduce another
            // intermediate threshold that will put is into hard mode even
            // before we have waiters.
            size_t freed = s->request_memory(target, hard());
            trace_memory_reclaim(s->name().c_str(), target, freed);
        }
    }
}

void reclaimer::_do_reclaim()
{
    ssize_t target;
    emergency_alloc_level = 1;

    while (true) {
        WITH_LOCK(free_page_ranges_lock) {
            _blocked.wait(free_page_ranges_lock);
            target = bytes_until_normal();
        }

#if CONF_memory_jvm_balloon
        // This means that we are currently ballooning, we should
        // try to serve the waiters from temporary memory without
        // going on hard mode. A big batch of more memory is likely
        // in its way.
        if (_oom_blocked.has_waiters() && throttling_needed()) {
            _shrinker_loop(target, [] { return false; });
            WITH_LOCK(free_page_ranges_lock) {
                if (_oom_blocked.wake_waiters()) {
                        continue;
                }
            }
        }
#endif

        _shrinker_loop(target, [this] { return _oom_blocked.has_waiters(); });

        WITH_LOCK(free_page_ranges_lock) {
            if (target >= 0) {
                // Wake up all waiters that are waiting and now have a chance to succeed.
                // If we could not wake any, there is nothing really we can do.
                if (!_oom_blocked.wake_waiters()) {
                    oom();
                }
            }

#if CONF_memory_jvm_balloon
            if (balloon_api) {
                balloon_api->voluntary_return();
            }
#endif
        }
    }
}
static size_t large_object_offset(void *&obj)
{
    void *original_obj = obj;
    obj = align_down(obj - 1, page_size);
    return reinterpret_cast<uint64_t>(original_obj) - reinterpret_cast<uint64_t>(obj);
}

static size_t large_object_size(void *obj)
{
    size_t offset = large_object_offset(obj);
    auto header = static_cast<page_range*>(obj);
    return header->size - offset;
}

void llf::init(size_t cores) {
    // llfree sees the memory as contiguous, we reserve the gaps and already allocated memory below
    size_t highest{0};
    for(auto& pr : phys_mem_ranges){
        size_t cur = reinterpret_cast<size_t>(&pr) + pr.size ;
        if(cur > highest) highest = cur;
    }

    self = llfree_setup(
        cores,
        (highest - mmu::get_mem_area_base(mmu::mem_area::main)) / page_size,
        LLFREE_INIT_FREE
    );

    if(self){
        reserve_allocated();
        ready = true;
    } else {
      printf("llfree init failed\n");
    }
}

void llf::reserve_allocated(){
    WITH_LOCK(phys_mem_ranges_lock){
        for(size_t frame{0}; frame < llfree_frames(self); ++frame){
            void *addr = idx_to_virt(frame);
            for(auto& pr : phys_mem_ranges){
                if(addr >= &pr && addr < (static_cast<void *>(&pr) + pr.size)){
                    frame += pr.size / page_size - 1;
                    phys_mem_ranges.erase(phys_mem_ranges.iterator_to(pr));
                    break;
                }
            }
            if(addr != idx_to_virt(frame))
                continue;
            alloc_page_at(frame);
        }
    }
}

size_t llf::free_memory(){
    return llfree_free_frames(self) * page_size;
}

void llf::add_region(void *mem_start, size_t mem_size){
    assert(!is_ready());

    if (!mem_size) {
        return;
    }

    // ensure every region is page_size aligned
    auto a = reinterpret_cast<uintptr_t>(mem_start);
    auto delta = align_up(a, page_size) - a;
    if (delta > mem_size) {
        return;
    }
    mem_start += delta;
    mem_size -= delta;
    mem_size = align_down(mem_size, page_size);
    if (!mem_size) {
        return;
    }

    auto pr = new (mem_start) page_range(mem_size);
    WITH_LOCK(phys_mem_ranges_lock){
        phys_mem_ranges.insert(*pr);
    }
}

bool llf::is_ready(){
    return ready;
}

void *llf::alloc_page() {
    assert(is_ready());

    llfree_result_t page = llfree_get(
        self,
        mempool_cpuid(),
        llflags(0)
    );

    if(llfree_is_ok(page))
        return idx_to_virt(page.frame);

    oom();
    return nullptr;
}

u64 llf::alloc_page_phys_addr(unsigned order){
   assert(is_ready());

    llfree_result_t page = llfree_get(
        self,
        mempool_cpuid(),
        llflags(order)
    );

    if(llfree_is_ok(page)){
        return page.frame;
    }

    oom();
    return 0;

}

void *llf::alloc_huge_page(unsigned order){
   assert(is_ready());

    llfree_result_t page = llfree_get(
        self,
        mempool_cpuid(),
        llflags(order)
    );

    if(llfree_is_ok(page)){
        return idx_to_virt(page.frame);
    }

    oom();
    return nullptr;

}

void *llf::alloc_page_at(size_t frame){
    llfree_result_t page = llfree_get_at(
        self,
        mempool_cpuid(),
        frame,
        llflags(0)
    );

    if(llfree_is_ok(page)){
        return idx_to_virt(page.frame);
    }

    return nullptr;
}

void llf::free_page(void *addr){
    assert(is_ready());

    llfree_result_t res = llfree_put(
        self,
        mempool_cpuid(),
        virt_to_idx(addr),
        llflags(0)
    );

    assert(llfree_is_ok(res));
}

void llf::free_page(void *addr, unsigned order){
    assert(is_ready());

    llfree_result_t res = llfree_put(
        self,
        mempool_cpuid(),
        virt_to_idx(addr),
        llflags(order)
    );

    assert(llfree_is_ok(res));
}

void llf::free_page_phys_addr(u64 idx, unsigned order){
    assert(is_ready());

    llfree_result_t res = llfree_put(
        self,
        mempool_cpuid(),
        idx,
        llflags(order)
    );

    assert(llfree_is_ok(res));
}

void *llf::idx_to_virt(u64 idx){
    // TODO: change this as soon as we have our own mapping
    return mmu::phys_cast<void>(idx*page_size);
}
u64 llf::virt_to_idx(void *addr){
    // TODO: change this as soon as we have out own mapping
    if(mmu::get_mem_area(addr) == mmu::mem_area::page)
        addr = mmu::translate_mem_area(mmu::mem_area::page, mmu::mem_area::main, addr);
    return reinterpret_cast<u64>(
        addr - mmu::get_mem_area_base(mmu::mem_area::main)) / page_size;
}

void add_llfree_region(void *addr, size_t size){
    on_new_memory(size);

    llfree_allocator.add_region(addr, size);
}

unsigned llf::order(size_t size){
    if(size <= page_size)
      return 0;

    size_t frames = (size - 1) / page_size;
    unsigned order{0};
    while(frames){
        frames /= 2;
        order++;
    }
    return order;
}

void *early_alloc_pages(size_t size) {
    assert(!llfree_allocator.is_ready());

    size = align_up(size, page_size);

    WITH_LOCK(phys_mem_ranges_lock){
        for(auto& pr : phys_mem_ranges){
            if(pr.size >= size){
                auto range = &pr;
                phys_mem_ranges.erase(
                    phys_mem_ranges.iterator_to(pr));

                auto& pr = *range;
                // re-insert the rest of the memory region
                if(pr.size > size){
                    auto& np = *new (static_cast<void*>(&pr) + size)
                                    page_range(pr.size - size);
                    phys_mem_ranges.insert(np);
                }

                pr.size = size;

                return reinterpret_cast<void *>(&pr);
            }
        }
    }
    std::cout << "size: " << size << std::endl << std::flush;
    memory::oom();
    return nullptr;
}

void early_free_pages(void *obj, size_t size){
    WITH_LOCK(phys_mem_ranges_lock){
        for(auto& pr : phys_mem_ranges){
            if(obj + size == &pr){
                size += pr.size;
                phys_mem_ranges.erase(phys_mem_ranges.iterator_to(pr));
                return early_free_pages(obj, size);
            }

            if(static_cast<void *>(&pr) + pr.size == obj){
                pr.size += size;
                return;
            }
        }

        auto pr = new (obj) page_range(size);
        phys_mem_ranges.insert(*pr);
    }
}

static void free_large(void* obj)
{
    obj = align_down(obj - 1, page_size);
    auto pr = static_cast<page_range *>(obj);
    if(llfree_allocator.is_ready()){
        llfree_allocator.free_page(obj, llf::order(pr->size));
    } else {
        early_free_pages(obj, pr->size);
    }
}

static void* malloc_large(size_t size, size_t alignment, bool block = true, bool contiguous = true)
{
    void* ret;
    size_t requested_size{size};
    size_t offset;
    if (alignment >= 2ul * 1024 * 1024){
        offset = 0;
    } else if (alignment < page_size) {
        offset = align_up(sizeof(page_range), alignment);
    } else {
        offset = page_size;
    }
    // Raw size required
    size += offset;
    size = align_up(size, page_size);

    // Use mapped memory is possible
    if(!contiguous && (!use_linear_map || (llfree_allocator.is_ready() && size > llf_max_size - page_size))){
        ret = mmu::map_anon(nullptr, requested_size, mmu::mmap_populate, mmu::perm_read | mmu::perm_write);
        trace_memory_malloc_large(ret, requested_size, requested_size, page_size);
        return ret;
    } else if (llfree_allocator.is_ready() && size > llf_max_size){
        abort("physically contiguous allocations above 0x%lxB are not possible (requested: 0x%lxB)\n", llf_max_size, size);
    }

    // handle in linearly mapped, contiguous physical memory.
    // Additional space for a header needs to be allocated in this case
    if(llfree_allocator.is_ready()){
        unsigned order = llf::order(size);
        ret = llfree_allocator.alloc_huge_page(order);
    } else {
        ret = early_alloc_pages(size);
    }

    size_t* ret_header = static_cast<size_t*>(ret);
    *ret_header = size;
    trace_memory_malloc_large(ret, requested_size, size, alignment);
    return ret + offset;
}

static void mapped_free_large(void *object)
{
    mmu::munmap_vma(object);
}

std::atomic<unsigned> llf_cnt{0};
static sched::cpu::notifier _notifier([]{
    // TODO reinit llfree if necessary
    // Check smp_allocator init in other branch for how to wait for all threads to be initialized
});
//
// Following variables and functions are used to implement simple
// early (pre-SMP) memory allocation scheme.
mutex early_alloc_lock;
// early_object_pages holds a pointer to the beginning of the current page
// intended to be used for next early object allocation
static void* early_object_page = nullptr;
// early_alloc_next_offset points to the 0-relative address of free
// memory within a page pointed by early_object_page. Normally it is an
// offset of the first byte right after last byte of the previously
// allocated object in current page. Typically it is NOT an offset
// of next object to be allocated as we need to account for proper
// alignment and space for 2-bytes size field preceding every
// allocated object.
static size_t early_alloc_next_offset = 0;
static size_t early_alloc_previous_offset = 0;

static early_page_header* to_early_page_header(void* object)
{
    return reinterpret_cast<early_page_header*>(
            reinterpret_cast<std::uintptr_t>(object) & ~(page_size - 1));
}

static void setup_early_alloc_page() {
    early_object_page = alloc_page();
    early_page_header *page_header = to_early_page_header(early_object_page);
    // Set the owner field to null so that functions that free objects
    // or compute object size can differentiate between post-SMP malloc pool
    // and early (pre-SMP) allocation
    page_header->owner = nullptr;
    page_header->allocations_count = 0;
    early_alloc_next_offset = sizeof(early_page_header);
}

static bool will_fit_in_early_alloc_page(size_t size, size_t alignment)
{
    auto lowest_offset = align_up(sizeof(early_page_header) + sizeof(unsigned short), alignment);
    return lowest_offset + size <= page_size;
}

//
// This function implements simple but effective scheme
// of allocating objects of size < 4096 before SMP is setup. It does so by
// remembering where within current page free memory starts. Then it
// calculates next closest offset matching specified alignment
// and verifies there is enough space until end of the current
// page to allocate from. If not it allocates next full page
// to find enough requested space.
static void* early_alloc_object(size_t size, size_t alignment)
{
    WITH_LOCK(early_alloc_lock) {
        if (!early_object_page) {
            setup_early_alloc_page();
        }

        // Each object is preceded by 2 bytes (unsigned short) of size
        // so make sure there is enough room between new object and previous one
        size_t offset = align_up(early_alloc_next_offset + sizeof(unsigned short), alignment);

        if (offset + size > page_size) {
            setup_early_alloc_page();
            offset = align_up(early_alloc_next_offset + sizeof(unsigned short), alignment);
        }

        // Verify we have enough space to satisfy this allocation
        assert(offset + size <= page_size);

        auto ret = early_object_page + offset;
        early_alloc_previous_offset = early_alloc_next_offset;
        early_alloc_next_offset = offset + size;

        // Save size of the allocated object 2 bytes before it address
        *reinterpret_cast<unsigned short *>(ret - sizeof(unsigned short)) =
                static_cast<unsigned short>(size);
        to_early_page_header(early_object_page)->allocations_count++;
        return ret;
    }
}

//
// This function fairly rarely actually frees previously
// allocated memory. It does so only if all objects
// have been freed in a page based on allocations_count or
// if the object being freed is the last one that was allocated.
static void early_free_object(void *object)
{
    WITH_LOCK(early_alloc_lock) {
        early_page_header *page_header = to_early_page_header(object);
        assert(!page_header->owner);
        unsigned short *size_addr = reinterpret_cast<unsigned short*>(object - sizeof(unsigned short));
        unsigned short size = *size_addr;
        if (!size) {
            return;
        }

        *size_addr = 0; // Reset size to 0 so that we know this object was freed and prevent from freeing again
        page_header->allocations_count--;
        if (page_header->allocations_count <= 0) { // Any early page
            free_page(page_header);
            if (early_object_page == page_header) {
                early_object_page = nullptr;
            }
        }
        else if(early_object_page == page_header) { // Current early page
            // Assuming we are freeing the object that was the last one allocated,
            // simply subtract its size from the early_alloc_next_offset to arrive at the previous
            // value of early_alloc_next_offset it was when allocating last object
            void *last_obj = static_cast<void *>(page_header) + (early_alloc_next_offset - size);
            // Check if we are freeing last allocated object (free followed by malloc)
            // and deallocate if so by moving the early_alloc_next_offset to the previous
            // position
            if (last_obj == object) {
                early_alloc_next_offset = early_alloc_previous_offset;
            }
        }
    }
}

static size_t early_object_size(void* v)
{
    return *reinterpret_cast<unsigned short*>(v - sizeof(unsigned short));
}

static void* untracked_alloc_page()
{
    void* ret;

    if (!llfree_allocator.is_ready()) {
        ret = early_alloc_pages(page_size);
    } else {
        ret = llfree_allocator.alloc_page();
    }
    trace_memory_page_alloc(ret);
    return ret;
}

void* alloc_page()
{
    void *p = untracked_alloc_page();
#if CONF_memory_tracker
    tracker_remember(p, page_size);
#endif
    return p;
}

static inline void untracked_free_page(void *v)
{
    trace_memory_page_free(v);
    if (!llfree_allocator.is_ready()) {
        early_free_pages(v, page_size);
    } else {
        llfree_allocator.free_page(v);
    }
}

void free_page(void* v)
{
    untracked_free_page(v);
#if CONF_memory_tracker
    tracker_forget(v);
#endif
}

/* Allocate a huge page of a given size N (which must be a power of two)
 * N bytes of contiguous physical memory whose address is a multiple of N.
 * Memory allocated with alloc_huge_page() must be freed with free_huge_page(),
 * not free(), as the memory is not preceded by a header.
 */
void* alloc_huge_page(size_t N)
{
  if(!llfree_allocator.is_ready()){
      return early_alloc_pages(N);
  }
  return llfree_allocator.alloc_huge_page(llf::order(N));
}

void free_huge_page(void* v, size_t N)
{
  if(!llfree_allocator.is_ready()){
      early_free_pages(v, N);
  } else {
      llfree_allocator.free_page(v, llf::order(N));
  }
}

void  __attribute__((constructor(init_prio::mempool))) setup()
{
    arch_setup_free_memory();
}

}

extern "C" {
    void* malloc(size_t size);
    void free(void* object);
    size_t malloc_usable_size(void *object);
}

static inline void* std_malloc(size_t size, size_t alignment)
{
    if ((ssize_t)size < 0)
        return libc_error_ptr<void *>(ENOMEM);
    void *ret;
    size_t minimum_size = std::max(size, memory::pool::min_object_size);

    if (memory::smp_allocator && size <= memory::pool::max_object_size && alignment <= minimum_size) {
        unsigned n = ilog2_roundup(minimum_size);
        ret = memory::malloc_pools[n].alloc();
        if(mmu::get_mem_area(ret) != mmu::mem_area::mempool){
            ret = translate_mem_area(mmu::get_mem_area(ret), mmu::mem_area::mempool, ret);
        }
        trace_memory_malloc_mempool(ret, size, 1 << n, alignment);
    } else if (memory::smp_allocator && alignment <= memory::pool::max_object_size && minimum_size <= alignment) {
        unsigned n = ilog2_roundup(alignment);
        ret = memory::malloc_pools[n].alloc();
        if(mmu::get_mem_area(ret) != mmu::mem_area::mempool){
            ret = translate_mem_area(mmu::get_mem_area(ret), mmu::mem_area::mempool, ret);
        }
        trace_memory_malloc_mempool(ret, size, 1 << n, alignment);
    } else if (memory::will_fit_in_early_alloc_page(size,alignment) && !memory::smp_allocator) {
        ret = memory::early_alloc_object(size, alignment);
        if(mmu::get_mem_area(ret) != mmu::mem_area::mempool){
            ret = translate_mem_area(mmu::get_mem_area(ret), mmu::mem_area::mempool, ret);
        }
    } else if (minimum_size <= memory::page_size && alignment <= memory::page_size && memory::use_linear_map) {
        ret = mmu::translate_mem_area(mmu::mem_area::main, mmu::mem_area::page, memory::alloc_page());
        trace_memory_malloc_page(ret, size, memory::page_size, alignment);
    } else {
        ret = memory::malloc_large(minimum_size, alignment, true, false);
    } 

#if CONF_memory_tracker
    memory::tracker_remember(ret, size);
#endif

    return ret;
}

void* calloc(size_t nmemb, size_t size)
{
    if (nmemb == 0 || size == 0)
        return malloc(0);
    if (nmemb > std::numeric_limits<size_t>::max() / size)
        return nullptr;
    auto n = nmemb * size;
    auto p = malloc(n);
    if (!p)
        return nullptr;
    memset(p, 0, n);
    return p;
}

static size_t object_size(void *object)
{
    if (!mmu::is_linear_mapped(object, 0)) {
        return mmu::vma_size(object);
    }

    switch (mmu::get_mem_area(object)) {
    case mmu::mem_area::main:
        return memory::large_object_size(object);
    case mmu::mem_area::mempool:
        object = mmu::translate_mem_area(mmu::mem_area::mempool,
                                         mmu::mem_area::main, object);
        {
            auto pool = memory::pool::from_object(object);
            if (pool)
                return pool->get_size();
            else
                return memory::early_object_size(object);
        }
    case mmu::mem_area::page:
        return memory::page_size;
    case mmu::mem_area::debug:
        return dbg::object_size(object);
    default:
        abort();
    }
}

static inline void* std_realloc(void* object, size_t size)
{
    if (!object)
        return malloc(size);
    if (!size) {
        free(object);
        return nullptr;
    }

    size_t old_size = object_size(object);
    size_t copy_size = size > old_size ? old_size : size;
    void* ptr = malloc(size);
    if (ptr) {
        memcpy(ptr, object, copy_size);
        free(object);
    }

    return ptr;
}

  
void free(void* object)
{
    trace_memory_free(object);
    if (!object) {
        return;
    }
#if CONF_memory_tracker
    memory::tracker_forget(object);
#endif

    if (!mmu::is_linear_mapped(object, 0)) {
        return memory::mapped_free_large(object);
    }

    switch (mmu::get_mem_area(object)) {
    case mmu::mem_area::page:
        object = mmu::translate_mem_area(mmu::mem_area::page,
                                         mmu::mem_area::main, object);
        return memory::free_page(object);
    case mmu::mem_area::main:
         return memory::free_large(object);
    case mmu::mem_area::mempool:
        object = mmu::translate_mem_area(mmu::mem_area::mempool,
                                         mmu::mem_area::main, object);
        {
            auto pool = memory::pool::from_object(object);
            if (pool)
                return pool->free(object);
            else
                return memory::early_free_object(object);
        }
    case mmu::mem_area::debug:
        return dbg::free(object);
    default:
        abort();
    }
}

namespace dbg {

// debug allocator - give each allocation a new virtual range, so that
// any use-after-free will fault.

bool enabled;

using mmu::debug_base;
// FIXME: we assume the debug memory space is infinite (which it nearly is)
// and don't reuse space
std::atomic<char*> free_area{debug_base};
struct header {
    explicit header(size_t sz) : size(sz), size2(sz) {
        memset(fence, '$', sizeof fence);
    }
    ~header() {
        assert(size == size2);
        assert(std::all_of(fence, std::end(fence), [=](char c) { return c == '$'; }));
    }
    size_t size;
    char fence[16];
    size_t size2;
};
static const size_t pad_before = 2 * memory::page_size;
static const size_t pad_after = memory::page_size;

static __thread bool recursed;

void* malloc(size_t size, size_t alignment)
{
    if (!enabled || recursed) {
        return std_malloc(size, alignment);
    }

    recursed = true;
    auto unrecurse = defer([&] { recursed = false; });

    WITH_LOCK(memory::free_page_ranges_lock) {
        memory::reclaimer_thread.wait_for_minimum_memory();
    }

    // There will be multiple allocations needed to satisfy this allocation; request
    // access to the emergency pool to avoid us holding some lock and then waiting
    // in an internal allocation
    WITH_LOCK(memory::reclaimer_lock) {
        auto asize = align_up(size, memory::page_size);
        auto padded_size = pad_before + asize + pad_after;
        if (alignment > memory::page_size) {
            // Our allocations are page-aligned - might need more
            padded_size += alignment - memory::page_size;
        }
        char* v = free_area.fetch_add(padded_size, std::memory_order_relaxed);
        // change v so that (v + pad_before) is aligned.
        v += align_up(v + pad_before, alignment) - (v + pad_before);
        mmu::vpopulate(v, memory::page_size);
        new (v) header(size);
        v += pad_before;
        mmu::vpopulate(v, asize);
        memset(v + size, '$', asize - size);
        // fill the memory with garbage, to catch use-before-init
        uint8_t garbage = 3;
        std::generate_n(v, size, [&] { return garbage++; });
        return v;
    }
}

void free(void* v)
{
    assert(false);
    assert(!recursed);
    recursed = true;
    auto unrecurse = defer([&] { recursed = false; });
    WITH_LOCK(memory::reclaimer_lock) {
        auto h = static_cast<header*>(v - pad_before);
        auto size = h->size;
        auto asize = align_up(size, memory::page_size);
        char* vv = reinterpret_cast<char*>(v);
        assert(std::all_of(vv + size, vv + asize, [=](char c) { return c == '$'; }));
        h->~header();
        mmu::vdepopulate(h, memory::page_size);
        mmu::vdepopulate(v, asize);
        mmu::vcleanup(h, pad_before + asize);
    }
}

static inline size_t object_size(void* v)
{
    return static_cast<header*>(v - pad_before)->size;
}

}

void* malloc(size_t size)
{
    static_assert(alignof(max_align_t) >= 2 * sizeof(size_t),
                  "alignof(max_align_t) smaller than glibc alignment guarantee");
    auto alignment = alignof(max_align_t);
    if (alignment > size) {
        alignment = 1ul << ilog2_roundup(size);
    }
#if CONF_memory_debug == 0
    void* buf = std_malloc(size, alignment);
#else
    void* buf = dbg::malloc(size, alignment);
#endif

    trace_memory_malloc(buf, size, alignment);
    return buf;
}

OSV_LIBC_API
void* realloc(void* obj, size_t size)
{
    void* buf = std_realloc(obj, size);
    trace_memory_realloc(obj, size, buf);
    return buf;
}

extern "C" OSV_LIBC_API
void *reallocarray(void *ptr, size_t nmemb, size_t elem_size)
{
    size_t bytes;
    if (__builtin_mul_overflow(nmemb, elem_size, &bytes)) {
        errno = ENOMEM;
        return 0;
    }
    return realloc(ptr, nmemb * elem_size);
}

OSV_LIBC_API
size_t malloc_usable_size(void* obj)
{
    if ( obj == nullptr ) {
        return 0;
    }
    return object_size(obj);
}

// We dont have a heap
OSV_LIBC_API
int malloc_trim(size_t pad){
  return 0;
}

// posix_memalign() and C11's aligned_alloc() return an aligned memory block
// that can be freed with an ordinary free().

int posix_memalign(void **memptr, size_t alignment, size_t size)
{
    // posix_memalign() but not aligned_alloc() adds an additional requirement
    // that alignment is a multiple of sizeof(void*). We don't verify this
    // requirement, and rather always return memory which is aligned at least
    // to sizeof(void*), even if lesser alignment is requested.
    if (!is_power_of_two(alignment)) {
        return EINVAL;
    }
#if CONF_memory_debug == 0
    void* ret = std_malloc(size, alignment);
#else
    void* ret = dbg::malloc(size, alignment);
#endif
    trace_memory_malloc(ret, size, alignment);
    if (!ret) {
        return ENOMEM;
    }
    // Until we have a full implementation, just croak if we didn't get
    // the desired alignment.
    assert (!(reinterpret_cast<uintptr_t>(ret) & (alignment - 1)));
    *memptr = ret;
    return 0;

}

void *aligned_alloc(size_t alignment, size_t size)
{
    void *ret;
    int error = posix_memalign(&ret, alignment, size);
    if (error) {
        errno = error;
        return NULL;
    }
    return ret;
}

// memalign() is an older variant of aligned_alloc(), which does not require
// that size be a multiple of alignment.
// memalign() is considered to be an obsolete SunOS-ism, but Linux's glibc
// supports it, and some applications still use it.
OSV_LIBC_API
void *memalign(size_t alignment, size_t size)
{
    return aligned_alloc(alignment, size);
}

namespace memory {

void enable_debug_allocator()
{
    dbg::enabled = true;
}

void* alloc_phys_contiguous_aligned(size_t size, size_t align, bool block)
{
    assert(is_power_of_two(align));
    // make use of the standard large allocator returning properly aligned
    // physically contiguous memory:

    void *ret;
    if(size <= page_size && align <= page_size){
        ret = mmu::translate_mem_area(mmu::mem_area::main, mmu::mem_area::page, alloc_page());
    }
    else {
        ret = malloc_large(size, align, block, true);
    }

    assert (!(reinterpret_cast<uintptr_t>(ret) & (align - 1)));
    return ret;
}

void free_phys_contiguous_aligned(void* p)
{
    free(p);
}

bool throttling_needed()
{
#if CONF_memory_jvm_balloon
    if (!balloon_api) {
        return false;
    }

    return balloon_api->ballooning();
#else
    return false;
#endif
}

#if CONF_memory_jvm_balloon
jvm_balloon_api *balloon_api = nullptr;
#endif
}

extern "C" void* alloc_contiguous_aligned(size_t size, size_t align)
{
    return memory::alloc_phys_contiguous_aligned(size, align, true);
}

extern "C" void free_contiguous_aligned(void* p)
{
    memory::free_phys_contiguous_aligned(p);
}

namespace ucache {
    void* frames_alloc(unsigned order){ return memory::llfree_allocator.alloc_huge_page(order); }
    u64 frames_alloc_phys_addr(size_t size) { return memory::llfree_allocator.alloc_page_phys_addr(memory::llfree_allocator.order(size)); }
    void free_frames(void* addr, unsigned order){ memory::llfree_allocator.free_page(addr, order); }
    void frames_free_phys_addr(u64 idx, size_t size){ memory::llfree_allocator.free_page_phys_addr(idx, memory::llfree_allocator.order(size)); }

    u64 stat_free_phys_mem() { return memory::llfree_allocator.free_memory(); }
    u64 stat_total_phys_mem() { return memory::total_memory.load(); }
}
