/*
 * Copyright (C) 2013 Cloudius Systems, Ltd.
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

#ifndef MMU_HH
#define MMU_HH

#include "fs/fs.hh"
#include <stdint.h>
#include <boost/intrusive/set.hpp>
#include <osv/types.h>
#include <functional>
#include <osv/error.h>
#include <osv/addr_range.hh>
#include <unordered_map>
#include <memory>
#include <osv/mmu-defs.hh>
#include <osv/align.hh>
#include <osv/trace.hh>
#include <osv/rwlock.h>
#include <set>
#include <osv/kernel_config_memory_debug.h>
#include <osv/kernel_config_memory_jvm_balloon.h>

struct exception_frame;
#if CONF_memory_jvm_balloon
class balloon;
typedef std::shared_ptr<balloon> balloon_ptr;
#endif

/**
 * MMU namespace
 */
namespace mmu {

// when we know it was dynamically allocated
inline phys virt_to_phys_dynamic_phys(void* virt)
{
    return static_cast<char*>(virt) - phys_mem;
}

constexpr inline unsigned pt_index(void *virt, unsigned level)
{
    return (reinterpret_cast<ulong>(virt) >> (page_size_shift + level * pte_per_page_shift)) & (pte_per_page - 1);
}

struct page_allocator;

struct linear_vma {
    void* _virt_addr;
    phys _phys_addr;
    size_t _size;
    mattr _mem_attr;
    std::string _name;

    linear_vma(void* virt, phys phys, size_t size, mattr mem_attr, const char* name);
    ~linear_vma();

    uintptr_t v_start() const { return reinterpret_cast<uintptr_t>(_virt_addr); }
    uintptr_t v_end() const { return reinterpret_cast<uintptr_t>(_virt_addr) + _size; }
};

class vma {
public:
    vma(addr_range range, unsigned perm, unsigned flags, bool map_dirty, page_allocator *page_ops = nullptr, u64 id=0);
    virtual ~vma();
    void set(uintptr_t start, uintptr_t end);
    void protect(unsigned perm);
    uintptr_t start() const;
    uintptr_t end() const;
    void* addr() const;
    uintptr_t size() const;
    unsigned perm() const;
    unsigned flags() const;
    u64 id() const;
    virtual void fault(uintptr_t addr, exception_frame *ef);
    virtual void split(uintptr_t edge) = 0;
    virtual error sync(uintptr_t start, uintptr_t end) = 0;
    virtual int validate_perm(unsigned perm) { return 0; }
    virtual page_allocator* page_ops();
    void update_flags(unsigned flag);
    bool has_flags(unsigned flag);
    template<typename T> ulong operate_range(T mapper, void *start, size_t size);
    template<typename T> ulong operate_range(T mapper);
    bool map_dirty();
    class addr_compare;
protected:
    addr_range _range;
    unsigned _perm;
    unsigned _flags;
    bool _map_dirty;
    page_allocator *_page_ops;
    u64 _id;
public:
    boost::intrusive::set_member_hook<> _vma_list_hook;
};

struct vma_range {
    const void* _vma;
    bool _is_linear;

    vma_range(const linear_vma* v) {
       _vma = v;
       _is_linear = true;
    }

    vma_range(const vma* v) {
       _vma = v;
       _is_linear = false;
    }

    uintptr_t start() const {
       if (_is_linear) {
          return static_cast<const linear_vma*>(_vma)->v_start();
       } else {
          return static_cast<const vma*>(_vma)->start();
       }
    }

    uintptr_t end() const {
       if (_is_linear) {
          return static_cast<const linear_vma*>(_vma)->v_end();
       } else {
          return static_cast<const vma*>(_vma)->end();
       }
    }
};

class anon_vma : public vma {
public:
    anon_vma(addr_range range, unsigned perm, unsigned flags);
    anon_vma(addr_range range, unsigned perm, unsigned flags, uint64_t id);
    virtual void split(uintptr_t edge) override;
    virtual error sync(uintptr_t start, uintptr_t end) override;
};

class file_vma : public vma {
public:
    file_vma(addr_range range, unsigned perm, unsigned flags, fileref file, f_offset offset, page_allocator *page_ops);
    ~file_vma();
    virtual void split(uintptr_t edge) override;
    virtual error sync(uintptr_t start, uintptr_t end) override;
    virtual int validate_perm(unsigned perm);
    virtual void fault(uintptr_t addr, exception_frame *ef) override;
    fileref file() const { return _file; }
    f_offset offset() const { return _offset; }
    u64 file_inode() const { return _file_inode; }
    dev_t file_dev_id() const { return _file_dev_id; }
private:
    f_offset offset(uintptr_t addr);
    fileref _file;
    f_offset _offset;
    u64 _file_inode;
    dev_t _file_dev_id;
};

#if CONF_memory_jvm_balloon
ulong map_jvm(unsigned char* addr, size_t size, size_t align, balloon_ptr b);

class jvm_balloon_vma : public vma {
public:
    jvm_balloon_vma(unsigned char *jvm_addr, uintptr_t start, uintptr_t end, balloon_ptr b, unsigned perm, unsigned flags);
    virtual ~jvm_balloon_vma();
    virtual void split(uintptr_t edge) override;
    virtual error sync(uintptr_t start, uintptr_t end) override;
    virtual void fault(uintptr_t addr, exception_frame *ef) override;
    void detach_balloon();
    unsigned char *jvm_addr() { return _jvm_addr; }
    unsigned char *effective_jvm_addr() { return _effective_jvm_addr; }
    bool add_partial(size_t partial, unsigned char *eff);
    size_t partial() { return _partial_copy; }
    // Iff we have a partial, the size may be temporarily changed. We keep it in a different
    // variable so don't risk breaking any mmu core code that relies on the derived size()
    // being the same.
    uintptr_t real_size() const { return _real_size; }
    friend ulong map_jvm(unsigned char* jvm_addr, size_t size, size_t align, balloon_ptr b);
protected:
    balloon_ptr _balloon;
    unsigned char *_jvm_addr;
private:
    unsigned char *_effective_jvm_addr = nullptr;
    uintptr_t _partial_addr = 0;
    anon_vma *_partial_vma = nullptr;
    size_t _partial_copy = 0;
    unsigned _real_perm;
    unsigned _real_flags;
    uintptr_t _real_size;
};
#endif

class shm_file final : public special_file {
    size_t _size;
    std::unordered_map<uintptr_t, void*> _pages;
    void* page(uintptr_t hp_off);
public:
    shm_file(size_t size, int flags);
    virtual int stat(struct stat* buf) override;
    virtual int close() override;
    virtual std::unique_ptr<file_vma> mmap(addr_range range, unsigned flags, unsigned perm, off_t offset) override;

    virtual bool map_page(uintptr_t offset, hw_ptep<0> ptep, pt_element<0> pte, bool write, bool shared) override;
    virtual bool map_page(uintptr_t offset, hw_ptep<1> ptep, pt_element<1> pte, bool write, bool shared) override;
    virtual bool put_page(void *addr, uintptr_t offset, hw_ptep<0> ptep) override;
    virtual bool put_page(void *addr, uintptr_t offset, hw_ptep<1> ptep) override;
};

size_t vma_size(const void* addr);

void* map_file(const void* addr, size_t size, unsigned flags, unsigned perm,
              fileref file, f_offset offset);
void* map_anon(const void* addr, size_t size, unsigned flags, unsigned perm);

error munmap(const void* addr, size_t size);
error munmap_vma(const void* addr);
error mprotect(const void *addr, size_t size, unsigned int perm);
error msync(const void* addr, size_t length, int flags);
error mincore(const void *addr, size_t length, unsigned char *vec);
bool is_linear_mapped(const void *addr, size_t size);
bool ismapped(const void *addr, size_t size);
bool isreadable(void *addr, size_t size);
std::unique_ptr<file_vma> default_file_mmap(file* file, addr_range range, unsigned flags, unsigned perm, off_t offset);
std::unique_ptr<file_vma> map_file_mmap(file* file, addr_range range, unsigned flags, unsigned perm, off_t offset);


template<int N>
inline bool pte_is_cow(pt_element<N> pte)
{
    return false;
}

template<>
inline bool pte_is_cow(pt_element<0> pte)
{
    return pte.sw_bit(pte_cow); // only 4k pages can be cow for now
}

static TRACEPOINT(trace_clear_pte, "ptep=%p, cow=%d, pte=%x", void*, bool, uint64_t);

template<int N>
__attribute__((always_inline)) // Necessary because of issue #1029
inline pt_element<N> clear_pte(hw_ptep<N> ptep)
{
    auto old = ptep.exchange(make_empty_pte<N>());
    trace_clear_pte(ptep.release(), pte_is_cow(old), old.addr());
    return old;
}

template<int N>
inline bool clear_accessed(hw_ptep<N> ptep)
{
    pt_element<N> pte = ptep.read();
    bool accessed = pte.accessed();
    if (accessed) {
        pt_element<N> clear = pte;
        clear.set_accessed(false);
        ptep.compare_exchange(pte, clear);
    }
    return accessed;
}

template<int N>
inline bool clear_dirty(hw_ptep<N> ptep)
{
    static_assert(pt_level_traits<N>::leaf_capable::value, "non leaf pte");
    pt_element<N> pte = ptep.read();
    bool dirty = pte.dirty();
    if (dirty) {
        pt_element<N> clear = pte;
        clear.set_dirty(false);
        ptep.compare_exchange(pte, clear);
    }
    return dirty;
}

template<int N>
inline pt_element<N> make_intermediate_pte(hw_ptep<N> ptep, phys addr)
{
    static_assert(pt_level_traits<N>::intermediate_capable::value, "level 0 pte cannot be intermediate");
    return make_pte<N>(addr, false);
}

template<int N>
inline pt_element<N> make_leaf_pte(hw_ptep<N> ptep, phys addr,
                                   unsigned perm = perm_rwx,
                                   mattr mem_attr = mattr_default)
{   
    static_assert(pt_level_traits<N>::leaf_capable::value, "non leaf pte");
    return make_pte<N>(addr, true, perm, mem_attr);
}

class virt_pte_visitor {
public:
    virtual void pte(pt_element<0>) = 0;
    virtual void pte(pt_element<1>) = 0;
};

void virt_visit_pte_rcu(uintptr_t virt, virt_pte_visitor& visitor);

template<int N>
inline bool write_pte(void *addr, hw_ptep<N> ptep, pt_element<N> old_pte, pt_element<N> new_pte)
{
    new_pte.mod_addr(virt_to_phys(addr));
    return ptep.compare_exchange(old_pte, new_pte);
}

template<int N>
inline bool write_pte(void *addr, hw_ptep<N> ptep, pt_element<N> pte)
{
    pte.mod_addr(virt_to_phys(addr));
    return ptep.compare_exchange(ptep.read(), pte);
}

pt_element<0> pte_mark_cow(pt_element<0> pte, bool cow);

template <typename OutputFunc>
inline
void virt_to_phys(void* vaddr, size_t len, OutputFunc out)
{
    if (CONF_memory_debug && vaddr >= debug_base) {
        while (len) {
            auto next = std::min(align_down(vaddr + page_size, page_size), vaddr + len);
            size_t delta = static_cast<char*>(next) - static_cast<char*>(vaddr);
            out(virt_to_phys(vaddr), delta);
            vaddr = next;
            len -= delta;
        }
    } else {
        out(virt_to_phys(vaddr), len);
    }
}

void* phys_to_virt(phys pa);

template <typename T>
T* phys_cast(phys pa)
{
    return static_cast<T*>(phys_to_virt(pa));
}

inline
bool is_page_aligned(intptr_t addr)
{
    return !(addr & (page_size-1));
}

inline
bool is_page_aligned(void* addr)
{
    return is_page_aligned(reinterpret_cast<intptr_t>(addr));
}

// The mattr type is defined differently for each architecture
// and interpreted by the architecture-specific code, and has
// an architecture-specific meaning.
// Currently mem_attr is ignored on x86_64. For aarch64 specifics see
// definitions in arch/aarch64/arch-mmu.hh
void linear_map(void* virt, phys addr, size_t size, const char* name,
                size_t slop = mmu::page_size,
                mattr mem_attr = mmu::mattr_default);

void free_initial_memory_range(uintptr_t addr, size_t size);
void switch_to_runtime_page_tables();

void set_nr_page_sizes(unsigned nr);

void vpopulate(void* addr, size_t size);
void vdepopulate(void* addr, size_t size);
void vcleanup(void* addr, size_t size);

error  advise(void* addr, size_t size, int advice);

void vm_fault(uintptr_t addr, exception_frame* ef);

std::string procfs_maps();
std::string sysfs_linear_maps();

unsigned long all_vmas_size();

// Synchronize cpu data and instruction caches for specified area of virtual memory
void synchronize_cpu_caches(void *v, size_t size);

namespace bi = boost::intrusive;

struct vma_range_compare {
    bool operator()(const vma_range& a, const vma_range& b) const {
        return a.start() < b.start();
    }
};

extern rwlock_t vma_range_set_mutex;
extern std::set<vma_range, vma_range_compare> vma_range_set;

class vma_compare {
public:
    bool operator ()(const vma& a, const vma& b) const {
        return a.addr() < b.addr();
    }
};

constexpr uintptr_t lower_vma_limit = 0x0;
constexpr uintptr_t upper_vma_limit = 0x400000000000;

typedef boost::intrusive::set<vma,
                              bi::compare<vma_compare>,
                              bi::member_hook<vma,
                                              bi::set_member_hook<>,
                                              &vma::_vma_list_hook>,
                              bi::optimize_size<true>
                              > vma_list_base;

struct vma_list_type : vma_list_base {
    vma_list_type() {
        // insert markers for the edges of allocatable area
        // simplifies searches
        auto lower_edge = new anon_vma(addr_range(lower_vma_limit, lower_vma_limit), 0, 0);
        insert(*lower_edge);
        auto upper_edge = new anon_vma(addr_range(upper_vma_limit, upper_vma_limit), 0, 0);
        insert(*upper_edge);

        WITH_LOCK(vma_range_set_mutex.for_write()) {
            vma_range_set.insert(vma_range(lower_edge));
            vma_range_set.insert(vma_range(upper_edge));
        }
    }
};

struct vm_range : public bi::set_base_hook<> {
    uintptr_t addr;
    uint64_t size;

    vm_range(uintptr_t addr, uint64_t size) : addr(addr), size(size) {}

    bool operator<(const vm_range& other) const {
        return addr < other.addr;
    }
};

struct vm_range_compare {
    bool operator()(const vm_range& a, const vm_range& b) const {
        return a.addr < b.addr;
    }
};

using vm_range_set = bi::set<vm_range, bi::compare<vm_range_compare>, bi::optimize_size<true>>;

struct vm_range_type : vm_range_set {
    vm_range_type() {

    }
};

struct superblock_bucket {
  vma_list_type vma_list;
  rwlock_t vma_list_mutex;
  vm_range_set free_ranges;
  rwlock_t free_ranges_mutex;
  char padding[48];
};

class superblock_manager {
    // 64 workers for superblock segments, 1 worker for outside segments
    std::array<superblock_bucket, sched::max_cpus + 1> workers;
    std::array<std::atomic_uint8_t, superblock_len> superblocks;

    uint8_t _free_idx{255};
    uint8_t _reserved_idx{254};

    uint8_t cpu_id();
    u64 superblock_index(const uintptr_t addr);
    inline u64 superblock_index(const void* addr){ return superblock_index(reinterpret_cast<uintptr_t>(addr)); }
    uintptr_t superblock_ptr(const u64 superblock);
    uint8_t owner(const uintptr_t addr);
    inline uint8_t owner(const void* addr){ return owner(reinterpret_cast<uintptr_t>(addr)); }
    inline uint8_t owner(const vma& v){ return owner(v.start()); }
    void release_superblocks(u64 start, unsigned n);
    u64 allocate_superblocks(unsigned n);

    // tries to allocate the superblock with index n. Returns once the superblock is allocated with the cpu id
    uint8_t allocate_superblock_at(u64 index, uint8_t core);

    // Returns the iterator with the largest key less or equal than addr.
    vm_range_set::iterator leq_range(vm_range addr, vm_range_set& fr);

  public:
    superblock_manager();

    // Get vma lock for manager managing intersecting superblock
    rwlock_t& vma_lock(const uintptr_t addr);

    inline rwlock_t& vma_lock(const void* addr){ return vma_lock(reinterpret_cast<uintptr_t>(addr)); }

    rwlock_t& free_ranges_lock(uintptr_t addr);
    vma_list_type::iterator vma_end_iterator(uintptr_t addr);

    vma_list_type::iterator find_intersecting_vma(const uintptr_t addr);

    std::pair<vma_list_type::iterator, vma_list_type::iterator>find_intersecting_vmas(addr_range r);

    void insert(vma* v);

    // Allocating the given range means removing it from the free_ranges map
    void allocate_range(uintptr_t addr, u64 size);

    // If this returns true, the entire map is guaranteed to be owned by the same bucket
    bool validate_map_fixed(uintptr_t start, u64 size);
    
    void erase(vma& v);
  
    // Frees the given range by adding it to the free range map
    void free_range(uintptr_t addr, u64 size, uint8_t owner);
    
    // Frees the given range by adding it to the free range map. Requires the addr to be owned
    void free_range(uintptr_t addr, u64 size);
    
    // Returns the pointer to a free range of the requested size.
    // This function is threadsafe and already allocates the memory
    // from free_ranges
    uintptr_t reserve_range(u64 size);
    
    u64 all_vmas_size();
    
    std::string procfs_maps();
};

extern superblock_manager* sb_mgr;
void initialize_superblocks();
}

#endif /* MMU_HH */
