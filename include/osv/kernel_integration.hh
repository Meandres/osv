/*
 * Copyright (C) 2013 Cloudius Systems, Ltd.
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

#ifndef KII_HH_
#define KII_HH_

#include "osv/mmu.hh"
#include "osv/rwlock.h"

namespace kii {

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
  uintptr_t reserve_range(const u64 size);

  /// Free the given range by returning virtual memory back to OSv
  void free_range(const uintptr_t addr, const u64 size);

  /// [ THREADSAFE ]
  /// Allocate physically contiguous memory of the specified order
  /// Returns a virtual address from the linear mapping
  void* frames_alloc(unsigned order);

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
}

#endif /* KII_HH_ */
