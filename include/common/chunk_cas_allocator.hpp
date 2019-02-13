//
// No False Negatives Database - A prototype database to test concurrency control that scales to many cores.
// Copyright (C) 2019 Dominik Durner <dominik.durner@tum.de>
//
// This file is part of No False Negatives Database.
//
// No False Negatives Database is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// No False Negatives Database is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with No False Negatives Database.  If not, see <http://www.gnu.org/licenses/>.
//

#pragma once
#include "common/epoch_manager.hpp"
#include "ds/atomic_data_structures_decl.hpp"
#include "ds/atomic_extent_vector.hpp"
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <iostream>
#include <vector>
#include <tbb/spin_mutex.h>

namespace common {
template <typename T>
struct atomwrapper {
  std::atomic<T> _a;

  atomwrapper() : _a() {}

  atomwrapper(const std::atomic<T>& a) : _a(a.load()) {}

  atomwrapper(const atomwrapper& other) : _a(other._a.load()) {}

  atomwrapper& operator=(const atomwrapper& other) { _a.store(other._a.load()); }
};

class ChunkCASAllocator {  //: std::allocator_traits<common::ChunkAllocator<T, concurrent_threads, page_size>> {
 private:
  static constexpr uint8_t alignment_ = 8;
  static constexpr unsigned int bits_page_ = 20;
  static constexpr uint64_t page_size_ = 1 << bits_page_;
  static constexpr uint8_t bits_chunks_ = 64 - bits_page_;
  atom::AtomicExtentVector<atomwrapper<uint64_t>*> delete_count_;
  atom::AtomicExtentVector<atomwrapper<bool>*> finished_;
  atom::AtomicExtentVector<void*> chunks_;
  atom::AtomicUnorderedMap<uint64_t,
                           uint64_t,
                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::StdAllocator,
                           false>* vptr_map_;
  std::atomic<uint64_t> chunk_ptr_;
  std::atomic<uint64_t> size_;
  tbb::spin_mutex mut;
  static thread_local uint64_t chunk_ptr_local;
  common::StdAllocator alloc_;
  atom::EpochManagerBase<common::StdAllocator> emb_;

  ChunkCASAllocator(const ChunkCASAllocator& other) = delete;
  ChunkCASAllocator(ChunkCASAllocator&& other) = delete;
  ChunkCASAllocator& operator=(const ChunkCASAllocator& other) = delete;
  ChunkCASAllocator& operator=(ChunkCASAllocator&& other) = delete;

  bool add();
  void remove(void* p);

 public:
  ChunkCASAllocator(uint64_t chunk_count);
  ~ChunkCASAllocator();
  void printDetails();

  template <typename T>
  T* allocate(std::size_t n) {
    assert(n == 1);
    // std::cout << alignof(T) << std::endl;
    assert(alignof(T) <= alignment_);

    // return reinterpret_cast<T*>(malloc(sizeof(T)));

    constexpr uint64_t pad = (~(sizeof(T) + alignment_) + 1) & (alignment_ - 1);
    constexpr uint64_t size = sizeof(T) + 8 + pad;
    static_assert(pad < alignment_, "Alignment wrong!");
    static_assert(size <= page_size_, "Element larger than Page!");
    constexpr uint64_t mask_page = (1 << bits_page_) - 1;
    constexpr uint64_t mask_chunks = ~mask_page;
    uint64_t chunk_ptr_old, chunk_ptr_new;

    if (chunk_ptr_local == 0 || ((chunk_ptr_local + size - 1) & mask_chunks) > ((chunk_ptr_local - 1) & mask_chunks)) {
      do {
        chunk_ptr_old = chunk_ptr_.load();

        if (((chunk_ptr_old + page_size_ - 1) & mask_chunks) > ((chunk_ptr_old - 1) & mask_chunks)) {
          // current page space is to small to insert element of type T
          if ((chunk_ptr_old & mask_page) != 0) {
            // current page is not filled completley hence space left
            // therefore we need to fill up the counter first otherwise data would not lie on one page!
            chunk_ptr_new = chunk_ptr_old + page_size_ + (page_size_ - (chunk_ptr_old & mask_page));
          } else {
            chunk_ptr_new = chunk_ptr_old + page_size_;
          }

        } else {
          chunk_ptr_new = chunk_ptr_old + page_size_;
        }
      } while (!chunk_ptr_.compare_exchange_weak(chunk_ptr_old, chunk_ptr_new));

      uint64_t ctr = 0;
      while ((chunk_ptr_new - 1) >> bits_page_ >= size_) {
        ctr += add() ? 1 : 0;
      }

      // new page has been added
      if (chunk_ptr_local > 0)
        finished_[(chunk_ptr_local - 1) >> bits_page_]->_a = true;

      chunk_ptr_local = chunk_ptr_new - page_size_;
    }
    chunk_ptr_local = chunk_ptr_local + size;

    delete_count_[(chunk_ptr_local - 1) >> bits_page_]->_a.fetch_add(1);

    void* chunk = chunks_[(chunk_ptr_local - 1) >> bits_page_];
    uintptr_t addr_ptr = reinterpret_cast<uintptr_t>(chunk);
    addr_ptr += ((chunk_ptr_local - size + pad + 8) & mask_page);

    /*    Debug Infos
    //std::cout << sizeof(T) << std::endl;
    //std::cout << page_size_ << std::endl;
    //std::cout << chunk_ptr << std::endl;
    //std::cout << (chunk_ptr >> bits_page_) << std::endl;
    std::cout << "Size:\t" << size << std::endl;
    std::cout << "Page:\t" << (chunk_ptr >> bits_page_) << "\t | Max: " << size_[current_thread] << std::endl;
    std::cout << "Offset:\t" << ((chunk_ptr << bits_chunks_) >> bits_chunks_) - size << std::endl;
    std::cout << "void*:\t" << chunk << std::endl;
    std::cout << "uint*:\t" << addr_ptr << std::endl << std::endl;*/

    uint64_t* vptr = reinterpret_cast<uint64_t*>(reinterpret_cast<void*>(addr_ptr - 8));
    *vptr = reinterpret_cast<uintptr_t>(chunk);

    // assert(chunk_ptr_new - chunk_ptr_old - 8 - pad < page_size_);
    // size_count_[(chunk_ptr_new - 1) >> bits_page_]->_a.fetch_add(chunk_ptr_new - chunk_ptr_old);

    return reinterpret_cast<T*>(reinterpret_cast<void*>(addr_ptr));
  }

  inline void deallocate(void* p, std::size_t n) {
    assert(n == 1);
    // free(p);
    remove(p);
  }

  template <typename T>
  inline void deallocate(T* p, std::size_t n) {
    assert(n == 1);
    p->~T();
    // free(p);
    remove(p);
  }
};
};  // namespace common
