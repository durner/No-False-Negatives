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
// SPDX-License-Identifier: GPL-3.0-or-later
//

#include "common/chunk_cas_allocator.hpp"
#include "ds/atomic_unordered_map.hpp"

namespace common {
thread_local uint64_t ChunkCASAllocator::chunk_ptr_local = 0;

bool ChunkCASAllocator::add() {
  mut.lock();
  if (((chunk_ptr_ - 1) >> bits_page_) < size_) {
    mut.unlock();
    return false;
  }
  void* chunk = aligned_alloc(alignment_, page_size_);
  uint64_t val = chunks_.push_back(chunk);
  delete_count_.push_back(new atomwrapper<uint64_t>(0));
  finished_.push_back(new atomwrapper<bool>(false));
  vptr_map_->insert(reinterpret_cast<uintptr_t>(chunk), val);
  size_++;
  mut.unlock();
  return true;
}

ChunkCASAllocator::ChunkCASAllocator(uint64_t chunk_count)
    : delete_count_(), finished_(), chunks_(), chunk_ptr_(0), size_(0), mut(), alloc_(), emb_(&alloc_) {
  vptr_map_ = new atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::StdAllocator, false>(chunk_count << 12, &alloc_, &emb_);
}

ChunkCASAllocator::~ChunkCASAllocator() {
  uint64_t i = 0;
  printDetails();
  for (auto chunk : chunks_) {
    if (delete_count_.isAlive(i)) {
      if (delete_count_[i]->_a != 0 || !finished_[i]->_a) {
        free(chunk);
      }
      delete delete_count_[i];
      delete finished_[i];
    }
    i++;
  }
  delete vptr_map_;
}

void ChunkCASAllocator::printDetails() {
  uint64_t i = 0;
  uint64_t k = 0;
  std::cout << "delete_count of chunks: {" << std::endl << "\t";
  while (i < chunks_.size()) {
    if (delete_count_.isAlive(i)) {
      if (delete_count_[i]->_a != 0 || !finished_[i]->_a) {
        k++;
        std::cout << "'" << i << "': '" << delete_count_[i]->_a << "', ";
      } else {
        std::cerr << "Leak:" << finished_[i]->_a << " and " << delete_count_[i]->_a << std::endl;
      }
    }
    i++;
  }
  std::cout << std::endl << "}" << std::endl << "Unfreed chunks: " << k << std::endl;
}

void ChunkCASAllocator::remove(void* p) {
  uintptr_t addr_ptr = reinterpret_cast<uintptr_t>(reinterpret_cast<void*>(p));

  uint64_t key = 0;
  memcpy(&key, reinterpret_cast<void*>(addr_ptr - 8), 8);
  uint64_t vector_offset;
  bool lookup = vptr_map_->lookup(key, vector_offset);

  assert(lookup);

  if (lookup && delete_count_[vector_offset]->_a.fetch_sub(1) == 1 && finished_[vector_offset]->_a) {
    vptr_map_->erase(key);
    delete finished_[vector_offset];
    finished_.erase(vector_offset);
    delete delete_count_[vector_offset];
    delete_count_.erase(vector_offset);
    free(chunks_[vector_offset]);
  }
}
};  // namespace common
