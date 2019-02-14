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

#pragma once
#include "common/chunk_allocator.hpp"
#include "common/epoch_manager.hpp"
#include "ds/atomic_singly_linked_list.hpp"
#include "ds/atomic_unordered_map.hpp"

namespace step {
namespace serial {
class SerializationGraph;
struct Node;
/*
 * StepManager needs to have a wait count lower equals the hardware concurrency
 */
class StepManager {
  using Allocator = common::ChunkAllocator;
  using EMB = atom::EpochManagerBase<Allocator>;

 private:
  atom::AtomicUnorderedMap<uint64_t,
                           uint64_t,
                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator,
                           false>
      map_counter_;
  static thread_local uint64_t hashKey;
  SerializationGraph* sgt_;
  uint64_t wait_count_ = std::thread::hardware_concurrency();
  std::atomic<uint64_t> ctr_;

 public:
  StepManager(SerializationGraph* sgt, Allocator* alloc, EMB* em)
      : map_counter_(std::thread::hardware_concurrency(), alloc, em), sgt_(sgt), ctr_(0) {}

  inline uint64_t fetchAddCtr() {
    uint64_t id = pthread_self();
    if (hashKey == 0) {
      hashKey = map_counter_.hashKey(id);
    }
    uint64_t ctr = ctr_.fetch_add(1);
    map_counter_.replace(id, ctr, hashKey);
    return ctr;
  }

  inline void fetchReplace() {
    uint64_t id = pthread_self();
    if (hashKey == 0) {
      hashKey = map_counter_.hashKey(id);
    }
    map_counter_.replace(id, ctr_.fetch_add(1), hashKey);
  }

  inline bool isSaveRead(uint64_t ctr, uint64_t attempts) {
    //  if (attempts % 10 == 0)
    fetchReplace();
    //  if (attempts % 100 == 0)
    //    std::this_thread::yield();
    auto it = map_counter_.begin();
    const auto end = map_counter_.end();
    for (; it != end; ++it) {
      if (*it < ctr) {
        return false;
      }
    }
    return true;
  }

  inline void addPostCtr(uint64_t ctr) {
    uint64_t id = pthread_self();
    map_counter_.replace(id, std::numeric_limits<uint64_t>::max());
  }

  inline void waitSaveRead(uint64_t ctr) {
    uint64_t waiter = 0;
    while (!isSaveRead(ctr, waiter)) {
      waiter++;
    }
  }
  inline uint64_t currentCtr() { return ctr_; }
};

class StepGuard {
  StepManager& sm_;
  uint64_t ctr_;
  bool waiter_;
  bool not_alive_;

 public:
  inline StepGuard(StepManager& sm) : sm_(sm), waiter_(false), not_alive_(false) { ctr_ = sm_.fetchAddCtr(); }

  inline uint64_t getCtr() const { return ctr_; }

  inline void waitSaveRead() {
    waiter_ = true;
    sm_.waitSaveRead(ctr_);
  }

  inline void destroy() {
    not_alive_ = true;
    sm_.addPostCtr(ctr_);
  }

  inline ~StepGuard() {
    if (!not_alive_)
      destroy();
  }
};
};  // namespace serial
};  // namespace step
