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
#include "common/shared_spin_mutex.hpp"
#include "ds/atomic_extent_vector.hpp"
#include <chrono>
#include <random>
#include <set>
#include <thread>
#include <unordered_set>

namespace common {
class MutexWaitManager {
  // atom::AtomicExtentVector<uint64_t> locks_;
  SharedSpinMutex* locks_;
  uint64_t thread_number_;

 public:
  MutexWaitManager(uint64_t thread_number) : locks_(), thread_number_(thread_number) {
    locks_ = new SharedSpinMutex[thread_number_]();
  };

  void wait(uint64_t transaction, const std::unordered_set<uint64_t> transaction_problem) {
    std::set<uint64_t> oset{};
    for (auto t : transaction_problem) {
      oset.emplace(calculateOffset(t));
    }
    oset.emplace(calculateOffset(transaction));

    for (auto t : oset) {
      locks_[t].lock();
    }
  }

  void release(uint64_t transaction, const std::unordered_set<uint64_t> transaction_problem) {
    std::set<uint64_t> oset{};
    for (auto t : transaction_problem) {
      oset.emplace(calculateOffset(t));
    }
    oset.emplace(calculateOffset(transaction));

    for (auto t : oset) {
      locks_[t].unlock();
    }
  }

  static inline constexpr uint64_t hashKey(uint64_t k) {
    constexpr uint64_t m = 0xc6a4a7935bd1e995;
    constexpr int r = 47;
    uint64_t h = 0x8445d61a4e774912 ^ (8 * m);
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h | (1ull << 63);
  }

  const inline uint64_t calculateOffset(uint64_t transaction) const { return hashKey(transaction) % thread_number_; }
};
};  // namespace common
