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
#include "ds/atomic_singly_linked_list.hpp"
#include <stdint.h>

namespace common {
// TODO generalize for range predicates
struct Predicate {
  uint64_t k;

  bool operator==(uint64_t k2) { return k == k2; }
};

template <typename Allocator = ChunkAllocator>
class OptimisticPredicateLocking {
 private:
  std::atomic<uint64_t> lock_;
  atom::AtomicSinglyLinkedList<Predicate> predicate_list_;

  static thread_local std::vector<std::pair<atom::AtomicSinglyLinkedList<Predicate>*, uint64_t>> predicate_removal_;

 public:
  OptimisticPredicateLocking(Allocator* alloc, atom::EpochManagerBase<Allocator>* emb)
      : lock_(0), predicate_list_(alloc, emb) {}

  static inline void finishTransaction() {
    for (auto& p : predicate_removal_) {
      p.first->erase(p.second);
    }
    predicate_removal_.clear();
  }

  template <typename Key, typename Row, typename Index, typename Function>
  inline bool lookup(Key& k, Row& r, Index& i, Function f) {
    bool b = f(i, r, k);
    /* // comment out for comparable results with other cc only databases
    if (!b) {
      lock_lookup();
      b = f(i, r, k);
      if (!b) {
        auto pos = predicate_list_.push_front(Predicate{std::hash<Key>{}(k)});
        predicate_removal_.emplace_back(std::make_pair(&predicate_list_, pos));
      }
      unlock_lookup();
    }*/

    return b;
  }

  template <typename Key, typename Row, typename Index, typename Function>
  inline bool insert(Key& k, Row& r, Index& i, Function f) {
    lock_insert();
    auto it = predicate_list_.begin();
    auto end = predicate_list_.end();

    auto allowed = true;

    while (it != end) {
      if (*it == std::hash<Key>{}(k)) {
        allowed = false;
        break;
      }
      ++it;
    }

    if (allowed) {
      allowed = f(i, r, k);
    }

    unlock_insert();
    return allowed;
  }

 private:
  inline void lock_insert() {
    auto xchg = false;
    auto l = lock_.load();
    while (!xchg) {
      auto n = (1ull << 63);
      if (l >= n) {
        n = l + 1;
      }
      if (l == 0 || l >= (1ull << 63)) {
        xchg = lock_.compare_exchange_weak(l, n);
      }
      l = lock_;
    }
  }

  inline void unlock_insert() {
    auto l = lock_.load();
    assert(l >= (1ull << 63));
    auto xchg = false;
    while (!xchg) {
      auto n = (l == (1ull << 63)) ? 0 : l - 1;
      xchg = lock_.compare_exchange_weak(l, n);
    }
  }

  inline void lock_lookup() {
    auto xchg = false;
    auto l = lock_.load();
    while (!xchg) {
      if (l < (1ull << 63)) {
        xchg = lock_.compare_exchange_weak(l, (l + 1));
      }
      l = lock_;
    }
  }

  inline void unlock_lookup() {
    auto l = lock_.load();
    assert(l < (1ull << 63));
    auto xchg = false;
    while (!xchg) {
      xchg = lock_.compare_exchange_weak(l, (l - 1));
      l = lock_;
    }
  }
};
};  // namespace common
