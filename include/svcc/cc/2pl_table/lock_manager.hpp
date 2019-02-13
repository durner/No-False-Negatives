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
#include "common/global_logger.hpp"
#include "ds/atomic_unordered_map.hpp"
#include <algorithm>
#include <set>
#include <tuple>
#include <unordered_set>
#include <tbb/spin_rw_mutex.h>
//#define LOGGER 1

namespace twopl_table {

template <typename Allocator>
class LockManager {
  using EM = atom::EpochManager<Allocator>;
  using EMB = atom::EpochManagerBase<Allocator>;

  using TimeStampTable = atom::
      AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, Allocator, false>;

 private:
  // fast implementation used with atomic maps since otherwise latching the lock manager data structure is needed
  // care for inner map ptr needed to guarantee no frees before last one finished using the ptr
  TimeStampTable tst_;
  Allocator* alloc_;
  EMB* emb_;
  common::GlobalLogger logger;

 public:
  LockManager(Allocator* alloc, EMB* emb)
      : tst_(std::thread::hardware_concurrency(), alloc, emb), alloc_(alloc), emb_(emb) {}

  bool waitDie(uint64_t transaction, bool exclusive, std::pair<uint64_t, std::set<uint64_t>>* mutex_ptr) {
    bool result = true;
    uint64_t ns_ts = 0;
    tst_.lookup(transaction, ns_ts);

    uint64_t check_ns = 0;
    bool found = false;
    if (mutex_ptr->first != 0) {
      found = tst_.lookup(mutex_ptr->first, check_ns);
      if (!found || ns_ts >= check_ns) {
        result = false;
      }
    }
    if (exclusive) {
      for (auto s : mutex_ptr->second) {
        found = tst_.lookup(s, check_ns);
        if (!found || ns_ts >= check_ns) {
          result = false;
        }
      }
    }
    return result;
  }

  template <template <typename> class Vector>
  bool lock(uint64_t transaction,
            bool exclusive,
            Vector<std::pair<uint64_t, std::set<uint64_t>>*>& table,
            uint64_t row,
            std::unordered_set<uint64_t>& abort_transaction) {
    bool inserted = false;
    atom::EpochGuard<EMB, EM> eg{emb_};
    void* addr = alloc_->template allocate<std::pair<uint64_t, std::set<uint64_t>>>(1);
    while (!inserted) {
      auto mutex_ptr = table[row];
      std::pair<uint64_t, std::set<uint64_t>>* new_mutex_ptr;
      if (mutex_ptr != nullptr) {
        if (mutex_ptr->first != 0 && mutex_ptr->first != transaction) {
          if (waitDie(transaction, exclusive, mutex_ptr)) {
            continue;
          } else {
            abort_transaction.insert(mutex_ptr->first);
            alloc_->deallocate(addr, 1);
            return false;
          }
        } else if (mutex_ptr->second.size() > 1 && exclusive) {
          if (waitDie(transaction, exclusive, mutex_ptr)) {
            continue;
          } else {
            for (auto i : mutex_ptr->second)
              abort_transaction.insert(i);
            alloc_->deallocate(addr, 1);
            return false;
          }
        } else if (mutex_ptr->second.size() == 1 && exclusive) {
          auto pos = std::find_if(mutex_ptr->second.begin(), mutex_ptr->second.end(),
                                  [=](uint64_t t) { return t == transaction; });
          if (pos == mutex_ptr->second.end()) {
            if (waitDie(transaction, exclusive, mutex_ptr)) {
              continue;
            } else {
              abort_transaction.insert(*mutex_ptr->second.begin());
              alloc_->deallocate(addr, 1);
              return false;
            }
          }
        }

        new_mutex_ptr = new (addr) std::pair<uint64_t, std::set<uint64_t>>{*mutex_ptr};
      } else {
        new_mutex_ptr = new (addr) std::pair<uint64_t, std::set<uint64_t>>{};
      }
      if (exclusive) {
        new_mutex_ptr->first = transaction;
      } else {
        new_mutex_ptr->second.insert(transaction);
      }
      inserted = table.compare_exchange(row, mutex_ptr, new_mutex_ptr);

      if (inserted && mutex_ptr != nullptr) {
        eg.erase(removeSet, nullptr, eg.getCurrentCounter(), reinterpret_cast<void*>(mutex_ptr));
      } else {
        new_mutex_ptr->second.clear();
      }
    }
    return true;
  }

  template <template <typename> class Vector>
  bool unlock(uint64_t transaction, Vector<std::pair<uint64_t, std::set<uint64_t>>*>& table, uint64_t row) {
    atom::EpochGuard<EMB, EM> eg{emb_};
    bool deleted = false;
    void* addr = alloc_->template allocate<std::pair<uint64_t, std::set<uint64_t>>>(1);

    while (!deleted) {
      auto mutex_ptr = table[row];

      std::pair<uint64_t, std::set<uint64_t>>* new_mutex_ptr =
          new (addr) std::pair<uint64_t, std::set<uint64_t>>{*mutex_ptr};

      if (new_mutex_ptr->first == transaction) {
        new_mutex_ptr->first = 0;
      }

      auto pos = std::find_if(new_mutex_ptr->second.begin(), new_mutex_ptr->second.end(),
                              [=](uint64_t t) { return t == transaction; });
      if (pos != new_mutex_ptr->second.end())
        new_mutex_ptr->second.erase(pos);
      deleted = table.compare_exchange(row, mutex_ptr, new_mutex_ptr);

      if (deleted) {
        eg.erase(removeSet, nullptr, eg.getCurrentCounter(), reinterpret_cast<void*>(mutex_ptr));
      } else {
        new_mutex_ptr->second.clear();
      }
    }
    return true;
  }

  static void removeSet(void* np, uint64_t commit_ts, void* ptr) {
    auto mset = reinterpret_cast<std::pair<uint64_t, std::set<uint64_t>>*>(ptr);
    mset->~pair();
  }

  void start(const uint64_t transaction) {
    atom::EpochGuard<EMB, EM> eg{emb_};
    std::chrono::nanoseconds ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch());
    tst_.insert(transaction, ns.count());
  };

  void end(const uint64_t transaction) {
    atom::EpochGuard<EMB, EM> eg{emb_};
    tst_.erase(transaction);
  };

  void log(const common::LogInfo log_info) { logger.log(log_info); }

  void log(const std::string log_info) { logger.log(log_info); }
};
};  // namespace twopl_table
