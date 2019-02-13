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
#include <tbb/spin_rw_mutex.h>

//#define LOGGER 1

#ifndef NDEBUG
#define verify(expression) assert(expression)
#else
#define verify(expression) ((void)(expression))
#endif

namespace twopl {

template <typename Allocator>
class LockManager {
  using EM = atom::EpochManager<Allocator>;
  using EMB = atom::EpochManagerBase<Allocator>;

  using LockTable =
      atom::AtomicUnorderedMap<std::pair<uint64_t, std::set<uint64_t>>*,
                               uint64_t,
                               atom::AtomicUnorderedMapBucket<std::pair<uint64_t, std::set<uint64_t>>*, uint64_t>,
                               Allocator,
                               false>;

  using RelationTable = atom::
      AtomicUnorderedMap<LockTable*, uint64_t, atom::AtomicUnorderedMapBucket<LockTable*, uint64_t>, Allocator, false>;

  using TimeStampTable = atom::
      AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, Allocator, false>;

 private:
  // fast implementation used with atomic maps since otherwise latching the lock manager data structure is needed
  // care for inner map ptr needed to guarantee no frees before last one finished using the ptr
  TimeStampTable tst_;
  Allocator* alloc_;
  EMB* emb_;
  RelationTable locks_to_table_map_;
  uint64_t row_size_;
  common::GlobalLogger logger;
  tbb::spin_mutex mut;

 public:
  LockManager(Allocator* alloc, EMB* emb, uint64_t relation_size, uint64_t row_size)
      : tst_(std::thread::hardware_concurrency(), alloc, emb),
        alloc_(alloc),
        emb_(emb),
        locks_to_table_map_(relation_size, alloc, emb),
        row_size_(row_size),
        mut() {}

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

  bool lock(uint64_t transaction, bool exclusive, void* table, uint64_t row) {
    bool inserted = false;
    atom::EpochGuard<EMB, EM> eg{emb_};
    while (!inserted) {
      LockTable* table_lock_ptr;
      bool found = locks_to_table_map_.lookup(reinterpret_cast<uintptr_t>(table), table_lock_ptr);

      if (!found) {
        mut.lock();
        found = locks_to_table_map_.lookup(reinterpret_cast<uintptr_t>(table), table_lock_ptr);
        if (!found) {
          auto insert_map = new LockTable{row_size_, alloc_, emb_};
          bool del = locks_to_table_map_.insert(reinterpret_cast<uintptr_t>(table), insert_map);
          if (!del)
            delete insert_map;
        }
        mut.unlock();
      } else {
        while (!inserted) {
          std::pair<uint64_t, std::set<uint64_t>>* mutex_ptr;
          found = table_lock_ptr->lookup(row, mutex_ptr);

          if (!found) {
            void* addr = alloc_->template allocate<std::pair<uint64_t, std::set<uint64_t>>>(1);
            mutex_ptr = new (addr) std::pair<uint64_t, std::set<uint64_t>>{};
            assert(mutex_ptr->first == 0);
            bool del = table_lock_ptr->insert(row, mutex_ptr);
            if (!del)
              alloc_->deallocate(mutex_ptr, 1);
          } else {
            if (mutex_ptr->first != 0 && mutex_ptr->first != transaction) {
              if (waitDie(transaction, exclusive, mutex_ptr)) {
                continue;
              } else {
                return false;
              }
            } else if (mutex_ptr->second.size() > 1 && exclusive) {
              if (waitDie(transaction, exclusive, mutex_ptr)) {
                continue;
              } else {
                return false;
              }
            } else if (mutex_ptr->second.size() == 1 && exclusive) {
              auto pos = std::find_if(mutex_ptr->second.begin(), mutex_ptr->second.end(),
                                      [=](uint64_t t) { return t == transaction; });
              if (pos == mutex_ptr->second.end()) {
                if (waitDie(transaction, exclusive, mutex_ptr)) {
                  continue;
                } else {
                  return false;
                }
              }
            }

            void* addr = alloc_->template allocate<std::pair<uint64_t, std::set<uint64_t>>>(1);
            std::pair<uint64_t, std::set<uint64_t>>* new_mutex_ptr =
                new (addr) std::pair<uint64_t, std::set<uint64_t>>{*mutex_ptr};
            if (exclusive) {
              new_mutex_ptr->first = transaction;
            } else {
              new_mutex_ptr->second.insert(transaction);
            }
            inserted = table_lock_ptr->compare_and_swap(row, mutex_ptr, new_mutex_ptr);

            if (inserted) {
              eg.add(mutex_ptr);
            } else {
              new_mutex_ptr->second.clear();
              alloc_->deallocate(new_mutex_ptr, 1);
            }
          }
        }
      }
    }
    return true;
  }

  bool unlock(uint64_t transaction, void* table, uint64_t row) {
    LockTable* table_lock_ptr;
    bool found = locks_to_table_map_.lookup(reinterpret_cast<uintptr_t>(table), table_lock_ptr);

    verify(found);

    atom::EpochGuard<EMB, EM> eg{emb_};
    bool deleted = false;
    while (!deleted) {
      std::pair<uint64_t, std::set<uint64_t>>* mutex_ptr;
      found = table_lock_ptr->lookup(row, mutex_ptr);

      assert(found);
      if (!found) {
        return false;
      }

      void* addr = alloc_->template allocate<std::pair<uint64_t, std::set<uint64_t>>>(1);
      std::pair<uint64_t, std::set<uint64_t>>* new_mutex_ptr =
          new (addr) std::pair<uint64_t, std::set<uint64_t>>{*mutex_ptr};
      if (new_mutex_ptr->first == transaction) {
        new_mutex_ptr->first = 0;
      }

      auto pos = std::find_if(new_mutex_ptr->second.begin(), new_mutex_ptr->second.end(),
                              [=](uint64_t t) { return t == transaction; });
      if (pos != new_mutex_ptr->second.end())
        new_mutex_ptr->second.erase(pos);
      deleted = table_lock_ptr->compare_and_swap(row, mutex_ptr, new_mutex_ptr);

      if (deleted) {
        eg.add(mutex_ptr);
      } else {
        new_mutex_ptr->second.clear();
        alloc_->deallocate(new_mutex_ptr, 1);
      }
    }
    return true;
  }

  void start(const uint64_t transaction) {
    std::chrono::nanoseconds ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch());
    tst_.insert(transaction, ns.count());
  };

  void end(const uint64_t transaction) { tst_.erase(transaction); };

  void log(const common::LogInfo log_info) { logger.log(log_info); }

  void log(const std::string log_info) { logger.log(log_info); }
};
};  // namespace twopl
