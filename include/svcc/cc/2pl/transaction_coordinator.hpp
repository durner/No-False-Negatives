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
#include "common/chunk_allocator.hpp"
#include "common/epoch_manager.hpp"
#include "ds/atomic_singly_linked_list.hpp"
#include "ds/atomic_unordered_map.hpp"
#include "ds/atomic_unordered_set.hpp"
#include "svcc/cc/2pl/lock_manager.hpp"
#include "svcc/cc/2pl/transaction_information.hpp"
#include <atomic>
#include <cassert>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>
#include <stdint.h>

namespace twopl {
namespace transaction {

using namespace twopl;
using namespace twopl::transaction;

/*
 *
 * The Transaction Coordinator does coordinate transaction s.t. it denies
 * transactions that would result in a conflict. It is modular s.t. the
 * transaction coordinator can be used by multiple conflict resolution
 * strategies.
 *
 */
template <typename Allocator>
class TransactionCoordinator {
 private:
  Allocator* alloc_;
  atom::EpochManagerBase<Allocator>* emb_;
  common::StdAllocator* std_alloc_;
  atom::EpochManagerBase<common::StdAllocator>* std_emb_;

  twopl::LockManager<common::StdAllocator> lock_manager_;
  tbb::spin_mutex mut;

  static thread_local std::atomic<uint64_t> transaction_counter_;
  static thread_local std::unordered_set<uint64_t> not_alive_;
  static thread_local uint8_t current_core_;
  static thread_local atom::AtomicSinglyLinkedList<TransactionInformationBase<Allocator>*>* atom_info_;
  static thread_local atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>* eg_;

 public:
  TransactionCoordinator(Allocator* alloc, atom::EpochManagerBase<Allocator>* emb, bool online = false)
      : alloc_(alloc),
        emb_(emb),
        std_alloc_(new common::StdAllocator{}),
        std_emb_(new atom::EpochManagerBase<common::StdAllocator>{std_alloc_}),
        lock_manager_(std_alloc_, std_emb_, 10, 100000),
        mut(){};

  /* The highest bit is used to determine read or write accesses the lower 63 for the actual transaction id */
  /* Returns the encoded bitstring for a given transaction and there action */
  inline static constexpr uint64_t access(const uint64_t transaction, const bool rw) {
    return rw ? 0x8000000000000000 | transaction : 0x7FFFFFFFFFFFFFFF & transaction;
  }

  /* Returns the transaction and the used transaction given an encoded bitstring */
  inline static constexpr std::tuple<uint64_t, bool> find(const uint64_t encoded_id) {
    return std::make_tuple(0x7FFFFFFFFFFFFFFF & encoded_id, encoded_id >> 63);
  }

  template <typename Value,
            template <typename>
            class ValueVector,
            template <typename>
            class Vector,
            template <typename>
            class List>
  bool readValue(Value& readValue,
                 ValueVector<Value>& column,
                 Vector<uint64_t>& lsn_column,
                 Vector<List<uint64_t>*>& rw_table,
                 Vector<uint64_t>& locked,
                 uint64_t offset,
                 uint64_t transaction) {
    /*
     * transaction numbering enough since multiple writes would automatically come to an error if someone is in
     * between cause he is either before or after me.
     */

    verify(transaction > 0);

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      return false;
    }

    uint64_t info = access(transaction, false);
    verify(info > 0);

    bool no_abort_needed = lock_manager_.lock(transaction, false, reinterpret_cast<void*>(&rw_table), offset);

#ifdef LOGGER
    lock_manager_.log(common::LogInfo{transaction, 0, reinterpret_cast<uintptr_t>(&rw_table), offset, 'r'});
#endif

    if (!no_abort_needed) {
      this->abort(transaction);
      return false;
    }

    readValue = column[offset];

    auto rti = alloc_->template allocate<ReadTransactionInformation<Vector, List, Allocator>>(1);
    atom_info_->push_front(new (rti)
                               ReadTransactionInformation<Vector, List, Allocator>(rw_table, 0, offset, transaction));

    return true;
  }

  template <template <typename> class Vector, template <typename> class List>
  uint64_t read(Vector<uint64_t>& lsn_column,
                Vector<List<uint64_t>*>& rw_table,
                Vector<uint64_t>& locked,
                uint64_t offset,
                uint64_t transaction) {
    /*
     * transaction numbering enough since multiple writes would automatically come to an error if someone is in
     * between cause he is either before or after me.
     */

    verify(transaction > 0);

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      return false;
    }

    uint64_t info = access(transaction, false);
    verify(info > 0);

    bool no_abort_needed = lock_manager_.lock(transaction, false, reinterpret_cast<void*>(&rw_table), offset);

#ifdef LOGGER
    lock_manager_.log(common::LogInfo{transaction, 0, reinterpret_cast<uintptr_t>(&rw_table), offset, 'r'});
#endif

    if (!no_abort_needed) {
      this->abort(transaction);
      return false;
    }

    return 1;
  }

  template <template <typename> class Vector, template <typename> class List>
  void readUndo(uint64_t prv,
                Vector<uint64_t>& lsn_column,
                Vector<List<uint64_t>*>& rw_table,
                Vector<uint64_t>& locked,
                uint64_t offset,
                uint64_t transaction) {
    auto rti = alloc_->template allocate<ReadTransactionInformation<Vector, List, Allocator>>(1);
    atom_info_->push_front(new (rti)
                               ReadTransactionInformation<Vector, List, Allocator>(rw_table, 0, offset, transaction));
  }

  template <typename Value,
            template <typename>
            class ValueVector,
            template <typename>
            class Vector,
            template <typename>
            class List,
            bool abort = false>
  bool writeValue(Value& writeValue,
                  ValueVector<Value>& column,
                  Vector<uint64_t>& lsn_column,
                  Vector<List<uint64_t>*>& rw_table,
                  Vector<uint64_t>& locked,
                  uint64_t offset,
                  uint64_t transaction) {
    /*
     * transaction numbering enough since multiple writes would automatically come to an error if someone is in
     * between cause he is either before or after me.
     */

    verify(transaction > 0);
    if (!abort) {
      auto not_alive = not_alive_.find(transaction);
      if (not_alive != not_alive_.end()) {
        return false;
      }

      bool no_abort_needed = lock_manager_.lock(transaction, true, reinterpret_cast<void*>(&rw_table), offset);

#ifdef LOGGER
      lock_manager_.log(common::LogInfo{transaction, 0, reinterpret_cast<uintptr_t>(&rw_table), offset, 'w'});
#endif

      if (!no_abort_needed) {
        this->abort(transaction);
        return false;
      }
    }

    Value old = column.replace(offset, writeValue);

    if (!abort) {
      auto wti = alloc_->template allocate<WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>>(1);
      atom_info_->push_front(new (wti) WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>(
          writeValue, old, column, lsn_column, rw_table, 0, offset, transaction, abort));
    }

    return true;
  }

  /*
   * Abort: Needs to redo all write operations of the aborted transaction and needs to abort all transactions
   * read or write on data used in this transaction -> cascading aborts
   */
  // template <template <typename> class Vector>
  void abort(uint64_t transaction) {
    /*
     * Idea: Do undo of the operations in the undo log. Then check which transaction where in between the first
     * transaction of the to aborting transaction and the last undo of the abort process. Following, abort the
     * transaction found in between.
     */
    not_alive_.insert(transaction);

    // std::cout << transaction << " is done!" << std::endl;

    for (auto t : *atom_info_) {
      if (!t->isWriteTransaction() || t->isAbort())
        continue;
      t->writeValue(*this);
    }

#ifdef LOGGER
    lock_manager_.log(common::LogInfo{transaction, 0, 0, 0, 'a'});
#endif

    /*
     * As long as uncomment expect mem leakage!
     */
    for (auto t : *atom_info_) {
      t->unlock(&lock_manager_);
      t->deleteFromRWTable();
      t->deallocate(alloc_);
    }
    delete atom_info_;
    delete eg_;
  }

  /* Commit: Needs to wait for the commit of all transactions in the read / write set of this transaction
   * to avoid w_1(x) r_2(x) w_2(x) c_2 a_1 and therefore inconsistent data in the database
   */
  bool commit(uint64_t transaction, std::unordered_set<uint64_t>& oset) {
    /*
     * Idea: Check if there are nodes in the SGT reachable from my node within 1 hop that haven't committed yet. If
     * not
     * all more hop nodes need to have committed already by induction. Hence, it is save to also commit this
     * transaction.
     * Otherwise, wait and yield for the other transactions having committed or aborted.
     */

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      not_alive_.erase(transaction);
      lock_manager_.end(transaction);
      return false;
    }

#ifdef LOGGER
    lock_manager_.log(common::LogInfo{transaction, 0, 0, 0, 'c'});
#endif

    for (auto t : *atom_info_) {
      t->unlock(&lock_manager_);
      t->deleteFromRWTable();
      t->deallocate(alloc_);
    }
    delete atom_info_;
    delete eg_;

    lock_manager_.end(transaction);

    return true;
  }

  inline uint64_t start() {
    auto tc = transaction_counter_.fetch_add(1) + 1;
    if (current_core_ == std::numeric_limits<uint8_t>::max()) {
      current_core_ = sched_getcpu();
    }
    uint64_t core = current_core_;
    verify(core <= 127);
    tc &= 0x00FFFFFFFFFFFFFF;
    tc |= (core << 56);

    atom_info_ = new atom::AtomicSinglyLinkedList<TransactionInformationBase<Allocator>*>(alloc_, emb_);
    eg_ = new atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>(emb_);

    bot(tc);
    lock_manager_.start(tc);
    return tc;
  }

  void bot(uint64_t transaction) {}
};

};  // namespace transaction
};  // namespace twopl
