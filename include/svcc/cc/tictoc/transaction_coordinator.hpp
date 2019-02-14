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
#include "ds/atomic_unordered_set.hpp"
#include "svcc/cc/tictoc/transaction_information.hpp"
#include "svcc/cc/tictoc/validator.hpp"
#include <atomic>
#include <cassert>
#include <iostream>
#include <list>
#include <memory>
#include <thread>
#include <unordered_set>
#include <stdint.h>

#ifndef NDEBUG
#define verify(expression) assert(expression)
#else
#define verify(expression) ((void)(expression))
#endif

namespace tictoc {
namespace transaction {

using namespace tictoc;
using namespace tictoc::transaction;

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
  serial::Validator v_;
  Allocator* alloc_;
  atom::EpochManagerBase<Allocator>* emb_;

  static thread_local bool has_writer_;
  static thread_local std::atomic<uint64_t> transaction_counter_;
  static thread_local std::unordered_set<uint64_t> not_alive_;
  static thread_local uint8_t current_core_;
  static thread_local std::list<TransactionInformationBase<Allocator>*>* atom_info_;
  static thread_local atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>* eg_;

 public:
  TransactionCoordinator(Allocator* alloc, atom::EpochManagerBase<Allocator>* emb, bool online = false)
      : alloc_(alloc), emb_(emb){};

  /* The highest bit is used to determine read or write accesses the lower 63 for the actual transaction id */
  /* Returns the encoded bitstring for a given transaction and there action */
  inline static constexpr uint64_t access(const uint64_t transaction, const bool rw) {
    return rw ? 0x8000000000000000 | transaction : 0x7FFFFFFFFFFFFFFF & transaction;
  }

  /* Returns the transaction and the used transaction given an encoded bitstring */
  inline static constexpr std::tuple<uint64_t, bool> find(const uint64_t encoded_id) {
    return std::make_tuple(0x7FFFFFFFFFFFFFFF & encoded_id, encoded_id >> 63);
  }

  inline static constexpr uint64_t getWriteTS(uint64_t ts_word) { return ts_word & 0x0000FFFFFFFFFFFF; }

  inline static constexpr uint64_t getReadTS(uint64_t ts_word) {
    return getWriteTS(ts_word) + ((ts_word >> 48) & 0x7FFF);
  }

  inline static constexpr bool isLocked(uint64_t ts_word) { return ts_word >> 63; }

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
    verify(transaction > 0);

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      return false;
    }

    uint64_t info = access(transaction, false);
    verify(info > 0);

    uint64_t locked_1, locked_2;
    do {
      locked_1 = locked[offset];
      readValue = column[offset];
      locked_2 = locked[offset];
    } while (locked_1 != locked_2 || (locked_1 >> 63) == 1);

    for (auto it = atom_info_->begin(); it != atom_info_->end() && has_writer_;) {
      if ((*it)->isWriteTransaction() && (*it)->sameDataElem(reinterpret_cast<void*>(&locked), offset)) {
        readValue = *reinterpret_cast<Value*>((*it)->getValue());
      }
      ++it;
    }

#ifdef SGLOGGER
    v_.log(common::LogInfo{transaction, 0, reinterpret_cast<uintptr_t>(&column), offset, 'r'});
#endif

    auto rti = alloc_->template allocate<ReadTransactionInformation<Vector, List, Allocator>>(1);
    atom_info_->emplace_back(new (rti) ReadTransactionInformation<Vector, List, Allocator>(
        rw_table, locked, lsn_column, offset, transaction, locked_1));

    return true;
  }

  template <template <typename> class Vector, template <typename> class List>
  uint64_t read(Vector<uint64_t>& lsn_column,
                Vector<List<uint64_t>*>& rw_table,
                Vector<uint64_t>& locked,
                uint64_t offset,
                uint64_t transaction) {
    verify(transaction > 0);

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      return std::numeric_limits<uint64_t>::max();
    }

    uint64_t info = access(transaction, false);
    verify(info > 0);

    return locked[offset];
  }

  template <typename Value,
            template <typename>
            class ValueVector,
            template <typename>
            class Vector,
            template <typename>
            class List>
  void pureValue(Value& readValue,
                 ValueVector<Value>& column,
                 Vector<uint64_t>& lsn_column,
                 Vector<List<uint64_t>*>& rw_table,
                 Vector<uint64_t>& locked,
                 uint64_t offset,
                 uint64_t transaction) {
    readValue = column[offset];
    for (auto it = atom_info_->begin(); it != atom_info_->end() && has_writer_;) {
      if ((*it)->isWriteTransaction() && (*it)->sameDataElem(reinterpret_cast<void*>(&locked), offset)) {
        readValue = *reinterpret_cast<Value*>((*it)->getValue());
      }
      ++it;
    }
  }

  template <template <typename> class Vector, template <typename> class List>
  bool readUndo(uint64_t locked_prv,
                Vector<uint64_t>& lsn_column,
                Vector<List<uint64_t>*>& rw_table,
                Vector<uint64_t>& locked,
                uint64_t offset,
                uint64_t transaction) {
    auto locked_value = locked[offset];
    if (locked_value != locked_prv || (locked_prv >> 63) == 1)
      return false;

#ifdef SGLOGGER
    v_.log(common::LogInfo{transaction, 0, reinterpret_cast<uintptr_t>(&column), offset, 'r'});
#endif

    auto rti = alloc_->template allocate<ReadTransactionInformation<Vector, List, Allocator>>(1);
    atom_info_->emplace_back(new (rti) ReadTransactionInformation<Vector, List, Allocator>(
        rw_table, locked, lsn_column, offset, transaction, locked_prv));

    return true;
  }

  template <typename Value,
            template <typename>
            class ValueVector,
            template <typename>
            class Vector,
            template <typename>
            class List>
  bool writeValue(Value& writeValue,
                  ValueVector<Value>& column,
                  Vector<uint64_t>& lsn_column,
                  Vector<List<uint64_t>*>& rw_table,
                  Vector<uint64_t>& locked,
                  uint64_t offset,
                  uint64_t transaction) {
    verify(transaction > 0);

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      return false;
    }

    uint64_t locked_1, locked_2;
    do {
      locked_1 = locked[offset];
      locked_2 = locked[offset];
    } while (locked_1 != locked_2 || (locked_1 >> 63) == 1);

    /*
     * Not needed because the sorting of the write operation is correct for applying the final result
     *
     * for (auto it = atom_info_->begin(); it != atom_info_->end();) {
     *  if ((*it)->isWriteTransaction() && (*it)->sameDataElem(reinterpret_cast<void*>(&column), offset)) {
     *    (*it)->deallocate(alloc_);
     *    it = atom_info_->erase(it);
     *  } else {
     *    ++it;
     *  }
     * }
     */
    has_writer_ = true;
    auto wti = alloc_->template allocate<WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>>(1);
    atom_info_->emplace_back(new (wti) WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>(
        writeValue, column, rw_table, locked, lsn_column, offset, transaction, locked_1));

    return true;
  }

  template <typename Value, template <typename> class ValueVector>
  void writePhase(Value& writeValue, ValueVector<Value>& column, uint64_t offset, uint64_t transaction) {
#ifdef SGLOGGER
    v_.log(common::LogInfo{transaction, 0, reinterpret_cast<uintptr_t>(&column), offset, 'w'});
#endif

    column.replace(offset, writeValue);
  }

  template <template <typename> class Vector>
  void writePhaseCommit(Vector<uint64_t>& locked,
                        Vector<uint64_t>& lsn,
                        uint64_t offset,
                        uint64_t transaction,
                        uint64_t commit_ts) {
    auto old = 0x7FFFFFFFFFFFFFFF & locked.atomic_replace(offset, 0x8000000000000000 | commit_ts);
    lsn.atomic_replace(offset, old);
  }

  /*
   * Abort: Needs to redo all write operations of the aborted transaction and needs to abort all transactions
   * read or write on data used in this transaction -> cascading aborts
   */
  // template <template <typename> class Vector>
  void abort(uint64_t transaction, bool locked = false) {
    /*
     * Idea: Do undo of the operations in the undo log. Then check which transaction where in between the first
     * transaction of the to aborting transaction and the last undo of the abort process. Following, abort the
     * transaction found in between.
     */
    not_alive_.insert(transaction);

#ifdef SGLOGGER
    v_.log(common::LogInfo{transaction, 0, 0, 0, 'a'});
#endif

    if (locked)
      v_.template unlock(*this, *atom_info_);
    for (auto t : *atom_info_) {
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
      return false;
    }

    uint64_t commit_ts;
    bool val = v_.template validate(*this, *atom_info_, commit_ts, has_writer_);
    if (!val) {
      abort(transaction, true);
      not_alive_.erase(transaction);
      return false;
    }
    for (auto t : *atom_info_) {
      if (t->isWriteTransaction()) {
        t->writeValue(*this, commit_ts);
      }
    }

    for (auto t : *atom_info_) {
      if (t->isWriteTransaction()) {
        t->writeCommit(*this, commit_ts);
      }
    }

#ifdef SGLOGGER
    v_.log(common::LogInfo{transaction, 0, 0, 0, 'c'});
#endif

    v_.template unlock(*this, *atom_info_);
    for (auto t : *atom_info_) {
      t->deallocate(alloc_);
    }

    delete atom_info_;
    delete eg_;
    alloc_->tidyUp();
    return true;
  }

  inline uint64_t start() {
    auto tc = transaction_counter_.fetch_add(1) + 1;
    if (current_core_ == std::numeric_limits<uint8_t>::max()) {
      current_core_ = sched_getcpu();
    }
    uint64_t core = current_core_;
    tc &= 0x00FFFFFFFFFFFFFF;
    tc |= (core << 56);

    has_writer_ = false;
    atom_info_ = new std::list<TransactionInformationBase<Allocator>*>();
    eg_ = new atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>{emb_};

    bot(tc);
    return tc;
  }

  template <template <typename> class Vector>
  inline void lockValue(Vector<uint64_t>& locked, uint64_t offset) {
    bool replace = false;
    while (!replace) {
      uint64_t locked_ctr = locked[offset];
      if (locked_ctr >> 63 == 1)
        continue;
      replace = locked.compare_exchange(offset, locked_ctr, 0x8000000000000000 | locked_ctr);
    }
  }

  template <template <typename> class Vector>
  inline void unlockValue(Vector<uint64_t>& locked, uint64_t offset) {
    bool replace = false;
    while (!replace) {
      uint64_t locked_ctr = locked[offset];
      replace = locked.compare_exchange(offset, locked_ctr, locked_ctr & 0x7FFFFFFFFFFFFFFF);
    }
  }

  inline void bot(uint64_t transaction) {}
};
};  // namespace transaction
};  // namespace tictoc
