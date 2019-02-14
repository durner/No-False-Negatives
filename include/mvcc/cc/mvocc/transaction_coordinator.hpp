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
#include "mvcc/cc/mvocc/transaction_information.hpp"
#include "mvcc/cc/mvocc/validator.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <thread>
#include <unordered_set>
#include <stdint.h>

#ifndef NDEBUG
#define verify(expression) assert(expression)
#else
#define verify(expression) ((void)(expression))
#endif

namespace mv {
namespace mvocc {
namespace transaction {

using namespace mv;
using namespace mv::mvocc;
using namespace mv::mvocc::transaction;

struct UndoBuffer {
  void* column;
  uint64_t offset;
  uint64_t commit_ts;
};

/*
 *
 * The Transaction Coordinator does coordinate transaction s.t. it denies
 * transactions that would result in a conflict. It is modular s.t. the
 * transaction coordinator can be used by multiple conflict resolution
 * strategies.
 *
 */
template <template <typename> class ValueVector, template <typename> class Vector, typename Allocator>
class TransactionCoordinator {
 public:
  using Alloc = Allocator;

 private:
  serial::Validator v_;
  Allocator* alloc_;
  atom::EpochManagerBase<Allocator>* emb_;
  std::atomic<uint64_t> transaction_counter_;
  std::map<uint64_t, std::vector<UndoBuffer*>*> undo_buffer_;

  static thread_local std::unordered_set<uint64_t> not_alive_;
  static thread_local std::unordered_set<uint64_t> abort_transaction_;
  static thread_local std::list<TransactionInformationBase<ValueVector, Vector, Allocator>*>* atom_info_;
  static thread_local atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>* eg_;
  static tbb::spin_mutex mut;

 public:
  TransactionCoordinator(Allocator* alloc, atom::EpochManagerBase<Allocator>* emb, bool online = false)
      : alloc_(alloc), emb_(emb), undo_buffer_(){};

  ~TransactionCoordinator() { std::cout << "Average Chain Length: " << getAVGVC() << std::endl; }

  /* The highest bit is used to determine read or write accesses the lower 63 for the actual transaction id */
  /* Returns the encoded bitstring for a given transaction and there action */
  inline static constexpr uint64_t access(const uint64_t transaction, const bool rw) {
    return rw ? 0x8000000000000000 | transaction : 0x7FFFFFFFFFFFFFFF & transaction;
  }

  /* Returns the transaction and the used transaction given an encoded bitstring */
  inline static constexpr std::tuple<uint64_t, bool> find(const uint64_t encoded_id) {
    return std::make_tuple(0x7FFFFFFFFFFFFFFF & encoded_id, encoded_id >> 63);
  }

  inline void waitSafeRead() const {}

  inline double getAVGVC() { return 0; }

  template <typename MValue, bool MultiRead = false, class List>
  uint64_t readVersion(Vector<List*>& rw_table,
                       Vector<uint64_t>& locked,
                       Vector<uint64_t>& lsn,
                       Vector<MValue*>& version_chain,
                       uint64_t& aid,
                       MValue*& ptr,
                       uint64_t offset,
                       uint64_t transaction) {
    verify(transaction > 0);

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      aid = std::numeric_limits<uint64_t>::max();
      ptr = nullptr;
      return 0;
    }

    bool already_writing = false;
    for (auto t : *atom_info_) {
      if (t->isWriteTransaction() && t->sameDataElem(reinterpret_cast<void*>(&locked), offset))
        already_writing = true;
    }

    tagPtr(version_chain, offset, MultiRead);
    if (reinterpret_cast<uintptr_t>(version_chain[offset]) == 0x8000000000000000) {
      aid = 0;
      ptr = nullptr;
      return 1;
    } else {
      // different than nullptr
      aid = 0;
      ptr = nullptr;

      if (lsn[offset] <= transaction || already_writing)
        return 1;

      auto elem = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(version_chain[offset]));
      while (true) {
        if (elem->epoch <= transaction) {
          break;
        }
        elem = elem->nxt;
      }
      ptr = elem;
      return 1;
    }
  }

  template <typename Value, typename MValue, typename Accessor>
  void readValue(Value& val,
                 ValueVector<Value>& column,
                 Accessor acc,
                 MValue* version_ptr,
                 uint64_t offset,
                 uint64_t transaction) {
    if (version_ptr == nullptr) {
      val = column[offset];
    } else {
      val = acc(version_ptr);
    }
  }

  template <typename MValue, bool MultiRead = false, class List>
  void readFinish(uint64_t id,
                  uint64_t val,
                  Vector<List*>& rw_table,
                  Vector<uint64_t>& locked,
                  Vector<uint64_t>& lsn,
                  Vector<MValue*>& version_chain,
                  uint64_t offset,
                  uint64_t transaction) {
    untagPtr(version_chain, offset);
    if (!MultiRead) {
      auto rti = alloc_->template allocate<ReadTransactionInformation<ValueVector, Vector, List, Allocator>>(1);
      atom_info_->emplace_front(new (rti) ReadTransactionInformation<ValueVector, Vector, List, Allocator>(
          rw_table, locked, lsn, offset, offset, transaction));
    }
  }

  template <typename MValue, typename COW, typename COA, class List>
  uint64_t writeInit(Vector<List*>& rw_table,
                     Vector<uint64_t>& locked,
                     Vector<uint64_t>& lsn,
                     Vector<MValue*>& version_chain,
                     COW cow,
                     COA coa,
                     uint64_t offset,
                     uint64_t transaction) {
    verify(transaction > 0);

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      return std::numeric_limits<uint64_t>::max();
    }

    bool already_writing = false;
    for (auto t : *atom_info_) {
      if (t->isWriteTransaction() && t->sameDataElem(reinterpret_cast<void*>(&locked), offset))
        already_writing = true;
    }

    tagPtr(version_chain, offset);

    if (lsn[offset] == std::numeric_limits<uint64_t>::max()) {
      if (!already_writing) {
        untagPtr(version_chain, offset);
        abort(transaction);
        return std::numeric_limits<uint64_t>::max();
      }
    }

    if (!already_writing) {
      MValue* elem = alloc_->template allocate<MValue>(1);
      elem = new (elem) MValue{};
      cow(elem, offset);
      elem->transaction = lsn[offset];
      elem->epoch = lsn[offset];

      elem->nxt = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(version_chain[offset]));
      elem->prv = nullptr;

      if (reinterpret_cast<void*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(elem->nxt)) != nullptr) {
        elem->nxt->prv = elem;
      }

      version_chain.atomic_replace(offset,
                                   reinterpret_cast<MValue*>(0x8000000000000000 | reinterpret_cast<uintptr_t>(elem)));

      lsn.atomic_replace(offset, std::numeric_limits<uint64_t>::max());

      auto wti =
          alloc_->template allocate<WriteTransactionInformation<MValue, COA, ValueVector, Vector, List, Allocator>>(1);
      atom_info_->emplace_front(new (wti)
                                    WriteTransactionInformation<MValue, COA, ValueVector, Vector, List, Allocator>(
                                        rw_table, locked, lsn, version_chain, coa, offset, transaction));
    }
    untagPtr(version_chain, offset);
    return 1;
  }

  template <typename Value>
  void write(Value& writeValue, ValueVector<Value>& column, uint64_t offset) {
    column.replace(offset, writeValue);
  }

  void inline writeFinish(Vector<uint64_t>& locked, Vector<uint64_t>& lsn_column, uint64_t offset, uint64_t prv) {}

  template <typename MValue>
  static void erase(void* chain, uint64_t offset, void* ptr) {
    auto version_chain = reinterpret_cast<Vector<MValue*>*>(chain);
    tagPtr(*version_chain, offset);
    auto elem = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(ptr));

    // std::cout << offset << "delete : " << elem->epoch << " in " << vg_->getCurrentCounter() << std::endl;
    if (elem->prv == nullptr) {
      version_chain->atomic_replace(
          offset, reinterpret_cast<MValue*>(0x8000000000000000 | reinterpret_cast<uintptr_t>(elem->nxt)));
      if (elem->nxt != nullptr)
        elem->nxt->prv = nullptr;
    } else {
      elem->prv->nxt = elem->nxt;
      if (elem->nxt != nullptr)
        elem->nxt->prv = elem->prv;
    }
    untagPtr(*version_chain, offset);
  }

  static void removeBuffer(void* undo_buf, uint64_t commit_ts, void* ptr) {
    auto undo_buffer_ = reinterpret_cast<std::map<uint64_t, std::vector<UndoBuffer*>*>*>(undo_buf);
    std::vector<UndoBuffer*>* vec;
    mut.lock();
    auto it = undo_buffer_->find(commit_ts);
    vec = it->second;
    undo_buffer_->erase(it);
    mut.unlock();

    for (auto t : *vec) {
      delete t;
    }
    delete vec;
  }

  template <typename MValue, typename COA>
  void abortWrite(Vector<MValue*>& version_chain, Vector<uint64_t>& lsn_column, uint64_t offset, COA coa) {
    auto beg = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(version_chain[offset]));
    // std::cout << offset << "a: " << beg->epoch << " | " << beg->balance << std::endl;
    coa(beg, offset);
    lsn_column.atomic_replace(offset, beg->epoch);
    if (beg->prv == nullptr) {
      version_chain.atomic_replace(
          offset, reinterpret_cast<MValue*>(0x8000000000000000 | reinterpret_cast<uintptr_t>(beg->nxt)));
      if (beg->nxt != nullptr)
        beg->nxt->prv = nullptr;
    } else {
      exit(-1);
    }
    alloc_->deallocate(beg, 1);
  }

  template <typename MValue>
  inline void* removeWriteChain(Vector<MValue*>& version_chain,
                                Vector<uint64_t>& lsn_column,
                                uint64_t offset,
                                uint64_t commit_ts) {
    auto elem = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(version_chain[offset]));
    // std::cout << offset << "c:" << beg << std::endl;
    lsn_column.atomic_replace(offset, commit_ts);
    return reinterpret_cast<void*>(elem);
  }

  template <typename MValue>
  inline void consolidateChain(Vector<MValue*>& version_chain, uint64_t offset, void* elem) {
    eg_->erase(erase<MValue>, reinterpret_cast<void*>(&version_chain), offset, elem);
  }

  /*
   * Abort: Needs to redo all write operations of the aborted transaction and needs to abort all transactions
   * read or write on data used in this transaction -> cascading aborts
   */
  void abort(uint64_t transaction) {
    /*
     * Idea: Do undo of the operations in the undo log. Then check which transaction where in between the first
     * transaction of the to aborting transaction and the last undo of the abort process. Following, abort the
     * transaction found in between.
     */
    not_alive_.insert(transaction);

#ifdef SGLOGGER
    v_.log(common::LogInfo{transaction, 0, 0, 0, 'a'});
#endif

    for (auto t : *atom_info_) {
      if (t->isWriteTransaction()) {
        t->lockValue(*this);
      }
    }

    for (auto t : *atom_info_) {
      if (t->isWriteTransaction()) {
        t->abortWrite(*this);
      }
    }

    for (auto t : *atom_info_) {
      if (t->isWriteTransaction()) {
        t->unlockValue(*this);
      }
    }

    for (auto t : *atom_info_) {
      t->deallocate(alloc_);
    }

    delete eg_;
    delete atom_info_;
    alloc_->tidyUp();
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

    bool isWriter = false;
    for (auto t : *atom_info_) {
      if (t->isWriteTransaction()) {
        isWriter = true;
      }
    }

    if (isWriter) {
      mut.lock();
      uint64_t commit_ts = transaction_counter_.fetch_add(1) + 1;
      bool val = v_.template validate<UndoBuffer>(*atom_info_, undo_buffer_, transaction, commit_ts);

      if (!val) {
        mut.unlock();
        abort(transaction);
        not_alive_.erase(transaction);
        return false;
      }

      for (auto t : *atom_info_) {
        if (t->isWriteTransaction()) {
          t->lockValue(*this);
        }
      }

      mut.unlock();

      for (auto t : *atom_info_) {
        if (t->isWriteTransaction()) {
          t->commitWrite(*this, commit_ts);
        }
      }

      for (auto t : *atom_info_) {
        if (t->isWriteTransaction()) {
          t->unlockValue(*this);
        }
      }

      for (auto t : *atom_info_) {
        if (t->isWriteTransaction()) {
          t->consolidateChain(*this);
        }
      }

      eg_->erase(removeBuffer, reinterpret_cast<void*>(&undo_buffer_), commit_ts, nullptr);
    }

#ifdef SGLOGGER
    v_.log(common::LogInfo{transaction, 0, 0, 0, 'c'});
#endif

    for (auto t : *atom_info_) {
      t->deallocate(alloc_);
    }

    delete atom_info_;
    delete eg_;
    alloc_->tidyUp();
    return true;
  }

  inline void waitAndTidy() {
    eg_ = new atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>{emb_};
    delete eg_;
    alloc_->tidyUp();
  }

  inline uint64_t start() {
    eg_ = new atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>{emb_};
    mut.lock();
    auto tc = transaction_counter_.fetch_add(1) + 1;
    mut.unlock();
    atom_info_ = new std::list<TransactionInformationBase<ValueVector, Vector, Allocator>*>();
    return tc;
  }

  template <typename MValue>
  static inline void tagPtr(Vector<MValue*>& ptr_vec, uint64_t offset, const bool wait = false) {
    bool replace = false;
    while (!replace) {
      auto ptr = reinterpret_cast<uintptr_t>(ptr_vec[offset]);
      if ((ptr >> 63) == 1) {
        if (wait) {
          auto waittime = rand() & 0xFFFF;
          for (volatile auto i = 0; i < waittime; i++) {
          }
        }
        continue;
      }
      replace = ptr_vec.compare_exchange(offset, reinterpret_cast<MValue*>(ptr),
                                         reinterpret_cast<MValue*>(0x8000000000000000 | ptr));
    }
  }

  template <typename MValue>
  static inline void untagPtr(Vector<MValue*>& ptr_vec, uint64_t offset) {
    bool replace = false;
    while (!replace) {
      auto ptr = reinterpret_cast<uintptr_t>(ptr_vec[offset]);
      replace = ptr_vec.compare_exchange(offset, reinterpret_cast<MValue*>(ptr),
                                         reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & ptr));
    }
  }

  inline uint64_t bot(uint64_t transaction) { return transaction; }
};
};  // namespace transaction
};  // namespace mvocc
};  // namespace mv
