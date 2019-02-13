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
#include "mvcc/cc/nofalsenegatives/serialization_graph.hpp"
#include "mvcc/cc/nofalsenegatives/transaction_information.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <thread>
#include <stdint.h>

#ifndef NDEBUG
#define verify(expression) assert(expression)
#else
#define verify(expression) ((void)(expression))
#endif

namespace mv {
namespace nofalsenegatives {
namespace transaction {

using namespace mv;
using namespace mv::nofalsenegatives;
using namespace mv::nofalsenegatives::transaction;

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
  serial::SerializationGraph sg_;
  Allocator* alloc_;
  atom::EpochManagerBase<Allocator>* emb_;
  tbb::spin_mutex mut;

  std::atomic<uint64_t> vc_length_;
  std::atomic<uint64_t> vc_count_;

  static thread_local std::atomic<uint64_t> transaction_counter_;
  static thread_local std::unordered_set<uint64_t> not_alive_;
  static thread_local uint8_t current_core_;
  static thread_local std::unordered_set<uint64_t> abort_transaction_;
  static thread_local std::list<TransactionInformationBase<ValueVector, Vector, Allocator>*>* atom_info_;
  static thread_local atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>* eg_;

 public:
  TransactionCoordinator(Allocator* alloc, atom::EpochManagerBase<Allocator>* emb, bool online = false)
      : sg_(alloc, emb), alloc_(alloc), emb_(emb), mut(){};

  /* The highest bit is used to determine read or write accesses the lower 63 for the actual transaction id */
  /* Returns the encoded bitstring for a given transaction and there action */
  inline static constexpr uint64_t access(const uint64_t transaction, const bool rw) {
    return rw ? 0x8000000000000000 | transaction : 0x7FFFFFFFFFFFFFFF & transaction;
  }

  /* Returns the transaction and the used transaction given an encoded bitstring */
  inline static constexpr std::tuple<uint64_t, bool> find(const uint64_t encoded_id) {
    return std::make_tuple(0x7FFFFFFFFFFFFFFF & encoded_id, encoded_id >> 63);
  }

  inline void waitSafeRead() {
    sg_.setInactive();
    eg_->waitSafeRead();
    // std::cout << "VG: " << vg_->getCurrentCounter() << " | Safe Read: " << vg_->getSafeReadVersion() << std::endl;
  }

  inline double getAVGVC() {
    return 0;  //((double) vc_length_) / vc_count_;
  }

  template <typename MValue, bool ReadOnly = false, class List>
  uint64_t readVersion(Vector<List*>& rw_table,
                       Vector<uint64_t>& locked,
                       Vector<uint64_t>& lsn,
                       Vector<MValue*>& version_chain,
                       uint64_t& aid,
                       MValue*& ptr,
                       uint64_t offset,
                       uint64_t transaction) {
    verify(transaction > 0);

    if (!ReadOnly) {
      auto rw = rw_table[offset];
      auto not_alive = not_alive_.find(transaction);
      if (not_alive != not_alive_.end()) {
        aid = std::numeric_limits<uint64_t>::max();
        ptr = nullptr;
        return 0;
      }

      if (sg_.needsAbort(transaction)) {
        this->abort(transaction);
        aid = std::numeric_limits<uint64_t>::max();
        ptr = nullptr;
        return 0;
      }

      uint64_t info = access(transaction, false);
      verify(info > 0);

      uint64_t prv = rw_table[offset]->push_front(info);
      if (prv > 0) {
        /*
         * expected_id is fine for coluom check since if the prv transaction is not yet finished in the
         * rw_table[offset]
         * vector the values are initialized with 0. Since the transaction ids must be greater 0, the following
         * for loop is only evaluated positive iff the transaction id was set beforehand and the vector operation is
         * already finished.
         */
        for (auto i = 0; lsn[offset] != prv; i++) {
          if (i >= 10000)
            std::this_thread::yield();
        }
      }

      auto cyclic = false;
      auto it = rw->begin();
      for (; it != rw->end(); ++it) {
        if (it.getId() < prv && std::get<1>(find(*it))) {
          if (!sg_.insert_and_check(std::get<0>(find(*it)), false)) {
            cyclic = true;
          }
        }
      }

#ifdef SGLOGGER
      char c = 'r';
      if (cyclic) {
        c = 'f';
      }
      sg_.log(common::LogInfo{transaction, val, reinterpret_cast<uintptr_t>(&rw_table), offset, c});
#endif

      if (cyclic) {
        rw->erase(prv);
        lsn.atomic_replace(offset, prv + 1);
        this->abort(transaction);
        aid = std::numeric_limits<uint64_t>::max();
        ptr = nullptr;
        return 0;
      }

      aid = prv;
      ptr = nullptr;
      return prv + 1;

    } else {
      tagPtr(version_chain, offset, true);
      // vc_count_++;
      if (reinterpret_cast<uintptr_t>(version_chain[offset]) == 0x8000000000000000) {
        aid = 0;
        ptr = nullptr;
        return std::numeric_limits<uint64_t>::max();
      } else {
        // uint64_t vc = 0;
        // different than nullptr
        aid = 0;
        ptr = nullptr;

        uint64_t version = eg_->getSafeReadVersion();
        auto elem = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(version_chain[offset]));
        if (elem->epoch <= version) {
          // vc_length_++;
          return std::numeric_limits<uint64_t>::max();
        }
        while (elem != nullptr) {
          // vc++;
          if (elem->nxt == nullptr) {
            break;
          }
          if (elem->nxt->epoch <= version) {
            break;
          }
          elem = elem->nxt;
        }
        // vc_length_ += vc;
        ptr = elem;
        return std::numeric_limits<uint64_t>::max();
      }
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

  template <typename MValue, bool ReadOnly = false, class List>
  void readFinish(uint64_t id,
                  uint64_t val,
                  Vector<List*>& rw_table,
                  Vector<uint64_t>& locked,
                  Vector<uint64_t>& lsn,
                  Vector<MValue*>& version_chain,
                  uint64_t offset,
                  uint64_t transaction) {
    if (ReadOnly) {
      untagPtr(version_chain, offset);
    } else {
      lsn.atomic_replace(offset, val);
      auto rti = alloc_->template allocate<ReadTransactionInformation<ValueVector, Vector, List, Allocator>>(1);
      atom_info_->emplace_front(new (rti) ReadTransactionInformation<ValueVector, Vector, List, Allocator>(
          rw_table, locked, lsn, id, offset, transaction));
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
  begin_write:
    auto rw = rw_table[offset];
    verify(transaction > 0);

    auto not_alive = not_alive_.find(transaction);
    if (not_alive != not_alive_.end()) {
      return std::numeric_limits<uint64_t>::max();
    }

    if (sg_.needsAbort(transaction)) {
      this->abort(transaction);
      return std::numeric_limits<uint64_t>::max();
    }

    uint64_t info = access(transaction, true);
    verify(info > 0);

    uint64_t prv = rw_table[offset]->push_front(info);
    if (prv > 0) {
      /*
       * expected_id is fine for coluom check since if the prv transaction is not yet finished in the
       * rw_table[offset]
       * vector the values are initialized with 0. Since the transaction ids must be greater 0, the following
       * for loop is only evaluated positive iff the transaction id was set beforehand and the vector operation is
       * already finished.
       */
      for (auto i = 0; lsn[offset] != prv; i++) {
        if (i >= 10000)
          std::this_thread::yield();
      }
    }

    // We need to delay w,w conflicts to be able to serialize the graph
    auto it_wait = rw->begin();
    auto it_end = rw->end();

    verify(it_wait != rw->end());

    bool already_writing = false;

    bool cyclic = false, wait = false;
    while (it_wait != it_end) {
      if (it_wait.getId() < prv && std::get<1>(find(*it_wait)) && std::get<0>(find(*it_wait)) != transaction) {
        // Need to be removed from rw table because of the deletion of the first version element
        // if (!sg_.isCommited(std::get<0>(find(*it_wait)))) {
        if (!sg_.insert_and_check(std::get<0>(find(*it_wait)), false)) {
          cyclic = true;
        }
        wait = true;
        //}
      }
      if (it_wait.getId() < prv && std::get<1>(find(*it_wait)) && std::get<0>(find(*it_wait)) == transaction) {
        ++it_wait;
        already_writing = true;
      } else
        ++it_wait;
    }

    if (cyclic) {
    cyclic_write:
      rw->erase(prv);
      lsn.atomic_replace(offset, prv + 1);
      this->abort(transaction);
      return std::numeric_limits<uint64_t>::max();
    }

    if (wait) {
      rw->erase(prv);
      lsn.atomic_replace(offset, prv + 1);
      goto begin_write;
    }

    auto it = rw->begin();
    auto end = rw->end();
    verify(rw->size() > 0);
    verify(it != rw->end());

    while (it != end) {
      if (it.getId() < prv && !sg_.insert_and_check(std::get<0>(find(*it)), !std::get<1>(find(*it)))) {
        cyclic = true;
      }
      ++it;
    }

#ifdef SGLOGGER
    if (!(wait && !cyclic)) {
      char c = 'w';
      if (cyclic) {
        c = 'e';
      }
      sg_.log(common::LogInfo{transaction, val, reinterpret_cast<uintptr_t>(&rw_table), offset, c});
    }
#endif

    if (cyclic) {
      goto cyclic_write;
    }

    if (!already_writing) {
      auto wti =
          alloc_->template allocate<WriteTransactionInformation<MValue, COA, ValueVector, Vector, List, Allocator>>(1);
      atom_info_->emplace_front(new (wti)
                                    WriteTransactionInformation<MValue, COA, ValueVector, Vector, List, Allocator>(
                                        rw_table, locked, lsn, version_chain, coa, prv, offset, transaction));

      tagPtr(version_chain, offset);
      MValue* elem = alloc_->template allocate<MValue>(1);
      elem = new (elem) MValue{};
      cow(elem, offset);
      elem->transaction = transaction;
      elem->epoch = std::numeric_limits<uint64_t>::max();

      elem->nxt = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(version_chain[offset]));
      elem->prv = nullptr;

      if (reinterpret_cast<void*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(elem->nxt)) != nullptr) {
        elem->nxt->prv = elem;
      }

      version_chain.atomic_replace(offset,
                                   reinterpret_cast<MValue*>(0x8000000000000000 | reinterpret_cast<uintptr_t>(elem)));

      untagPtr(version_chain, offset);
    } else {
      auto rti = alloc_->template allocate<ReadTransactionInformation<ValueVector, Vector, List, Allocator>>(1);
      atom_info_->emplace_front(new (rti) ReadTransactionInformation<ValueVector, Vector, List, Allocator>(
          rw_table, locked, lsn, prv, offset, transaction));
    }

    return prv + 1;
  }

  template <typename Value>
  void write(Value& writeValue, ValueVector<Value>& column, uint64_t offset) {
    column.replace(offset, writeValue);
  }

  void inline writeFinish(Vector<uint64_t>& locked, Vector<uint64_t>& lsn, uint64_t offset, uint64_t prv) {
    lsn.atomic_replace(offset, prv);
  }

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

  template <typename MValue, typename COA>
  void abortWrite(Vector<MValue*>& version_chain, uint64_t offset, COA coa) {
    tagPtr(version_chain, offset);
    auto beg = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(version_chain[offset]));
    // std::cout << offset << "a: " << beg->epoch << " | " << beg->balance << std::endl;
    coa(beg, offset);
    if (beg->prv == nullptr) {
      version_chain.atomic_replace(
          offset, reinterpret_cast<MValue*>(0x8000000000000000 | reinterpret_cast<uintptr_t>(beg->nxt)));
      if (beg->nxt != nullptr)
        beg->nxt->prv = nullptr;
    } else {
      std::cout << "abortWrite" << std::endl;
      exit(-1);
    }
    alloc_->deallocate(beg, 1);
    untagPtr(version_chain, offset);
  }

  template <typename MValue>
  void inline removeWriteChain(Vector<MValue*>& version_chain, uint64_t offset) {
    tagPtr(version_chain, offset);
    auto beg = reinterpret_cast<MValue*>(0x7FFFFFFFFFFFFFFF & reinterpret_cast<uintptr_t>(version_chain[offset]));
    // std::cout << offset << "c:" << beg << std::endl;
    beg->epoch = eg_->getCommitCtr();
    beg->commited = true;
    untagPtr(version_chain, offset);
    eg_->erase(erase<MValue>, reinterpret_cast<void*>(&version_chain), offset, reinterpret_cast<void*>(beg));
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

    for (auto t : *atom_info_) {
      t->abortWrite(*this);
    }

    sg_.abort(abort_transaction_);

    for (auto t : *atom_info_) {
      t->deleteEntry();
      t->deallocate(alloc_);
    }
    atom_info_->~list();
    eg_->~EpochGuard();
  }

  /* Commit: Needs to wait for the commit of all transactions in the read / write set of this transaction
   * to avoid w_1(x) r_2(x) w_2(x) c_2 a_1 and therefore inconsistent data in the database
   */
  bool commit(uint64_t transaction, std::unordered_set<uint64_t>& abort_transaction) {
    /*
     * Idea: Check if there are nodes in the SGT reachable from my node within 1 hop that haven't committed yet. If
     * not
     * all more hop nodes need to have committed already by induction. Hence, it is save to also commit this
     * transaction.
     * Otherwise, wait and yield for the other transactions having committed or aborted.
     */

    bool all_pending_transactions_commited = false;
    while (!all_pending_transactions_commited) {
      auto not_alive = not_alive_.find(transaction);
      if (not_alive != not_alive_.end()) {
        not_alive_.erase(transaction);
        abort_transaction = abort_transaction_;
        return false;
      }

      if (sg_.needsAbort(transaction)) {
        this->abort(transaction);
        not_alive_.erase(transaction);
        abort_transaction = abort_transaction_;
        return false;
      }

      all_pending_transactions_commited = sg_.checkCommited();

      if (all_pending_transactions_commited) {
        eg_->commit();
        for (auto t : *atom_info_) {
          t->removeChain(*this);
        }
        for (auto t : *atom_info_) {
          t->deleteEntry();
          t->deallocate(alloc_);
        }
      }
    }

    // alloc_->tidyUp();
    atom_info_->~list();
    eg_->~EpochGuard();
    return true;
  }

  inline void waitAndTidy() {
    if (eg_ == nullptr) {
      eg_ = new atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>{emb_};
    } else {
      eg_ = new (eg_) atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>{emb_};
    }
    eg_->~EpochGuard();
    sg_.waitAndTidy();
    // alloc_->tidyUp();
  }

  inline uint64_t start() {
    auto tc = transaction_counter_.fetch_add(1) + 1;
    if (current_core_ == std::numeric_limits<uint8_t>::max()) {
      current_core_ = sched_getcpu();
    }
    uint64_t core = current_core_;
    tc &= 0x00FFFFFFFFFFFFFF;
    tc |= (core << 56);

    if (atom_info_ == nullptr) {
      atom_info_ = new std::list<TransactionInformationBase<ValueVector, Vector, Allocator>*>();
    } else {
      atom_info_ = new (atom_info_) std::list<TransactionInformationBase<ValueVector, Vector, Allocator>*>();
    }
    abort_transaction_.clear();

    if (eg_ == nullptr) {
      eg_ = new atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>{emb_};
    } else {
      eg_ = new (eg_) atom::EpochGuard<atom::EpochManagerBase<Allocator>, atom::EpochManager<Allocator>>{emb_};
    }
    return sg_.createNode();
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

  inline uint64_t bot(uint64_t transaction) { return 0; }
};
};  // namespace transaction
};  // namespace nofalsenegatives
};  // namespace mv
