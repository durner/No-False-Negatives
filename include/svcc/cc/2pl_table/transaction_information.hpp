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
#include "ds/atomic_unordered_map.hpp"
#include "svcc/cc/2pl_table/lock_manager.hpp"
#include <atomic>
#include <iostream>
#include <stdint.h>

namespace twopl_table {
namespace transaction {

using namespace twopl_table;
using namespace twopl_table::transaction;

template <typename Allocator>
class TransactionCoordinator;

template <typename Allocator>
class TransactionInformationBase {
 public:
  virtual void deleteFromRWTable() = 0;
  virtual void writeValue(TransactionCoordinator<Allocator>& tc) = 0;
  virtual bool isAbort() = 0;
  virtual void unlock(LockManager<Allocator>* lm) = 0;
  virtual void deallocate(Allocator* alloc) = 0;
  bool isWriteTransaction() { return write_transaction_; };

  TransactionInformationBase(bool write_transaction) : write_transaction_(write_transaction) {}
  virtual ~TransactionInformationBase() {}

 private:
  bool write_transaction_;
};

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
class WriteTransactionInformation : public TransactionInformationBase<Allocator> {
 public:
  WriteTransactionInformation(const Value& data,
                              const Value& data_prv,
                              ValueVector<Value>& column,
                              Vector<uint64_t>& lsn_column,
                              Vector<std::pair<uint64_t, std::set<uint64_t>>*>& locking,
                              Vector<List<uint64_t>*>& rw_table,
                              uint64_t lsn,
                              uint64_t offset,
                              uint64_t transaction,
                              bool abort)
      : TransactionInformationBase<Allocator>(true),
        data_(data),
        data_prv_(data_prv),
        column_(column),
        lsn_column_(lsn_column),
        locking_(locking),
        rw_table_(rw_table),
        lsn_(lsn),
        offset_(offset),
        transaction_(transaction),
        abort_(abort){};

  void deleteFromRWTable();
  void writeValue(TransactionCoordinator<Allocator>& tc);
  bool isAbort();
  void deallocate(Allocator* alloc);
  void unlock(LockManager<Allocator>* lm);

 private:
  Value data_;
  Value data_prv_;
  ValueVector<Value>& column_;
  Vector<uint64_t>& lsn_column_;
  Vector<std::pair<uint64_t, std::set<uint64_t>>*>& locking_;
  Vector<List<uint64_t>*>& rw_table_;
  uint64_t lsn_;
  uint64_t offset_;
  uint64_t transaction_;
  bool abort_;
};

template <template <typename> class Vector, template <typename> class List, typename Allocator>
class ReadTransactionInformation : public TransactionInformationBase<Allocator> {
 public:
  ReadTransactionInformation(Vector<List<uint64_t>*>& rw_table,
                             Vector<std::pair<uint64_t, std::set<uint64_t>>*>& locking,
                             uint64_t lsn,
                             uint64_t offset,
                             uint64_t transaction)
      : TransactionInformationBase<Allocator>(false),
        rw_table_(rw_table),
        locking_(locking),
        lsn_(lsn),
        offset_(offset),
        transaction_(transaction){};
  void deleteFromRWTable();
  void writeValue(TransactionCoordinator<Allocator>& tc) {}
  bool isAbort() { return false; }
  void unlock(LockManager<Allocator>* lm);
  void deallocate(Allocator* alloc);

 private:
  Vector<List<uint64_t>*>& rw_table_;
  Vector<std::pair<uint64_t, std::set<uint64_t>>*>& locking_;
  uint64_t lsn_;
  uint64_t offset_;
  uint64_t transaction_;
};

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
void WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::deleteFromRWTable() {
  // std::cout << "Write: " << lsn_ << ", success: " <<  << std::endl;
  // rw_table_[offset_]->erase(lsn_);
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
void ReadTransactionInformation<Vector, List, Allocator>::deleteFromRWTable() {
  // std::cout << "Read: " << lsn_ << ", success: " << rw_table_[offset_]->erase(lsn_) << std::endl;
  // rw_table_[offset_]->erase(lsn_);
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
void WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::deallocate(Allocator* alloc) {
  // std::cout << lsn_ << std::endl;
  alloc->deallocate(this, 1);
  // delete this;
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
void ReadTransactionInformation<Vector, List, Allocator>::deallocate(Allocator* alloc) {
  alloc->deallocate(this, 1);
  // std::cout << lsn_ << std::endl;
  // delete this;
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
void WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::unlock(LockManager<Allocator>* lm) {
  lm->unlock(transaction_, locking_, offset_);
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
void ReadTransactionInformation<Vector, List, Allocator>::unlock(LockManager<Allocator>* lm) {
  lm->unlock(transaction_, locking_, offset_);
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
void WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::writeValue(
    TransactionCoordinator<Allocator>& tc) {
  tc.template writeValue<Value, ValueVector, Vector, List, true>(data_prv_, column_, lsn_column_, rw_table_, locking_,
                                                                 offset_, transaction_);
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
bool WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::isAbort() {
  return abort_;
}

};  // namespace transaction
};  // namespace twopl_table
