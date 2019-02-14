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
#include "ds/atomic_unordered_map.hpp"
#include <atomic>
#include <iostream>
#include <stdint.h>

namespace tictoc {
namespace transaction {

template <typename Allocator>
class TransactionCoordinator;

template <typename Allocator>
class TransactionInformationBase {
 public:
  virtual void writeValue(TransactionCoordinator<Allocator>& tc, uint64_t commit_ts) = 0;
  virtual void writeCommit(TransactionCoordinator<Allocator>& tc, uint64_t commit_ts) = 0;
  virtual void deallocate(Allocator* alloc) = 0;
  bool isWriteTransaction() { return write_transaction_; };
  virtual void lockValue(TransactionCoordinator<Allocator>& tc) = 0;
  virtual void unlockValue(TransactionCoordinator<Allocator>& tc) = 0;
  virtual char* getValue() = 0;

  virtual uint64_t getCurrentTimeStamp() = 0;
  virtual uint64_t getPreviousCurrentTimeStamp() = 0;
  uint64_t getTimeStamp() { return timestamp_; }

  virtual bool sameDataElem(void* column, uint64_t offset) = 0;
  virtual bool compare_and_swap(uint64_t v1, uint64_t v2) = 0;
  virtual bool replacePrevious(uint64_t v1, uint64_t v1_prime) = 0;
  virtual void* getColumn() = 0;
  virtual uint64_t getOffset() = 0;

  TransactionInformationBase(uint64_t timestamp, bool write_transaction)
      : write_transaction_(write_transaction), timestamp_(timestamp) {}
  virtual ~TransactionInformationBase() {}

 private:
  bool write_transaction_;

 protected:
  uint64_t timestamp_;
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
                              ValueVector<Value>& column,
                              Vector<List<uint64_t>*>& rw_table,
                              Vector<uint64_t>& locked,
                              Vector<uint64_t>& lsn,
                              uint64_t offset,
                              uint64_t transaction,
                              uint64_t timestamp)
      : TransactionInformationBase<Allocator>(timestamp, true),
        data_(data),
        column_(column),
        rw_table_(rw_table),
        locked_(locked),
        lsn_(lsn),
        offset_(offset),
        transaction_(transaction){};

  void writeValue(TransactionCoordinator<Allocator>& tc, uint64_t commit_ts);
  void writeCommit(TransactionCoordinator<Allocator>& tc, uint64_t commit_ts);
  char* getValue() { return reinterpret_cast<char*>(&data_); }
  void deallocate(Allocator* alloc);
  void lockValue(TransactionCoordinator<Allocator>& tc);
  void unlockValue(TransactionCoordinator<Allocator>& tc);
  uint64_t getCurrentTimeStamp();
  uint64_t getPreviousCurrentTimeStamp();
  bool sameDataElem(void* column, uint64_t offset);
  bool compare_and_swap(uint64_t v1, uint64_t v2);
  bool replacePrevious(uint64_t v1, uint64_t v1_prime);

  void* getColumn() { return &locked_; };
  uint64_t getOffset() { return offset_; }

 private:
  Value data_;
  ValueVector<Value>& column_;
  Vector<List<uint64_t>*>& rw_table_;
  Vector<uint64_t>& locked_;
  Vector<uint64_t>& lsn_;
  uint64_t offset_;
  uint64_t transaction_;
};

template <template <typename> class Vector, template <typename> class List, typename Allocator>
class ReadTransactionInformation : public TransactionInformationBase<Allocator> {
 public:
  ReadTransactionInformation(Vector<List<uint64_t>*>& rw_table,
                             Vector<uint64_t>& locked,
                             Vector<uint64_t>& lsn,
                             uint64_t offset,
                             uint64_t transaction,
                             uint64_t timestamp)
      : TransactionInformationBase<Allocator>(timestamp, false),
        rw_table_(rw_table),
        locked_(locked),
        lsn_(lsn),
        offset_(offset),
        transaction_(transaction){};

  void writeValue(TransactionCoordinator<Allocator>& tc, uint64_t commit_ts) {}
  void writeCommit(TransactionCoordinator<Allocator>& tc, uint64_t commit_ts) {}
  char* getValue() { return nullptr; }
  void deallocate(Allocator* alloc);
  void lockValue(TransactionCoordinator<Allocator>& tc);
  void unlockValue(TransactionCoordinator<Allocator>& tc);
  uint64_t getCurrentTimeStamp();
  uint64_t getPreviousCurrentTimeStamp();
  bool sameDataElem(void* column, uint64_t offset);
  bool compare_and_swap(uint64_t v1, uint64_t v2);
  bool replacePrevious(uint64_t v1, uint64_t v1_prime);

  void* getColumn() { return &locked_; };
  uint64_t getOffset() { return offset_; }

 private:
  Vector<List<uint64_t>*>& rw_table_;
  Vector<uint64_t>& locked_;
  Vector<uint64_t>& lsn_;
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
uint64_t WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::getCurrentTimeStamp() {
  return locked_[offset_];
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
uint64_t ReadTransactionInformation<Vector, List, Allocator>::getCurrentTimeStamp() {
  return locked_[offset_];
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
uint64_t WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::getPreviousCurrentTimeStamp() {
  return lsn_[offset_];
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
uint64_t ReadTransactionInformation<Vector, List, Allocator>::getPreviousCurrentTimeStamp() {
  return lsn_[offset_];
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
bool WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::sameDataElem(void* locked,
                                                                                            uint64_t offset) {
  return reinterpret_cast<void*>(&locked_) == locked && offset == offset_;
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
bool ReadTransactionInformation<Vector, List, Allocator>::sameDataElem(void* locked, uint64_t offset) {
  return reinterpret_cast<void*>(&locked_) == locked && offset == offset_;
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
bool WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::compare_and_swap(uint64_t v1,
                                                                                                uint64_t v2) {
  return locked_.compare_exchange(offset_, v1, v2);
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
bool ReadTransactionInformation<Vector, List, Allocator>::compare_and_swap(uint64_t v1, uint64_t v2) {
  return locked_.compare_exchange(offset_, v1, v2);
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
bool WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::replacePrevious(uint64_t v1,
                                                                                               uint64_t v1prime) {
  return lsn_.compare_exchange(offset_, v1prime, v1);
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
bool ReadTransactionInformation<Vector, List, Allocator>::replacePrevious(uint64_t v1, uint64_t v1prime) {
  return lsn_.compare_exchange(offset_, v1prime, v1);
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
  alloc->deallocate(this, 1);
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
void ReadTransactionInformation<Vector, List, Allocator>::deallocate(Allocator* alloc) {
  alloc->deallocate(this, 1);
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
void WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::unlockValue(
    TransactionCoordinator<Allocator>& tc) {
  tc.template unlockValue<Vector>(locked_, offset_);
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
void ReadTransactionInformation<Vector, List, Allocator>::unlockValue(TransactionCoordinator<Allocator>& tc) {
  tc.template unlockValue<Vector>(locked_, offset_);
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
void WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::lockValue(
    TransactionCoordinator<Allocator>& tc) {
  tc.template lockValue<Vector>(locked_, offset_);
}

template <template <typename> class Vector, template <typename> class List, typename Allocator>
void ReadTransactionInformation<Vector, List, Allocator>::lockValue(TransactionCoordinator<Allocator>& tc) {
  tc.template lockValue<Vector>(locked_, offset_);
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
    TransactionCoordinator<Allocator>& tc,
    uint64_t commit_ts) {
  tc.template writePhase<Value, ValueVector>(data_, column_, offset_, transaction_);
}

template <typename Value,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          template <typename>
          class List,
          typename Allocator>
void WriteTransactionInformation<Value, ValueVector, Vector, List, Allocator>::writeCommit(
    TransactionCoordinator<Allocator>& tc,
    uint64_t commit_ts) {
  tc.template writePhaseCommit<Vector>(locked_, lsn_, offset_, transaction_, commit_ts);
}
};  // namespace transaction
};  // namespace tictoc
