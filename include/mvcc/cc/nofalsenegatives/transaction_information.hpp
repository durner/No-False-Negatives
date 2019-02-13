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
#include <atomic>
#include <iostream>
#include <stdint.h>

namespace mv {
namespace nofalsenegatives {
namespace transaction {

template <template <typename> class ValueVector, template <typename> class Vector, typename Allocator>
class TransactionCoordinator;

template <template <typename> class ValueVector, template <typename> class Vector, typename Allocator>
class TransactionInformationBase {
 public:
  virtual void abortWrite(TransactionCoordinator<ValueVector, Vector, Allocator>& tc) = 0;
  virtual void removeChain(TransactionCoordinator<ValueVector, Vector, Allocator>& tc) = 0;
  virtual void deleteEntry() = 0;
  virtual void deallocate(Allocator* alloc) = 0;

  bool isWriteTransaction() { return write_transaction_; };
  uint64_t getOffset() { return offset_; }
  void* getColumn() { return &locked_; }

  TransactionInformationBase(Vector<uint64_t>& locked,
                             Vector<uint64_t>& lsn,
                             uint64_t info,
                             uint64_t offset,
                             uint64_t transaction,
                             bool write_transaction)
      : locked_(locked),
        lsn_(lsn),
        info_(info),
        offset_(offset),
        transaction_(transaction),
        write_transaction_(write_transaction) {}
  virtual ~TransactionInformationBase() {}

 protected:
  Vector<uint64_t>& locked_;
  Vector<uint64_t>& lsn_;
  uint64_t info_;
  uint64_t offset_;
  uint64_t transaction_;
  bool write_transaction_;
};

template <typename MValue,
          typename COA,
          template <typename>
          class ValueVector,
          template <typename>
          class Vector,
          class List,
          typename Allocator>
class WriteTransactionInformation : public TransactionInformationBase<ValueVector, Vector, Allocator> {
 public:
  WriteTransactionInformation(Vector<List*>& rw_table,
                              Vector<uint64_t>& locked,
                              Vector<uint64_t>& lsn,
                              Vector<MValue*>& version_chain,
                              COA coa,
                              uint64_t info,
                              uint64_t offset,
                              uint64_t transaction)
      : TransactionInformationBase<ValueVector, Vector, Allocator>(locked, lsn, info, offset, transaction, true),
        rw_table_(rw_table),
        version_chain_(version_chain),
        coa_(coa){};

 private:
  Vector<List*>& rw_table_;
  Vector<MValue*>& version_chain_;
  COA coa_;

  void deleteEntry() { rw_table_[this->offset_]->erase(this->info_); }
  void removeChain(TransactionCoordinator<ValueVector, Vector, Allocator>& tc) {
    tc.removeWriteChain(version_chain_, this->offset_);
  }
  void abortWrite(TransactionCoordinator<ValueVector, Vector, Allocator>& tc) {
    tc.abortWrite(this->version_chain_, this->offset_, coa_);
  }
  void deallocate(Allocator* alloc) { alloc->deallocate(this, 1); }
};

template <template <typename> class ValueVector, template <typename> class Vector, class List, typename Allocator>
class ReadTransactionInformation : public TransactionInformationBase<ValueVector, Vector, Allocator> {
 public:
  ReadTransactionInformation(Vector<List*>& rw_table,
                             Vector<uint64_t>& locked,
                             Vector<uint64_t>& lsn,
                             uint64_t info,
                             uint64_t offset,
                             uint64_t transaction)
      : TransactionInformationBase<ValueVector, Vector, Allocator>(locked, lsn, info, offset, transaction, false),
        rw_table_(rw_table){};

 private:
  Vector<List*>& rw_table_;

  void deleteEntry() { rw_table_[this->offset_]->erase(this->info_); }
  void removeChain(TransactionCoordinator<ValueVector, Vector, Allocator>& tc) {}
  void abortWrite(TransactionCoordinator<ValueVector, Vector, Allocator>& tc) {}
  void deallocate(Allocator* alloc) { alloc->deallocate(this, 1); }
};

};  // namespace transaction
};  // namespace nofalsenegatives
};  // namespace mv
