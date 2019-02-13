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
#include <functional>
#include <iostream>
#include <limits>
#include <utility>
#include <stdint.h>

namespace mv {
template <typename TC,
          typename MValue,
          typename Locking,
          template <typename>
          class Vector,
          class List,
          bool ReadOnly = false>
class ReadGuard {
  TC* tc_;
  Vector<MValue*>& version_chain_;
  Vector<List*>& rw_table_;
  Vector<Locking>& locked_;
  Vector<uint64_t>& lsn_column_;
  uint64_t offset_;
  uint64_t transaction_;
  uint64_t id_;
  MValue* ptr_;
  uint64_t version_;
  uint64_t prv_;

 public:
  ReadGuard(TC* tc,
            Vector<MValue*>& version_chain,
            Vector<List*>& rw_table,
            Vector<Locking>& locked,
            Vector<uint64_t>& lsn_column,
            uint64_t offset,
            uint64_t transaction)
      : tc_(tc),
        version_chain_(version_chain),
        rw_table_(rw_table),
        locked_(locked),
        lsn_column_(lsn_column),
        offset_(offset),
        transaction_(transaction) {
    if (ReadOnly) {
      tc_->waitSafeRead();
    } else {
      prv_ = tc_->template readVersion<MValue, ReadOnly>(rw_table_, locked_, lsn_column_, version_chain_, id_, ptr_,
                                                         offset_, transaction_);
    }
  }

  bool wasSuccessful() { return prv_ > 0; }

  template <typename Value, template <typename> class ValueVector, typename Accessor>
  void read(Value& val, ValueVector<Value>& column, Accessor acc) {
    if (wasSuccessful()) {
      tc_->readValue(val, column, acc, ptr_, offset_, transaction_);
    }
  }

  template <typename Value, template <typename> class ValueVector, typename Accessor>
  inline bool readOLAP(Value& val, ValueVector<Value>& column, Accessor acc, uint64_t offset) {
    prv_ = tc_->template readVersion<MValue, ReadOnly>(rw_table_, locked_, lsn_column_, version_chain_, id_, ptr_,
                                                       offset, transaction_);
    if (!wasSuccessful()) {
      std::cout << "wtf" << std::endl;
      return false;
    }

    tc_->readValue(val, column, acc, ptr_, offset, transaction_);
    tc_->template readFinish<MValue, ReadOnly>(id_, prv_, rw_table_, locked_, lsn_column_, version_chain_, offset,
                                               transaction_);
    return true;
  }

  ~ReadGuard() {
    if (!ReadOnly) {
      if (wasSuccessful()) {
        tc_->template readFinish<MValue, ReadOnly>(id_, prv_, rw_table_, locked_, lsn_column_, version_chain_, offset_,
                                                   transaction_);
      }
    }
  }
};
};  // namespace mv
