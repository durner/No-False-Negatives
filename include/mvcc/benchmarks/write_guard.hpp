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
#include <limits>
#include <stdint.h>

namespace mv {
template <typename TC,
          typename MValue,
          typename Locking,
          typename COW,
          typename COA,
          template <typename>
          class Vector,
          class List>
class WriteGuard {
  TC* tc_;
  Vector<MValue*>& version_chain_;
  Vector<List*>& rw_table_;
  Vector<Locking>& locked_;
  Vector<uint64_t>& lsn_;
  uint64_t offset_;
  uint64_t transaction_;
  uint64_t success_;

 public:
  WriteGuard(TC* tc,
             Vector<MValue*>& version_chain,
             Vector<List*>& rw_table,
             Vector<Locking>& locked,
             Vector<uint64_t>& lsn,
             COW cow,
             COA coa,
             uint64_t offset,
             uint64_t transaction)
      : tc_(tc),
        version_chain_(version_chain),
        rw_table_(rw_table),
        locked_(locked),
        lsn_(lsn),
        offset_(offset),
        transaction_(transaction) {
    success_ = tc_->writeInit(rw_table_, locked_, lsn_, version_chain_, cow, coa, offset_, transaction_);
  }

  bool wasSuccessful() { return (success_ != std::numeric_limits<uint64_t>::max()); }

  template <typename Value, template <typename> class ValueVector>
  void write(Value& val, ValueVector<Value>& column) {
    if (wasSuccessful()) {
      tc_->write(val, column, offset_);
    }
  }

  ~WriteGuard() {
    if (wasSuccessful()) {
      tc_->writeFinish(locked_, lsn_, offset_, success_);
    }
  }
};
};  // namespace mv
