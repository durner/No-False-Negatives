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
#include <stdint.h>

namespace sv {
template <typename TC, typename Locking, template <typename> class Vector, template <typename> class List>
class ReadGuard {
  TC* tc_;
  Vector<uint64_t>& lsn_column_;
  Vector<List<uint64_t>*>& rw_table_;
  Vector<Locking>& locked_;
  uint64_t offset_;
  uint64_t transaction_;
  uint64_t prv_;

 public:
  ReadGuard(TC* tc,
            Vector<uint64_t>& lsn_column,
            Vector<List<uint64_t>*>& rw_table,
            Vector<Locking>& locked,
            uint64_t offset,
            uint64_t transaction)
      : tc_(tc),
        lsn_column_(lsn_column),
        rw_table_(rw_table),
        locked_(locked),
        offset_(offset),
        transaction_(transaction),
        prv_(0) {
    prv_ = tc_->read(lsn_column_, rw_table_, locked_, offset_, transaction_);
  }

  bool wasSuccessful() { return prv_ > 0; }

  ~ReadGuard() {
    if (prv_ > 0) {
      tc_->readUndo(prv_, lsn_column_, rw_table_, locked_, offset_, transaction_);
    }
  }
};
};  // namespace sv
