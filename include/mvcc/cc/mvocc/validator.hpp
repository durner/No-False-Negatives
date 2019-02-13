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

#include "common/global_logger.hpp"
#include "ds/atomic_singly_linked_list.hpp"
#include "mvcc/cc/mvocc/transaction_information.hpp"
#include <list>
#include <map>

// #define SGLOGGER 1

namespace mv {
namespace mvocc {
namespace serial {

using namespace mv;
using namespace mv::mvocc;
using namespace mv::mvocc::transaction;

class Validator {
  common::GlobalLogger logger;

  template <typename UndoBuffer,
            template <typename>
            class ValueVector,
            template <typename>
            class Vector,
            class Map,
            typename Allocator>
  bool isInUndoBuffer(std::list<TransactionInformationBase<ValueVector, Vector, Allocator>*>& set_info,
                      Map& ubl,
                      uint64_t start_ts,
                      uint64_t commit_ts) {
    auto it = ubl.begin();
    auto end = ubl.end();
    while (it != end) {
      if (it->first >= start_ts && it->first <= commit_ts) {
        for (auto e : *(it->second)) {
          for (auto t : set_info) {
            if (!t->isWriteTransaction() && t->sameDataElem(e->column, e->offset)) {
              return true;
            }
          }
        }
      }
      ++it;
    }
    return false;
  }

  template <typename UndoBuffer,
            template <typename>
            class ValueVector,
            template <typename>
            class Vector,
            class Map,
            typename Allocator>
  void addToUndoBuffer(std::list<TransactionInformationBase<ValueVector, Vector, Allocator>*>& set_info,
                       Map& ubl,
                       uint64_t commit_ts) {
    auto vec = new std::vector<UndoBuffer*>();
    for (auto t : set_info) {
      if (t->isWriteTransaction()) {
        UndoBuffer* undo = new UndoBuffer{t->getColumn(), t->getOffset(), commit_ts};
        vec->push_back(undo);
      }
    }
    ubl.emplace(commit_ts, vec);
  }

 public:
  template <typename UndoBuffer,
            template <typename>
            class ValueVector,
            template <typename>
            class Vector,
            class Map,
            typename Allocator>
  bool validate(std::list<TransactionInformationBase<ValueVector, Vector, Allocator>*>& set_info,
                Map& ubl,
                uint64_t start_ts,
                uint64_t commit_ts) {
    bool abort = isInUndoBuffer<UndoBuffer>(set_info, ubl, start_ts, commit_ts);
    if (abort)
      return false;

    addToUndoBuffer<UndoBuffer>(set_info, ubl, commit_ts);

    return true;
  }

  void log(const common::LogInfo log_info) { logger.log(log_info); }

  void log(const std::string log_info) { logger.log(log_info); }
};

}  // namespace serial
}  // namespace mvocc
};  // namespace mv
