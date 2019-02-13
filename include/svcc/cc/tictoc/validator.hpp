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
#include "svcc/cc/tictoc/transaction_information.hpp"
#include <list>
#include <map>

// #define SGLOGGER 1

namespace tictoc {
namespace serial {

using namespace tictoc;
using namespace tictoc::transaction;

class Validator {
  common::GlobalLogger logger;

  template <typename Allocator>
  bool isInWriteSet(TransactionInformationBase<Allocator>* rt,
                    std::list<TransactionInformationBase<Allocator>*>& set_info) {
    for (auto t : set_info) {
      if (t->isWriteTransaction()) {
        if (t->sameDataElem(rt->getColumn(), rt->getOffset())) {
          return true;
        }
      }
    }
    return false;
  }

 public:
  template <typename TC, typename Allocator>
  bool validate(TC& tc,
                std::list<TransactionInformationBase<Allocator>*>& set_info,
                uint64_t& commit_ts,
                bool has_writer) {
    std::map<std::pair<uint64_t, uint64_t>, TransactionInformationBase<Allocator>*> omap;
    if (has_writer) {
      for (auto t : set_info) {
        if (t->isWriteTransaction())
          omap.insert({{t->getOffset(), reinterpret_cast<uintptr_t>(t->getColumn())}, t});
      }
    }

    for (auto t : omap) {
      t.second->lockValue(tc);
    }

    commit_ts = 0;
    for (auto t : set_info) {
      if (t->isWriteTransaction()) {
        has_writer = true;
        commit_ts = std::max(commit_ts, TC::getReadTS(t->getCurrentTimeStamp()) + 1);
      } else {
        commit_ts = std::max(commit_ts, TC::getWriteTS(t->getTimeStamp()));
      }
    }
    for (auto t : set_info) {
      if (!has_writer || !t->isWriteTransaction()) {
        uint64_t tts = t->getTimeStamp();
        if (TC::getReadTS(tts) < commit_ts) {
          bool success = false;
          do {
            success = true;
            uint64_t v1, v2, v1prime;
            v1 = t->getCurrentTimeStamp();
            v1prime = t->getPreviousCurrentTimeStamp();
            v2 = v1;
            if ((TC::getWriteTS(tts) != TC::getWriteTS(v1) &&
                 !(TC::getWriteTS(tts) == TC::getWriteTS(v1prime) && commit_ts < TC::getWriteTS(v1prime) &&
                   commit_ts >= TC::getWriteTS(tts))) ||
                (TC::getReadTS(v1) <= commit_ts && TC::isLocked(v1) && !isInWriteSet(t, set_info))) {
              return false;
            }

            if (TC::getReadTS(v1) <= commit_ts) {
              uint16_t delta = commit_ts - TC::getWriteTS(v1);
              uint16_t shift = delta - (delta & 0x7FFF);

              // v2.wts = v2.wts + shift
              v2 = 0;
              v2 = TC::getWriteTS(v1);
              v2 += shift;

              // v2.delta = delta - shift
              uint64_t ndelta = delta - shift;
              ndelta <<= 48;
              if (v1 >> 63 == 0)
                v2 |= ndelta;
              else
                v2 |= ndelta | (1ul << 63);

              success = t->compare_and_swap(v1, v2);

              assert(!(isInWriteSet(t, set_info) && (v1 >> 63) == 0 && success));
            }
          } while (!success);
        }
      }
    }
    return true;
  }

  template <typename TC, typename Allocator>
  void unlock(TC& tc, std::list<TransactionInformationBase<Allocator>*>& set_info) {
    std::map<std::pair<uint64_t, uint64_t>, TransactionInformationBase<Allocator>*> omap;
    for (auto t : set_info) {
      if (t->isWriteTransaction())
        omap.insert({{t->getOffset(), reinterpret_cast<uintptr_t>(t->getColumn())}, t});
    }

    for (auto t : omap) {
      t.second->unlockValue(tc);
    }
  }

  void log(const common::LogInfo log_info) { logger.log(log_info); }

  void log(const std::string log_info) { logger.log(log_info); }
};

}  // namespace serial
}  // namespace tictoc
