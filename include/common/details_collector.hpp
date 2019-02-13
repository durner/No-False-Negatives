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
#include <chrono>
#include <iostream>
#include <sstream>

namespace common {
class DetailCollector {
 private:
  std::chrono::system_clock::time_point tx_p_;
  std::chrono::system_clock::time_point commit_p_;
  std::chrono::system_clock::time_point wait_manager_p_;
  std::chrono::system_clock::time_point latency_p_;
  std::chrono::system_clock::time_point thread_start_;

  uint64_t total_time_ = 0;
  uint64_t commits_ = 0;
  uint64_t aborts_ = 0;
  uint64_t not_found_ = 0;
  uint64_t tx_ = 0;
  uint64_t commit_ = 0;
  uint64_t wait_manager_ = 0;
  uint64_t latency_ = 0;

  uint64_t olap_commits_ = 0;
  uint64_t olap_aborts_ = 0;
  uint64_t olap_not_found_ = 0;
  uint64_t olap_tx_ = 0;
  uint64_t olap_commit_ = 0;
  uint64_t olap_wait_manager_ = 0;
  uint64_t olap_latency_ = 0;

 public:
  void merge(common::DetailCollector& dc) {
    total_time_ += dc.total_time_;

    commits_ += dc.commits_;
    aborts_ += dc.aborts_;
    not_found_ += dc.not_found_;
    tx_ += dc.tx_;
    commit_ += dc.commit_;
    wait_manager_ += dc.wait_manager_;
    latency_ += dc.latency_;

    olap_commits_ += dc.olap_commits_;
    olap_aborts_ += dc.olap_aborts_;
    olap_not_found_ += dc.olap_not_found_;
    olap_tx_ += dc.olap_tx_;
    olap_commit_ += dc.olap_commit_;
    olap_wait_manager_ += dc.olap_wait_manager_;
    olap_latency_ += dc.olap_latency_;
  }

  void printStatistics() const {
    std::cout << "Time needed (netto): "
              << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(total_time_)).count()
              << "ms" << std::endl;
    std::cout << std::endl << std::endl;

    if (commits_ != 0) {
      std::cout << "OLTP Commits: " << commits_ << std::endl;
      std::cout << "OLTP Aborts: " << aborts_ << std::endl;
      std::cout << "OLTP Not found: " << not_found_ << std::endl;
      std::cout << "OLTP Transaction Time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(tx_)).count() << "ms"
                << std::endl;
      std::cout << "OLTP Commit Time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(commit_)).count()
                << "ms" << std::endl;
      std::cout
          << "OLTP Wait Manager Time: "
          << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(wait_manager_)).count()
          << "ms" << std::endl;
      std::cout << "OLTP Latency Time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(latency_)).count()
                << "ms" << std::endl
                << std::endl
                << std::endl;
    }

    if (olap_commits_ != 0) {
      std::cout << "OLAP Commits: " << olap_commits_ << std::endl;
      std::cout << "OLAP Aborts: " << olap_aborts_ << std::endl;
      std::cout << "OLAP Not found: " << olap_not_found_ << std::endl;

      std::cout << "OLAP Transaction Time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(olap_tx_)).count()
                << "ms" << std::endl;
      std::cout << "OLAP Commit Time: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(olap_commit_)).count()
                << "ms" << std::endl;
      std::cout
          << "OLAP Wait Manager Time: "
          << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(olap_wait_manager_)).count()
          << "ms" << std::endl;
      std::cout
          << "OLAP Latency Time: "
          << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(olap_latency_)).count()
          << "ms" << std::endl
          << std::endl
          << std::endl;
    }
  }

  void writeCSV(std::stringstream& log) {
    log << ";" << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(total_time_)).count()
        << ";" << commits_ << ";" << not_found_ << ";" << aborts_ << ";" << olap_commits_ << ";" << olap_not_found_
        << ";" << olap_aborts_ << ";"
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(tx_)).count() << ";"
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(commit_)).count() << ";"
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(wait_manager_)).count() << ";"
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(latency_)).count() << ";"
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(olap_tx_)).count() << ";"
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(olap_commit_)).count() << ";"
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(olap_wait_manager_)).count()
        << ";"
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::nanoseconds(olap_latency_)).count();
  }

  inline void commit(bool olap = false) { ((olap) ? olap_commits_ : commits_)++; }

  inline void abort(bool olap = false) { ((olap) ? olap_aborts_ : aborts_)++; }

  inline void notFound(bool olap = false) { ((olap) ? olap_not_found_ : not_found_)++; }

  inline void startWorker() { thread_start_ = std::chrono::system_clock::now(); }

  inline void stopWorker() {
    auto ltx =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - thread_start_).count();
    total_time_ += ltx;
  }

  inline void startTX() { tx_p_ = std::chrono::system_clock::now(); }

  inline uint64_t stopTX(bool olap = false) {
    auto ltx = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - tx_p_).count();
    ((olap) ? olap_tx_ : tx_) += ltx;
    return ltx;
  }

  inline void startCommit() { commit_p_ = std::chrono::system_clock::now(); }

  inline void stopCommit(bool olap = false) {
    ((olap) ? olap_commit_ : commit_) +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - commit_p_).count();
  }

  inline void startWaitManager() { wait_manager_p_ = std::chrono::system_clock::now(); }

  inline void stopWaitManager(bool olap = false) {
    ((olap) ? olap_wait_manager_ : wait_manager_) +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - wait_manager_p_)
            .count();
  }

  inline void startLatency() { latency_p_ = std::chrono::system_clock::now(); }

  inline void stopLatency(uint64_t tx_time, bool olap = false) {
    ((olap) ? olap_latency_ : latency_) +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - latency_p_).count() -
        tx_time;
  }
};
};  // namespace common
