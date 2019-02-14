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

#include <iostream>
#include <thread>
#include <pthread.h>
#include <stdint.h>

namespace common {
class ThreadHandler {
 private:
  std::thread this_thread_;
  uint16_t core_id_;

 public:
  ThreadHandler(uint16_t pinned_core) : this_thread_(), core_id_(pinned_core) {}

  ThreadHandler(const ThreadHandler& other) = delete;
  ThreadHandler(ThreadHandler&& other) = delete;
  ThreadHandler& operator=(const ThreadHandler& other) = delete;
  ThreadHandler& operator=(ThreadHandler&& other) = delete;

  template <typename Function, typename... Args>
  inline void run(Function&& f, Args&&... args) {
    this_thread_ = std::thread(std::forward<Function>(f), std::forward<Args>(args)...);

    cpu_set_t cpuset;
    pthread_t pthread = this_thread_.native_handle();

    CPU_ZERO(&cpuset);
    CPU_SET(core_id_, &cpuset);

    int s = pthread_setaffinity_np(pthread, sizeof(cpu_set_t), &cpuset);
    if (s != 0) {
      std::cout << "Error pthread_setaffinity_np" << std::endl;
    }

    s = pthread_getaffinity_np(pthread, sizeof(cpu_set_t), &cpuset);
    if (s != 0) {
      std::cout << "Error pthread_getaffinity_np" << std::endl;
    }
  }

  inline void join() { this_thread_.join(); }
};
};  // namespace common
