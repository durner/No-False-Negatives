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

#include "ds/atomic_extent_vector.hpp"
#include <gtest/gtest.h>
#include <tbb/tbb.h>

TEST(AtomicExtentVector, Insert) {
  atom::AtomicExtentVector<uint64_t> vector;
  for (uint64_t i = 0; i < 100; i++) {
    vector.push_back(i);
  }
  uint64_t i = 0;
  for (auto l : vector) {
    ASSERT_EQ(l, i);
    i++;
  }
}

TEST(AtomicExtentVector, InsertMultithread) {
  tbb::task_scheduler_init init(16);
  atom::AtomicExtentVector<uint64_t> vector;

  parallel_for(tbb::blocked_range<std::size_t>(0, 1000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      vector.push_back(i);
    }
  });

  uint64_t i = 0;
  for (auto l : vector) {
    i += l;
  }

  ASSERT_EQ(i, (1000 * 999) / 2);
}

TEST(AtomicExtentVector, InsertDelete) {
  atom::AtomicExtentVector<uint64_t> vector;
  uint64_t t = 0;
  uint64_t c = 0;
  for (uint64_t i = 0; i < 10000; i++) {
    if (c % 2 == 0 && i > 0) {
      t -= vector[c - 2];
      vector.erase(c - 2);
      c--;
    }
    vector.push_back(i);
    t += i;
    c++;
  }
  uint64_t i = 0;
  for (auto l : vector) {
    i += l;
  }
  ASSERT_EQ(i, t);
}

TEST(AtomicExtentVector, InsertDeleteMultithreadCount) {
  tbb::task_scheduler_init init(16);
  atom::AtomicExtentVector<uint64_t> vector;

  std::atomic<uint64_t> t(0);
  std::atomic<uint64_t> c(0);

  parallel_for(tbb::blocked_range<std::size_t>(0, 10000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      uint64_t s = 0;
      if (i < c && i % 3 == 0 && c > 0) {
        s = vector[i];
        // other trans might not be finished yet but incremented size count!
        if (s != 0) {
          vector.erase(i);
          t -= s;
        }
      }
      vector.push_back(i);
      t += i;
      c++;
    }
  });

  uint64_t i = 0;
  for (auto l : vector) {
    i += l;
  }

  ASSERT_EQ(i, t);
}

TEST(AtomicExtentVector, InsertDeleteMultithreadSize) {
  tbb::task_scheduler_init init(16);
  atom::AtomicExtentVector<uint64_t> vector;

  std::atomic<uint64_t> t(0);
  std::atomic<uint64_t> c(0);

  parallel_for(tbb::blocked_range<std::size_t>(0, 10000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      if (i < c && i % 3 == 0 && c > 0) {
        uint64_t s = vector[i];

        // other trans might not be finished yet but incremented size count!
        if (s != 0) {
          vector.erase(i);
          t--;
        }
      }
      vector.push_back(i);
      c++;
      t++;
    }
  });

  uint64_t i = 0;
  for (auto l : vector) {
    if (l > 0)
      i++;
    else
      i++;
  }

  ASSERT_EQ(i, t);
}

TEST(AtomicExtentVector, InsertReadMultithread) {
  tbb::task_scheduler_init init(16);
  atom::AtomicExtentVector<uint64_t> vector;

  parallel_for(tbb::blocked_range<std::size_t>(0, 10000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      uint64_t t = 0;
      for (auto l : vector) {
        if (l > 0)
          t++;
        if (t > 100)
          break;
      }
      vector.push_back(i);
    }
  });

  uint64_t i = 0;
  for (auto l : vector) {
    i += l;
  }

  ASSERT_EQ(i, (10000 * 9999) / 2);
}

TEST(AtomicExtentVector, InsertDeleteReadMultithread) {
  tbb::task_scheduler_init init(32);
  atom::AtomicExtentVector<uint64_t> vector;

  std::atomic<uint64_t> t(0);
  std::atomic<uint64_t> counter(0);
  std::atomic<uint64_t> counter_del(0);

  parallel_for(tbb::blocked_range<std::size_t>(0, 10000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      if (i < counter_del && i % 3 == 0 && counter_del > 0) {
        uint64_t s = 0;
        s = vector[i];
        // other trans might not be finished yet but incremented size count!
        if (s != 0 && vector.isAlive(i)) {
          vector.erase(i);
          counter--;
          t -= s;
        }
      }
      vector.push_back(i);
      t += i;
      counter++;
      counter_del++;

      uint64_t c = 0;
      for (auto l : vector) {
        if (l > 0)
          c++;
        if (c > 20)
          break;
      }
    }
  });

  uint64_t i = 0;
  uint64_t c = 0;
  for (auto l : vector) {
    i += l;
    c++;
  }

  ASSERT_EQ(i, t);
  ASSERT_EQ(c, counter);
}
