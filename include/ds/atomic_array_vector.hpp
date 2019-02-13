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

#define DEBUG 1
#include <atomic>
#include <cstring>
#include <iostream>
#include <vector>
#include <stdint.h>
#include <tbb/spin_mutex.h>

namespace atom {
template <typename Value>
class AtomicArrayVector {
 public:
 private:
  static constexpr uint8_t max_power_size_ = 64;
  std::atomic<Value>* buckets_[max_power_size_];
  uint64_t extend_;
  uint8_t reserved_ = 0;
  std::atomic<uint64_t> size_;
  tbb::spin_mutex mutex_;

 public:
  AtomicArrayVector() : extend_(0), size_(0), mutex_(){};
  AtomicArrayVector(const AtomicArrayVector&) = default;
  ~AtomicArrayVector() {
    for (uint64_t i = 0; i < extend_; i++)
      delete[] buckets_[i];
  }

  inline const uint64_t size() { return size_; }

  inline const uint64_t max_size() { return extend_ > 0 ? 1 << (reserved_ + extend_ - 1) : 0; }

  inline const uint64_t get_segment_base(const uint64_t v) { return v == 0 ? 0 : 1 << (v + reserved_ - 1); }

  inline const uint8_t get_segment_base_offset(const uint64_t n) {
    if (!n)
      return 0;
    uint8_t clz = __builtin_clzl(n);
    clz = (64 - reserved_) > clz ? clz + reserved_ : 64;
    return 64 - clz;
  }

  inline const uint64_t upper_power_of_two(uint64_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
  }

  inline const Value operator[](uint64_t n) {
    uint8_t v = get_segment_base_offset(n);
    return buckets_[v][n - get_segment_base(v)];
  }

  inline Value atomic_replace(uint64_t n, const Value value) {
    uint8_t v = get_segment_base_offset(n);
    n -= get_segment_base(v);
    Value old = buckets_[v][n].exchange(value);
    return old;
  }

  inline uint64_t push_back(const Value value) {
    uint64_t new_n = size_.fetch_add(1);
    while (new_n >= max_size()) {
      resize();
    }
    uint8_t v = get_segment_base_offset(new_n);
    uint64_t n = new_n - get_segment_base(v);
    buckets_[v][n] = value;
    return new_n;
  }

  inline void reserve(std::size_t n) {
    mutex_.lock();
    if (extend_ == 0 && n > 0) {
      n = upper_power_of_two(n);
      buckets_[extend_] = new std::atomic<Value>[n]();
      reserved_ = 64 - __builtin_clzl(n) - 1;
      extend_++;
    }
    mutex_.unlock();
  }

  inline void resize() {
    mutex_.lock();
    if (size_ >= max_size()) {
      uint64_t n = max_size();
      if (extend_ == 0) {
        n = 1;
        reserved_ = 64 - __builtin_clzl(n) - 1;
      }
      buckets_[extend_] = new std::atomic<Value>[n]();
      extend_++;
    }
    mutex_.unlock();
  }
};
};  // namespace atom
