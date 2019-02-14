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

#pragma once

#include "ds/atomic_data_structures_decl.hpp"
#include "ds/atomic_unordered_hashtable.hpp"

namespace atom {

template <typename Value, typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedMultiMap : public AtomicUnorderedHashtable<Bucket, Key, Allocator, Size> {
 public:
  using iterator = AtomicUnorderedMapIterator<Value, Key, Bucket, Allocator, Size>;
  using unsafe_iterator = UnsafeAtomicUnorderedMapIterator<Value, Key, Bucket, Allocator, Size>;
  using Base = AtomicUnorderedHashtable<Bucket, Key, Allocator, Size>;
  friend class AtomicUnorderedMapIterator<Value, Key, Bucket, Allocator, Size>;
  friend class UnsafeAtomicUnorderedMapIterator<Value, Key, Bucket, Allocator, Size>;

  AtomicUnorderedMultiMap(uint64_t buildSize, Allocator* alloc, typename Base::EMB* em)
      : AtomicUnorderedHashtable<Bucket, Key, Allocator, Size>(buildSize, alloc, em){};

  inline bool lookup(const Key key, std::vector<Value>& val) const {
    uint64_t hash = Base::hashKey(key) % Base::max_size_;

    EpochGuard<typename Base::EMB, typename Base::EM> eg{Base::em_};
    Bucket* elem = Base::buckets_[hash].load();
    while (elem != nullptr) {
      if (elem->key == key) {
        val.push_back(elem->val);
      }
      Bucket* elem_next = elem->next.load();
      elem = elem_next;
    }
    return (val.size() > 0);
  }

  template <typename T>
  inline bool insert(const Key key, T&& val) {
    uint64_t hash = Base::hashKey(key) % Base::max_size_;
    Bucket* old;
    auto id = Base::lock(hash);
    void* addr = Base::alloc_->template allocate<Bucket>(1);
    Bucket* elem = new (addr) Bucket(key, std::forward<T>(val));
    do {
      old = Base::buckets_[hash].load();
      elem->next = old;
    } while (!Base::buckets_[hash].compare_exchange_weak(old, elem));
    Base::unlock(hash, id);
    if (Size)
      Base::size_++;
    return true;
  }

  inline bool erase(const Key key, Value& val) {
    uint64_t hash = hashKey(key) % Base::max_size_;
    std::atomic<Bucket*> prv;
    std::atomic<Bucket*> elem;
    bool check = false;
    bool keyfound = false;
    auto id = Base::lock(hash);
    EpochGuard<typename Base::EMB, typename Base::EM> eg{Base::em_};
    do {
      elem.store(Base::buckets_[hash]);
      prv.store(nullptr);
      while (elem != nullptr && !keyfound) {
        Bucket* cur = elem;
        if (cur->key == key && cur->val == val) {
          keyfound = true;
          Bucket* ne = cur->next;
          if (prv != nullptr) {
            // load is fine here since this only could damage other write
            // operations but since we are in the write mutex this cant happen
            check = prv.load()->next.compare_exchange_strong(cur, ne);
          } else {
            check = Base::buckets_[hash].compare_exchange_strong(cur, ne);
          }
        }
        prv.store(cur);
        elem.store(cur->next);
      }
    } while (!check && keyfound);
    unlock(hash, id);
    if (keyfound) {
      eg.add(prv);
      if (Size)
        Base::size_--;
    }
    return keyfound;
  }

  inline uint64_t hashKey(const Key key) { return Base::hashKey(key) % Base::max_size_; }
  inline iterator begin() { return iterator(*this, 0, Base::em_); }
  inline iterator end() { return iterator(*this, Base::max_size_, Base::em_); }
};

template <typename Value, typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedMultiMapIterator : public std::iterator<std::forward_iterator_tag, Value> {
  AtomicUnorderedMultiMap<Value, Key, Bucket, Allocator, Size>& map_;
  uint64_t bucket_position_;
  Bucket* cur_bucket_;
  EpochGuard<typename AtomicUnorderedMultiMap<Value, Key, Bucket, Allocator, Size>::Base::EMB,
             typename AtomicUnorderedMultiMap<Value, Key, Bucket, Allocator, Size>::Base::EM>
      eg_;

 public:
  AtomicUnorderedMultiMapIterator(AtomicUnorderedMultiMap<Value, Key, Bucket, Allocator, Size>& map,
                                  uint64_t bucket_position,
                                  typename AtomicUnorderedMultiMap<Value, Key, Bucket, Allocator, Size>::Base::EMB* em)
      : map_(map), bucket_position_(bucket_position), cur_bucket_(nullptr), eg_(em) {
    if (bucket_position_ < map_.max_size_) {
      cur_bucket_ = map.buckets_[bucket_position].load();
      if (cur_bucket_ == nullptr)
        operator++();
    }
  }

  AtomicUnorderedMultiMapIterator(const AtomicUnorderedMultiMapIterator& it)
      : map_(it.map_), bucket_position_(it.bucket_position_), cur_bucket_(it.cur_bucket_), eg_(it.eg_) {}

  inline AtomicUnorderedMultiMapIterator& operator++() {
    Bucket* cur_bucket_next = nullptr;
    if (cur_bucket_ != nullptr) {
      cur_bucket_next = cur_bucket_->next.load();
      cur_bucket_ = cur_bucket_next;
    }
    while (cur_bucket_ == nullptr) {
      ++bucket_position_;
      if (bucket_position_ >= map_.max_size_)
        break;
      cur_bucket_ = map_.buckets_[bucket_position_].load();
    }
    return *this;
  }

  inline AtomicUnorderedMultiMapIterator operator++(int) {
    AtomicUnorderedMultiMapIterator tmp(*this);
    operator++();
    return tmp;
  }

  inline bool operator==(const AtomicUnorderedMultiMapIterator& rhs) const {
    return &map_ == &rhs.map_ && cur_bucket_ == rhs.cur_bucket_;
  }

  inline bool operator!=(const AtomicUnorderedMultiMapIterator& rhs) const { return !(*this == rhs); }

  inline Value operator*() const { return cur_bucket_->val; }

  inline uint64_t getKey() const { return cur_bucket_->key; }
};
};  // namespace atom
