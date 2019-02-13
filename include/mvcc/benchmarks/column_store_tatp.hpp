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
#include "common/details_collector.hpp"
#include "common/optimistic_predicate_locking.hpp"
#include "ds/atomic_extent_vector.hpp"
#include "ds/atomic_singly_linked_list.hpp"
#include "ds/atomic_unordered_map.hpp"
#include "mvcc/benchmarks/read_guard.hpp"
#include "mvcc/benchmarks/write_guard.hpp"
#include <iomanip>
#include <memory>
#include <random>
#include <tuple>
#include <unordered_set>
#include <vector>
#include <stdint.h>
#include <tbb/spin_mutex.h>
#include <unistd.h>

namespace mv {
namespace tatp {
template <unsigned int t>
struct alignas(8) StringStruct {
  char string[t];

  bool operator==(StringStruct<t> other) {
    for (unsigned int i = 0; i < t; i++) {
      if (other.string[i] != string[i])
        return false;
    }
    return true;
  }
};

struct VersionSubscriber {
  uint64_t s_id;
  StringStruct<15> sub_nbr;

  bool bit_1;
  bool bit_2;
  bool bit_3;
  bool bit_4;
  bool bit_5;
  bool bit_6;
  bool bit_7;
  bool bit_8;
  bool bit_9;
  bool bit_10;

  uint8_t hex_1;
  uint8_t hex_2;
  uint8_t hex_3;
  uint8_t hex_4;
  uint8_t hex_5;
  uint8_t hex_6;
  uint8_t hex_7;
  uint8_t hex_8;
  uint8_t hex_9;
  uint8_t hex_10;

  uint8_t byte2_1;
  uint8_t byte2_2;
  uint8_t byte2_3;
  uint8_t byte2_4;
  uint8_t byte2_5;
  uint8_t byte2_6;
  uint8_t byte2_7;
  uint8_t byte2_8;
  uint8_t byte2_9;
  uint8_t byte2_10;

  uint32_t msc_location;
  uint32_t vlr_location;

  uint64_t transaction;
  uint64_t epoch;
  bool commited;
  VersionSubscriber* nxt;
  VersionSubscriber* prv;
};

struct VersionAccess_Info {
  uint64_t s_id;
  uint8_t ai_type;
  uint8_t data1;
  uint8_t data2;
  StringStruct<3> data3;
  StringStruct<5> data4;

  uint64_t transaction;
  uint64_t epoch;
  bool commited;
  VersionAccess_Info* nxt;
  VersionAccess_Info* prv;
};

struct VersionSpecial_Facility {
  uint64_t s_id;
  uint8_t sf_type;
  bool is_active;
  uint8_t error_cntrl;
  uint8_t data_a;
  StringStruct<5> data_b;

  uint64_t transaction;
  uint64_t epoch;
  bool commited;
  VersionSpecial_Facility* nxt;
  VersionSpecial_Facility* prv;
};

struct VersionCall_Forwarding {
  uint64_t s_id;
  uint8_t sf_type;
  uint8_t start_time;
  uint8_t end_time;
  StringStruct<15> numberx;

  uint64_t transaction;
  uint64_t epoch;
  bool commited;
  VersionCall_Forwarding* nxt;
  VersionCall_Forwarding* prv;
};

template <typename Locking = uint64_t>
struct Subscriber {
  atom::ExtentVector<uint64_t> s_id;
  atom::ExtentVector<StringStruct<15>> sub_nbr;

  atom::ExtentVector<bool> bit_1;
  atom::ExtentVector<bool> bit_2;
  atom::ExtentVector<bool> bit_3;
  atom::ExtentVector<bool> bit_4;
  atom::ExtentVector<bool> bit_5;
  atom::ExtentVector<bool> bit_6;
  atom::ExtentVector<bool> bit_7;
  atom::ExtentVector<bool> bit_8;
  atom::ExtentVector<bool> bit_9;
  atom::ExtentVector<bool> bit_10;

  atom::ExtentVector<uint8_t> hex_1;
  atom::ExtentVector<uint8_t> hex_2;
  atom::ExtentVector<uint8_t> hex_3;
  atom::ExtentVector<uint8_t> hex_4;
  atom::ExtentVector<uint8_t> hex_5;
  atom::ExtentVector<uint8_t> hex_6;
  atom::ExtentVector<uint8_t> hex_7;
  atom::ExtentVector<uint8_t> hex_8;
  atom::ExtentVector<uint8_t> hex_9;
  atom::ExtentVector<uint8_t> hex_10;

  atom::ExtentVector<uint8_t> byte2_1;
  atom::ExtentVector<uint8_t> byte2_2;
  atom::ExtentVector<uint8_t> byte2_3;
  atom::ExtentVector<uint8_t> byte2_4;
  atom::ExtentVector<uint8_t> byte2_5;
  atom::ExtentVector<uint8_t> byte2_6;
  atom::ExtentVector<uint8_t> byte2_7;
  atom::ExtentVector<uint8_t> byte2_8;
  atom::ExtentVector<uint8_t> byte2_9;
  atom::ExtentVector<uint8_t> byte2_10;

  atom::ExtentVector<uint32_t> msc_location;
  atom::ExtentVector<uint32_t> vlr_location;

  atom::AtomicExtentVector<uint64_t> lsn;

  atom::AtomicExtentVector<VersionSubscriber*> version_chain;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> rw_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;

  static void copyBackOnAbort(Subscriber& s, uint64_t offset, VersionSubscriber* vs) {
    s.s_id.replace(offset, vs->s_id);
    s.sub_nbr.replace(offset, vs->sub_nbr);
    s.bit_1.replace(offset, vs->bit_1);
    s.bit_2.replace(offset, vs->bit_2);
    s.bit_3.replace(offset, vs->bit_3);
    s.bit_4.replace(offset, vs->bit_4);
    s.bit_5.replace(offset, vs->bit_5);
    s.bit_6.replace(offset, vs->bit_6);
    s.bit_7.replace(offset, vs->bit_7);
    s.bit_8.replace(offset, vs->bit_8);
    s.bit_9.replace(offset, vs->bit_9);
    s.bit_10.replace(offset, vs->bit_10);
    s.hex_1.replace(offset, vs->hex_1);
    s.hex_2.replace(offset, vs->hex_2);
    s.hex_3.replace(offset, vs->hex_3);
    s.hex_4.replace(offset, vs->hex_4);
    s.hex_5.replace(offset, vs->hex_5);
    s.hex_6.replace(offset, vs->hex_6);
    s.hex_7.replace(offset, vs->hex_7);
    s.hex_8.replace(offset, vs->hex_8);
    s.hex_9.replace(offset, vs->hex_9);
    s.hex_10.replace(offset, vs->hex_10);
    s.byte2_1.replace(offset, vs->byte2_1);
    s.byte2_2.replace(offset, vs->byte2_2);
    s.byte2_3.replace(offset, vs->byte2_3);
    s.byte2_4.replace(offset, vs->byte2_4);
    s.byte2_5.replace(offset, vs->byte2_5);
    s.byte2_6.replace(offset, vs->byte2_6);
    s.byte2_7.replace(offset, vs->byte2_7);
    s.byte2_8.replace(offset, vs->byte2_8);
    s.byte2_9.replace(offset, vs->byte2_9);
    s.byte2_10.replace(offset, vs->byte2_10);
    s.msc_location.replace(offset, vs->msc_location);
    s.vlr_location.replace(offset, vs->vlr_location);
  }

  static void copyOnWrite(Subscriber& s, uint64_t offset, VersionSubscriber* vs) {
    vs->s_id = s.s_id[offset];
    vs->sub_nbr = s.sub_nbr[offset];
    vs->bit_1 = s.bit_1[offset];
    vs->bit_2 = s.bit_2[offset];
    vs->bit_3 = s.bit_3[offset];
    vs->bit_4 = s.bit_4[offset];
    vs->bit_5 = s.bit_5[offset];
    vs->bit_6 = s.bit_6[offset];
    vs->bit_7 = s.bit_7[offset];
    vs->bit_8 = s.bit_8[offset];
    vs->bit_9 = s.bit_9[offset];
    vs->bit_10 = s.bit_10[offset];
    vs->hex_1 = s.hex_1[offset];
    vs->hex_2 = s.hex_2[offset];
    vs->hex_3 = s.hex_3[offset];
    vs->hex_4 = s.hex_4[offset];
    vs->hex_5 = s.hex_5[offset];
    vs->hex_6 = s.hex_6[offset];
    vs->hex_7 = s.hex_7[offset];
    vs->hex_8 = s.hex_8[offset];
    vs->hex_9 = s.hex_9[offset];
    vs->hex_10 = s.hex_10[offset];
    vs->byte2_1 = s.byte2_1[offset];
    vs->byte2_2 = s.byte2_2[offset];
    vs->byte2_3 = s.byte2_3[offset];
    vs->byte2_4 = s.byte2_4[offset];
    vs->byte2_5 = s.byte2_5[offset];
    vs->byte2_6 = s.byte2_6[offset];
    vs->byte2_7 = s.byte2_7[offset];
    vs->byte2_8 = s.byte2_8[offset];
    vs->byte2_9 = s.byte2_9[offset];
    vs->byte2_10 = s.byte2_10[offset];
    vs->msc_location = s.msc_location[offset];
    vs->vlr_location = s.vlr_location[offset];
  }
};

template <typename Locking = uint64_t>
struct Access_Info {
  atom::ExtentVector<uint64_t> s_id;
  atom::ExtentVector<uint8_t> ai_type;
  atom::ExtentVector<uint8_t> data1;
  atom::ExtentVector<uint8_t> data2;
  atom::ExtentVector<StringStruct<3>> data3;
  atom::ExtentVector<StringStruct<5>> data4;

  atom::AtomicExtentVector<uint64_t> lsn;

  atom::AtomicExtentVector<VersionAccess_Info*> version_chain;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> rw_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;

  static void copyBackOnAbort(Access_Info& ai, uint64_t offset, VersionAccess_Info* va) {
    ai.s_id.replace(offset, va->s_id);
    ai.ai_type.replace(offset, va->ai_type);
    ai.data1.replace(offset, va->data1);
    ai.data2.replace(offset, va->data2);
    ai.data3.replace(offset, va->data3);
    ai.data4.replace(offset, va->data4);
  }

  static void copyOnWrite(Access_Info& ai, uint64_t offset, VersionAccess_Info* va) {
    va->s_id = ai.s_id[offset];
    va->ai_type = ai.ai_type[offset];
    va->data1 = ai.data1[offset];
    va->data2 = ai.data2[offset];
    va->data3 = ai.data3[offset];
    va->data4 = ai.data4[offset];
  }
};

template <typename Locking = uint64_t>
struct Special_Facility {
  atom::ExtentVector<uint64_t> s_id;
  atom::ExtentVector<uint8_t> sf_type;
  atom::ExtentVector<bool> is_active;
  atom::ExtentVector<uint8_t> error_cntrl;
  atom::ExtentVector<uint8_t> data_a;
  atom::ExtentVector<StringStruct<5>> data_b;

  atom::AtomicExtentVector<uint64_t> lsn;

  atom::AtomicExtentVector<VersionSpecial_Facility*> version_chain;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> rw_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;

  static void copyBackOnAbort(Special_Facility& sf, uint64_t offset, VersionSpecial_Facility* vs) {
    sf.s_id.replace(offset, vs->s_id);
    sf.sf_type.replace(offset, vs->sf_type);
    sf.is_active.replace(offset, vs->is_active);
    sf.error_cntrl.replace(offset, vs->error_cntrl);
    sf.data_a.replace(offset, vs->data_a);
    sf.data_b.replace(offset, vs->data_b);
  }

  static void copyOnWrite(Special_Facility& sf, uint64_t offset, VersionSpecial_Facility* vs) {
    vs->s_id = sf.s_id[offset];
    vs->sf_type = sf.sf_type[offset];
    vs->is_active = sf.is_active[offset];
    vs->error_cntrl = sf.error_cntrl[offset];
    vs->data_a = sf.data_a[offset];
    vs->data_b = sf.data_b[offset];
  }
};

template <typename Locking = uint64_t>
struct Call_Forwarding {
  atom::ExtentVector<uint64_t> s_id;
  atom::ExtentVector<uint8_t> sf_type;
  atom::ExtentVector<uint8_t> start_time;
  atom::ExtentVector<uint8_t> end_time;
  atom::ExtentVector<StringStruct<15>> numberx;

  atom::AtomicExtentVector<uint64_t> lsn;

  atom::AtomicExtentVector<VersionCall_Forwarding*> version_chain;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> rw_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;

  static void copyBackOnAbort(Call_Forwarding& cf, uint64_t offset, VersionCall_Forwarding* vc) {
    cf.s_id.replace(offset, vc->s_id);
    cf.sf_type.replace(offset, vc->sf_type);
    cf.start_time.replace(offset, vc->start_time);
    cf.end_time.replace(offset, vc->end_time);
    cf.numberx.replace(offset, vc->numberx);
  }

  static void copyOnWrite(Call_Forwarding& cf, uint64_t offset, VersionCall_Forwarding* vc) {
    vc->s_id = cf.s_id[offset];
    vc->sf_type = cf.sf_type[offset];
    vc->start_time = cf.start_time[offset];
    vc->end_time = cf.end_time[offset];
    vc->numberx = cf.numberx[offset];
  }
};

template <typename TC, typename WM, typename Locking = uint64_t>
struct Database {
  common::ChunkAllocator ca{};
  atom::EpochManagerBase<common::ChunkAllocator> emp{&ca};
  TC tc;

  std::atomic<uint64_t> active_thr_;
  common::DetailCollector global_details_collector;

  WM wm_;
  tbb::spin_mutex mut;

  Subscriber<Locking> s;
  Access_Info<Locking> ai;
  Special_Facility<Locking> sf;
  Call_Forwarding<Locking> cf;

  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           uint64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::ChunkAllocator>>
      s_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           StringStruct<15>,
                                           atom::AtomicUnorderedMapBucket<uint64_t, StringStruct<15>>,
                                           common::ChunkAllocator>>
      subnbr_map;

  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           uint64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::ChunkAllocator>>
      ai_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           uint64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::ChunkAllocator>>
      sf_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           uint64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::ChunkAllocator>>
      cf_map;

 public:
  Database(bool online = false) : tc(&ca, &emp, online), active_thr_(0), wm_(std::thread::hardware_concurrency()) {}

  static void client(Database<TC, WM, Locking>& db, uint32_t population, int max_transactions, uint8_t core_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    // uint64_t transactions[10];

    while ((core_id % std::thread::hardware_concurrency()) != (unsigned)sched_getcpu()) {
    }

    db.active_thr_++;

    common::DetailCollector dc;
    dc.startWorker();

    for (int i = 0; i < max_transactions; ++i) {
      bool restart = false;
      uint16_t transaction_select = dis(gen) % 100;
      auto sid = db.getRandomSId(population, 1, population, gen);
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
    restart:
      transaction = db.tc.start();
      db.bot(transaction);

      int res = 0;
      if (transaction_select < 35) {
        VersionSubscriber s;
        res = db.getSubscriberData(transaction, sid, s);
      } else if (transaction_select < 45) {
        std::vector<StringStruct<15>> result;
        res = db.getNewDestination(transaction, sid, dis(gen) % 4 + 1, 8 * (dis(gen) % 3), dis(gen) % 24 + 1, result);
      } else if (transaction_select < 80) {
        VersionAccess_Info result;
        res = db.getAccessData(transaction, sid, dis(gen) % 4 + 1, result);
      } else if (transaction_select < 82) {
        res = db.updateSubscriberData(transaction, sid, dis(gen) % 2, dis(gen) % 256, dis(gen) % 4 + 1);
      } else if (transaction_select < 96) {
        // std::stringstream ss;
        // ss << std::setfill('0') << std::setw(15) << db.getRandomSId(population, 1, population, gen);
        StringStruct<15> stringstruct;
        // strncpy(stringstruct.string, ss.str().c_str(), 15);
        db.getRandomSubNbr(sid, stringstruct.string, 15);
        res = db.updateLocation(transaction, dis(gen), stringstruct);
      } else if (transaction_select < 98) {
        StringStruct<15> stringstruct;
        db.getRandomSubNbr(sid, stringstruct.string, 15);
        StringStruct<15> numberx;
        db.generateRandomString(numberx.string, 15, gen);
        res = db.insertCallForwarding(transaction, stringstruct, 8 * (dis(gen) % 3), dis(gen) % 24 + 1, numberx,
                                      dis(gen));
      }

      if (restart != 0) {
        restart = false;
        db.wm_.release(old_transaction, aborted_transaction);
      }

      if (res == 0) {
        dc.notFound();
        db.abort(transaction);
        db.commit(transaction, aborted_transaction);
      } else {
        bool comres = db.commit(transaction, aborted_transaction);
        if (comres && res == 1) {
          dc.commit();
        } else {
          dc.abort();
          restart = true;
          db.wm_.wait(transaction, aborted_transaction);
          old_transaction = transaction;
          goto restart;
        }
      }
    }
    db.active_thr_--;
    dc.stopWorker();

    db.waitAndTidy();
    while (db.active_thr_ > 0) {
      db.waitAndTidy();
    }

    db.mut.lock();
    db.global_details_collector.merge(dc);
    db.mut.unlock();

    db.emp.remove();
  }

  void bot(uint64_t transaction) { tc.bot(transaction); }
  void abort(uint64_t transaction) { tc.abort(transaction); }
  bool commit(uint64_t transaction, std::unordered_set<uint64_t>& oset) {
    bool commit = tc.commit(transaction, oset);
    common::OptimisticPredicateLocking<common::ChunkAllocator>::finishTransaction();
    return commit;
  }

  void populateDatabase(uint32_t population) {
    srand(time(0));

    s.s_id.reserve(population);
    s.sub_nbr.reserve(population);
    s.bit_1.reserve(population);
    s.bit_2.reserve(population);
    s.bit_3.reserve(population);
    s.bit_4.reserve(population);
    s.bit_5.reserve(population);
    s.bit_6.reserve(population);
    s.bit_7.reserve(population);
    s.bit_8.reserve(population);
    s.bit_9.reserve(population);
    s.bit_10.reserve(population);
    s.hex_1.reserve(population);
    s.hex_2.reserve(population);
    s.hex_3.reserve(population);
    s.hex_4.reserve(population);
    s.hex_5.reserve(population);
    s.hex_6.reserve(population);
    s.hex_7.reserve(population);
    s.hex_8.reserve(population);
    s.hex_9.reserve(population);
    s.hex_10.reserve(population);
    s.byte2_1.reserve(population);
    s.byte2_2.reserve(population);
    s.byte2_3.reserve(population);
    s.byte2_4.reserve(population);
    s.byte2_5.reserve(population);
    s.byte2_6.reserve(population);
    s.byte2_7.reserve(population);
    s.byte2_8.reserve(population);
    s.byte2_9.reserve(population);
    s.byte2_10.reserve(population);
    s.msc_location.reserve(population);
    s.vlr_location.reserve(population);
    s.version_chain.reserve(population);
    s.lsn.reserve(population);
    s.locked.reserve(population);
    s.rw_table.reserve(population);
    s.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    ai.s_id.reserve(3 * population);
    ai.ai_type.reserve(3 * population);
    ai.data1.reserve(3 * population);
    ai.data2.reserve(3 * population);
    ai.data3.reserve(3 * population);
    ai.data4.reserve(3 * population);
    ai.version_chain.reserve(3 * population);
    ai.lsn.reserve(3 * population);
    ai.locked.reserve(3 * population);
    ai.rw_table.reserve(3 * population);
    ai.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    sf.s_id.reserve(3 * population);
    sf.sf_type.reserve(3 * population);
    sf.data_a.reserve(3 * population);
    sf.data_b.reserve(3 * population);
    sf.is_active.reserve(3 * population);
    sf.error_cntrl.reserve(3 * population);
    sf.version_chain.reserve(3 * population);
    sf.lsn.reserve(3 * population);
    sf.locked.reserve(3 * population);
    sf.rw_table.reserve(3 * population);
    sf.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    cf.sf_type.reserve(4 * population);
    cf.s_id.reserve(4 * population);
    cf.start_time.reserve(4 * population);
    cf.end_time.reserve(4 * population);
    cf.numberx.reserve(4 * population);
    cf.version_chain.reserve(4 * population);
    cf.lsn.reserve(4 * population);
    cf.locked.reserve(4 * population);
    cf.rw_table.reserve(4 * population);
    cf.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    s_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(population,
                                                                                                         &ca, &emp);

    subnbr_map = std::make_unique<
        atom::AtomicUnorderedMap<uint64_t, StringStruct<15>, atom::AtomicUnorderedMapBucket<uint64_t, StringStruct<15>>,
                                 common::ChunkAllocator>>(population, &ca, &emp);

    ai_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(3 * population,
                                                                                                         &ca, &emp);

    sf_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(3 * population,
                                                                                                         &ca, &emp);

    cf_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(4 * population,
                                                                                                         &ca, &emp);

    for (uint32_t s_id = 1; s_id <= population; ++s_id) {
      // initilize subscriber
      s_map->insert(s_id, s.s_id.size());

      std::stringstream ss;
      ss << std::setfill('0') << std::setw(15) << s_id;
      StringStruct<15> stringstruct;
      strncpy(stringstruct.string, ss.str().c_str(), 15);
      s.sub_nbr.push_back(stringstruct);
      subnbr_map->insert(stringstruct, s.s_id.size());

      s.s_id.push_back(s_id);

      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
      s.bit_1.push_back(dis(gen) % 2);
      s.bit_2.push_back(dis(gen) % 2);
      s.bit_3.push_back(dis(gen) % 2);
      s.bit_4.push_back(dis(gen) % 2);
      s.bit_5.push_back(dis(gen) % 2);
      s.bit_6.push_back(dis(gen) % 2);
      s.bit_7.push_back(dis(gen) % 2);
      s.bit_8.push_back(dis(gen) % 2);
      s.bit_9.push_back(dis(gen) % 2);
      s.bit_10.push_back(dis(gen) % 2);

      s.hex_1.push_back(dis(gen) % 16);
      s.hex_2.push_back(dis(gen) % 16);
      s.hex_3.push_back(dis(gen) % 16);
      s.hex_4.push_back(dis(gen) % 16);
      s.hex_5.push_back(dis(gen) % 16);
      s.hex_6.push_back(dis(gen) % 16);
      s.hex_7.push_back(dis(gen) % 16);
      s.hex_8.push_back(dis(gen) % 16);
      s.hex_9.push_back(dis(gen) % 16);
      s.hex_10.push_back(dis(gen) % 16);

      s.byte2_1.push_back(dis(gen) % 256);
      s.byte2_2.push_back(dis(gen) % 256);
      s.byte2_3.push_back(dis(gen) % 256);
      s.byte2_4.push_back(dis(gen) % 256);
      s.byte2_5.push_back(dis(gen) % 256);
      s.byte2_6.push_back(dis(gen) % 256);
      s.byte2_7.push_back(dis(gen) % 256);
      s.byte2_8.push_back(dis(gen) % 256);
      s.byte2_9.push_back(dis(gen) % 256);
      s.byte2_10.push_back(dis(gen) % 256);

      s.msc_location.push_back(dis(gen));
      s.vlr_location.push_back(dis(gen));

      s.version_chain.push_back(nullptr);
      s.lsn.push_back(0);
      s.locked.push_back(static_cast<Locking>(0));
      s.rw_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

      uint8_t ai_cnt = 1 + dis(gen) % 4;
      uint8_t sf_cnt = 1 + dis(gen) % 4;
      uint8_t cf_cnt = dis(gen) % 4;

      std::vector<uint8_t> used;
      for (uint8_t j = 0; j < ai_cnt; ++j) {
        ai.s_id.push_back(s_id);
        uint8_t ai_type;
        do {
          ai_type = 1 + dis(gen) % 4;
        } while (std::end(used) != std::find(std::begin(used), std::end(used), ai_type));
        used.push_back(ai_type);
        ai.ai_type.push_back(ai_type);
        ai.data1.push_back(dis(gen) % 256);
        ai.data2.push_back(dis(gen) % 256);

        StringStruct<3> stringstruct_3;
        generateRandomString(stringstruct_3.string, 3, gen);
        ai.data3.push_back(stringstruct_3);

        StringStruct<5> stringstruct_5;
        generateRandomString(stringstruct_5.string, 5, gen);
        ai.data4.push_back(stringstruct_5);

        ai.lsn.push_back(0);
        ai.locked.push_back(static_cast<Locking>(0));
        ai.rw_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
        ai.version_chain.push_back(nullptr);

        ai_map->insert(ai_map->combine_key(s_id, ai_type - 1, 62), ai.s_id.size() - 1);
      }

      used.clear();
      std::vector<uint8_t> used_cf;
      for (uint8_t j = 0; j < sf_cnt; ++j) {
        sf.s_id.push_back(s_id);
        uint8_t sf_type;
        do {
          sf_type = 1 + dis(gen) % 4;
        } while (std::end(used) != std::find(std::begin(used), std::end(used), sf_type));
        used.push_back(sf_type);
        sf.sf_type.push_back(sf_type);
        sf.is_active.push_back(dis(gen) % 7);  // TODO(durner) roughly 85% one but hacky
        sf.error_cntrl.push_back(dis(gen) % 256);
        sf.data_a.push_back(dis(gen) % 256);

        StringStruct<5> stringstruct_5;
        generateRandomString(stringstruct_5.string, 5, gen);
        sf.data_b.push_back(stringstruct_5);

        sf.lsn.push_back(0);
        sf.locked.push_back(static_cast<Locking>(0));
        sf.rw_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
        sf.version_chain.push_back(nullptr);

        sf_map->insert(sf_map->combine_key(s_id, sf_type - 1, 62), sf.s_id.size() - 1);

        std::vector<uint8_t> tabu_set;
        for (uint8_t k = 0; k < cf_cnt; ++k) {
          cf.sf_type.push_back(sf_type);
          cf.s_id.push_back(s_id);

          uint8_t start_time;
          do {
            start_time = 8 * (dis(gen) % 3);
          } while (std::find(tabu_set.begin(), tabu_set.end(), start_time) != tabu_set.end());
          tabu_set.push_back(start_time);

          cf.start_time.push_back(start_time);
          cf.end_time.push_back(start_time + (dis(gen) % 8) + 1);

          StringStruct<15> stringstruct_15;
          generateRandomString(stringstruct_15.string, 15, gen);
          cf.numberx.push_back(stringstruct_15);

          cf.lsn.push_back(0);
          cf.locked.push_back(static_cast<Locking>(0));
          cf.rw_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
          cf.version_chain.push_back(nullptr);

          cf_map->insert(cf_map->combine_key(s_id, cf_map->combine_key(start_time >> 3, sf_type - 1, 62), 60),
                         cf.s_id.size() - 1);
        }
      }
    }

    std::cout << "s_map: " << s_map->size() << std::endl;
    std::cout << "sf_map: " << sf_map->size() << std::endl;
    std::cout << "ai_map: " << ai_map->size() << std::endl;
    std::cout << "cf_map: " << cf_map->size() << std::endl;
  }

  void waitAndTidy() { tc.waitAndTidy(); }

  void deleteDatabase() {
    for (uint64_t i = 0; i < s.rw_table.size(); i++) {
      if (s.rw_table[i]->size() > 0) {
        std::cout << i << ": ";
        for (auto s : *s.rw_table[i]) {
          std::cout << s << " -> ";
        }
        std::cout << std::endl;
      }
      delete s.rw_table[i];
    }
    for (uint64_t i = 0; i < sf.rw_table.size(); i++)
      delete sf.rw_table[i];
    for (uint64_t i = 0; i < ai.rw_table.size(); i++)
      delete ai.rw_table[i];
    for (uint64_t i = 0; i < cf.rw_table.size(); i++)
      delete cf.rw_table[i];
  }

  uint32_t getRandomSId(uint32_t population, uint32_t x, uint32_t y, std::mt19937& gen) {
    uint32_t A;
    if (population <= 1000000)
      A = 65535;
    else if (population <= 10000000)
      A = 1048575;
    else
      A = 2097151;

    std::uniform_int_distribution<unsigned int> pop_dis(0, A);
    std::uniform_int_distribution<unsigned int> xy_dis(x, y);

    return ((pop_dis(gen) | xy_dis(gen)) % (y - x + 1)) + x;
  }

  void getRandomSubNbr(uint32_t sid, char* stringstruct, uint32_t length) {
    for (uint64_t i = 0; i < length; i++)
      stringstruct[i] = '0';

    const std::string s = std::to_string(sid);
    int32_t slength = s.length();

    for (auto i = length; i > 0 && slength > 0;) {
      stringstruct[i - 1] = s.at(slength - 1);
      i--;
      slength--;
    }
  }

  void generateRandomString(char* v, uint8_t length, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, 26);
    for (uint8_t i = 0; i < length; ++i) {
      v[i] = 65 + dis(gen);
    }
  }

  int getSubscriberData(uint64_t transaction, uint32_t s_id, VersionSubscriber& result) {
    uint64_t offset;

    /*bool ins = sopl.insert(sid, sid, *s_map.get(),
        [](decltype(*s_map.get())& smap, uint32_t& o, uint32_t& s) {return smap.insert(s, o);}
      );

    if (!ins) {
      std::cout << "insert error:" << sid << std::endl;
    }*/

    bool found = s.opl->lookup(s_id, offset, *s_map, [](auto& smap, auto& o, auto& s) { return smap.lookup(s, o); });

    if (!found) {
      std::cout << offset << " not found!" << std::endl;
      return 0;
    }

    {
      mv::ReadGuard<TC, VersionSubscriber, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>>
          rg{&tc, s.version_chain, s.rw_table, s.locked, s.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(result.s_id, s.s_id, [](VersionSubscriber* vs) { return vs->s_id; });
      rg.read(result.sub_nbr, s.sub_nbr, [](VersionSubscriber* vs) { return vs->sub_nbr; });
      rg.read(result.bit_1, s.bit_1, [](VersionSubscriber* vs) { return vs->bit_1; });
      rg.read(result.bit_2, s.bit_2, [](VersionSubscriber* vs) { return vs->bit_2; });
      rg.read(result.bit_3, s.bit_3, [](VersionSubscriber* vs) { return vs->bit_3; });
      rg.read(result.bit_4, s.bit_4, [](VersionSubscriber* vs) { return vs->bit_4; });
      rg.read(result.bit_5, s.bit_5, [](VersionSubscriber* vs) { return vs->bit_5; });
      rg.read(result.bit_6, s.bit_6, [](VersionSubscriber* vs) { return vs->bit_6; });
      rg.read(result.bit_7, s.bit_7, [](VersionSubscriber* vs) { return vs->bit_7; });
      rg.read(result.bit_8, s.bit_8, [](VersionSubscriber* vs) { return vs->bit_8; });
      rg.read(result.bit_9, s.bit_9, [](VersionSubscriber* vs) { return vs->bit_9; });
      rg.read(result.bit_10, s.bit_10, [](VersionSubscriber* vs) { return vs->bit_10; });
      rg.read(result.hex_1, s.hex_1, [](VersionSubscriber* vs) { return vs->hex_1; });
      rg.read(result.hex_2, s.hex_2, [](VersionSubscriber* vs) { return vs->hex_2; });
      rg.read(result.hex_3, s.hex_3, [](VersionSubscriber* vs) { return vs->hex_3; });
      rg.read(result.hex_4, s.hex_4, [](VersionSubscriber* vs) { return vs->hex_4; });
      rg.read(result.hex_5, s.hex_5, [](VersionSubscriber* vs) { return vs->hex_5; });
      rg.read(result.hex_6, s.hex_6, [](VersionSubscriber* vs) { return vs->hex_6; });
      rg.read(result.hex_7, s.hex_7, [](VersionSubscriber* vs) { return vs->hex_7; });
      rg.read(result.hex_8, s.hex_8, [](VersionSubscriber* vs) { return vs->hex_8; });
      rg.read(result.hex_9, s.hex_9, [](VersionSubscriber* vs) { return vs->hex_9; });
      rg.read(result.hex_10, s.hex_10, [](VersionSubscriber* vs) { return vs->hex_10; });
      rg.read(result.byte2_1, s.byte2_1, [](VersionSubscriber* vs) { return vs->byte2_1; });
      rg.read(result.byte2_2, s.byte2_2, [](VersionSubscriber* vs) { return vs->byte2_2; });
      rg.read(result.byte2_3, s.byte2_3, [](VersionSubscriber* vs) { return vs->byte2_3; });
      rg.read(result.byte2_4, s.byte2_4, [](VersionSubscriber* vs) { return vs->byte2_4; });
      rg.read(result.byte2_5, s.byte2_5, [](VersionSubscriber* vs) { return vs->byte2_5; });
      rg.read(result.byte2_6, s.byte2_6, [](VersionSubscriber* vs) { return vs->byte2_6; });
      rg.read(result.byte2_7, s.byte2_7, [](VersionSubscriber* vs) { return vs->byte2_7; });
      rg.read(result.byte2_8, s.byte2_8, [](VersionSubscriber* vs) { return vs->byte2_8; });
      rg.read(result.byte2_9, s.byte2_9, [](VersionSubscriber* vs) { return vs->byte2_9; });
      rg.read(result.byte2_10, s.byte2_10, [](VersionSubscriber* vs) { return vs->byte2_10; });
      rg.read(result.msc_location, s.msc_location, [](VersionSubscriber* vs) { return vs->msc_location; });
      rg.read(result.vlr_location, s.vlr_location, [](VersionSubscriber* vs) { return vs->vlr_location; });
    }

    return 1;
  }

  int getNewDestination(uint64_t transaction,
                        uint32_t s_id,
                        uint8_t sf_type,
                        uint8_t start_time,
                        uint8_t end_time,
                        std::vector<StringStruct<15>>& result) {
    uint64_t sf_offset = 0;

    uint64_t sfid = sf_map->combine_key(s_id, sf_type - 1, 62);
    bool found =
        sf.opl->lookup(sfid, sf_offset, *sf_map, [](auto& sfmap, auto& o, auto& s) { return sfmap.lookup(s, o); });
    if (!found)
      return 0;

    bool active = false;
    {
      mv::ReadGuard<TC, VersionSpecial_Facility, Locking, atom::AtomicExtentVector,
                    atom::AtomicSinglyLinkedList<uint64_t>>
          rg{&tc, sf.version_chain, sf.rw_table, sf.locked, sf.lsn, sf_offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(active, sf.is_active, [](VersionSpecial_Facility* vs) { return vs->is_active; });
    }
    if (!active)
      return 0;

    // nested loop index join
    uint64_t cf_offset = 0;
    for (uint8_t k = 0; k < 3; ++k) {
      uint64_t cfid = cf_map->combine_key(s_id, cf_map->combine_key(k, sf_type - 1, 62), 60);
      bool found =
          cf.opl->lookup(cfid, cf_offset, *cf_map, [](auto& cfmap, auto& o, auto& s) { return cfmap.lookup(s, o); });
      if (!found)
        continue;

      uint8_t cf_start_time = 0, cf_end_time = 0;
      StringStruct<15> numberx;
      {
        mv::ReadGuard<TC, VersionCall_Forwarding, Locking, atom::AtomicExtentVector,
                      atom::AtomicSinglyLinkedList<uint64_t>>
            rg{&tc, cf.version_chain, cf.rw_table, cf.locked, cf.lsn, cf_offset, transaction};
        if (!rg.wasSuccessful()) {
          return -1;
        }
        rg.read(cf_start_time, cf.start_time, [](VersionCall_Forwarding* vc) { return vc->start_time; });
        rg.read(cf_end_time, cf.end_time, [](VersionCall_Forwarding* vc) { return vc->end_time; });
        rg.read(numberx, cf.numberx, [](VersionCall_Forwarding* vc) { return vc->numberx; });
      }

      if (cf_start_time <= start_time && end_time < cf_end_time) {
        result.push_back(numberx);
      }
    }
    return 1;
  }

  int getAccessData(uint64_t transaction, uint32_t s_id, uint8_t ai_type, VersionAccess_Info& result) {
    uint64_t offset;
    uint64_t aiid = ai_map->combine_key(s_id, ai_type - 1, 62);
    bool found =
        ai.opl->lookup(aiid, offset, *ai_map, [](auto& index, auto& o, auto& s) { return index.lookup(s, o); });
    if (!found)
      return 0;

    {
      mv::ReadGuard<TC, VersionAccess_Info, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>>
          rg{&tc, ai.version_chain, ai.rw_table, ai.locked, ai.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }

      rg.read(result.data1, ai.data1, [](VersionAccess_Info* va) { return va->data1; });
      rg.read(result.data2, ai.data2, [](VersionAccess_Info* va) { return va->data2; });
      rg.read(result.data3, ai.data3, [](VersionAccess_Info* va) { return va->data3; });
      rg.read(result.data4, ai.data4, [](VersionAccess_Info* va) { return va->data4; });
    }

    return 1;
  }

  int updateSubscriberData(uint64_t transaction, uint32_t s_id, bool bit_1, uint8_t data_a, uint8_t sf_type) {
    uint64_t offset;
    bool found = s.opl->lookup(s_id, offset, *s_map, [](auto& smap, auto& o, auto& s) { return smap.lookup(s, o); });
    if (!found)
      return 0;

    uint64_t sf_offset = 0;
    uint64_t sfid = sf_map->combine_key(s_id, sf_type - 1, 62);
    found = sf.opl->lookup(sfid, sf_offset, *sf_map, [](auto& index, auto& o, auto& s) { return index.lookup(s, o); });
    if (!found)
      return 0;

    {
      auto cow = [&](VersionSpecial_Facility* vs, uint64_t offset) {
        Special_Facility<Locking>::copyOnWrite(sf, offset, vs);
      };
      auto coa = [&](VersionSpecial_Facility* vs, uint64_t offset) {
        Special_Facility<Locking>::copyBackOnAbort(sf, offset, vs);
      };

      mv::WriteGuard<TC, VersionSpecial_Facility, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, sf.version_chain, sf.rw_table, sf.locked, sf.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(data_a, sf.data_a);
    }

    {
      auto cow = [&](VersionSubscriber* vs, uint64_t offset) { Subscriber<Locking>::copyOnWrite(s, offset, vs); };
      auto coa = [&](VersionSubscriber* vs, uint64_t offset) { Subscriber<Locking>::copyBackOnAbort(s, offset, vs); };

      mv::WriteGuard<TC, VersionSubscriber, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, s.version_chain, s.rw_table, s.locked, s.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(bit_1, s.bit_1);
    }

    return 1;
  }

  int updateLocation(uint64_t transaction, uint32_t vlr_location, StringStruct<15> sub_nbr) {
    uint64_t offset;
    bool found =
        s.opl->lookup(sub_nbr, offset, *subnbr_map, [](auto& smap, auto& o, auto& s) { return smap.lookup(s, o); });
    if (!found)
      return 0;

    {
      auto cow = [&](VersionSubscriber* vs, uint64_t offset) { Subscriber<Locking>::copyOnWrite(s, offset, vs); };
      auto coa = [&](VersionSubscriber* vs, uint64_t offset) { Subscriber<Locking>::copyBackOnAbort(s, offset, vs); };

      mv::WriteGuard<TC, VersionSubscriber, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, s.version_chain, s.rw_table, s.locked, s.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(vlr_location, s.vlr_location);
    }
    return 1;
  }

  int insertCallForwarding(uint64_t transaction,
                           StringStruct<15> sub_nbr,
                           uint8_t start_time,
                           uint8_t end_time,
                           StringStruct<15> numberx,
                           uint64_t random) {
    uint64_t s_id;
    bool found =
        s.opl->lookup(sub_nbr, s_id, *subnbr_map, [](auto& smap, auto& o, auto& s) { return smap.lookup(s, o); });
    if (!found)
      return 0;

    // nested loop fake secondary index
    uint64_t sf_offset = 0;
    std::vector<uint64_t> sftypes;
    for (uint8_t k = 0; k < 3; ++k) {
      uint64_t sfid = sf_map->combine_key(s_id, k, 62);
      bool found =
          sf.opl->lookup(sfid, sf_offset, *sf_map, [](auto& sfmap, auto& o, auto& s) { return sfmap.lookup(s, o); });

      if (!found)
        continue;

      sftypes.emplace_back(k);
    }

    if (sftypes.size() == 0) {
      return 0;
    }

    auto sftype = sftypes.at(random % sftypes.size());
    uint64_t cfid = cf_map->combine_key(s_id, cf_map->combine_key(start_time, sftype, 62), 60);

    uint64_t main_id = cf.s_id.push_back(s_id);
    uint64_t sub_id = cf.sf_type.push_back(sftype);
    if (main_id != sub_id) {
      while (!cf.sf_type.isAlive(main_id)) {
      }
      cf.sf_type.replace(main_id, sftype);
    }

    sub_id = cf.start_time.push_back(start_time);
    if (main_id != sub_id) {
      while (!cf.start_time.isAlive(main_id)) {
      }
      cf.start_time.replace(main_id, start_time);
    }

    sub_id = cf.end_time.push_back(end_time);
    if (main_id != sub_id) {
      while (!cf.end_time.isAlive(main_id)) {
      }
      cf.end_time.replace(main_id, end_time);
    }

    sub_id = cf.numberx.push_back(numberx);
    if (main_id != sub_id) {
      while (!cf.numberx.isAlive(main_id)) {
      }
      cf.numberx.replace(main_id, numberx);
    }

    sub_id = cf.lsn.push_back(0);
    if (main_id != sub_id) {
      while (!cf.lsn.isAlive(main_id)) {
      }
      cf.lsn.atomic_replace(main_id, 0);
    }

    sub_id = cf.version_chain.push_back(nullptr);
    if (main_id != sub_id) {
      while (!cf.version_chain.isAlive(main_id)) {
      }
      cf.version_chain.atomic_replace(main_id, nullptr);
    }

    sub_id = cf.locked.push_back(static_cast<Locking>(0));
    if (main_id != sub_id) {
      while (!cf.locked.isAlive(main_id)) {
      }
      cf.locked.atomic_replace(main_id, static_cast<Locking>(0));
    }

    auto ptr = new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp};
    sub_id = cf.rw_table.push_back(ptr);
    if (main_id != sub_id) {
      while (!cf.rw_table.isAlive(main_id)) {
      }
      cf.rw_table.atomic_replace(main_id, ptr);
    }

    bool check = true;
    {
      auto cow = [&](VersionCall_Forwarding* vs, uint64_t offset) {
        Call_Forwarding<Locking>::copyOnWrite(cf, offset, vs);
      };
      auto coa = [&](VersionCall_Forwarding* vs, uint64_t offset) {
        Call_Forwarding<Locking>::copyBackOnAbort(cf, offset, vs);
      };

      mv::WriteGuard<TC, VersionCall_Forwarding, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, cf.version_chain, cf.rw_table, cf.locked, cf.lsn, cow, coa, main_id, transaction};
      if (!wg.wasSuccessful()) {
        check = false;
      } else {
        wg.write(s_id, cf.s_id);
      }
    }
    if (check) {
      check = cf.opl->insert(cfid, main_id, *cf_map, [](auto& cfmap, auto& o, auto& s) { return cfmap.insert(s, o); });
    }

    if (!check) {
      cf.s_id.erase(main_id);
      cf.sf_type.erase(main_id);
      cf.start_time.erase(main_id);
      cf.end_time.erase(main_id);
      cf.numberx.erase(main_id);
      cf.lsn.erase(main_id);
      cf.locked.erase(main_id);
      cf.version_chain.erase(main_id);
      cf.rw_table.erase(main_id);
      return 0;
    }
    return 1;
  }

  template <typename Function, typename... Args>
  bool async(uint64_t transaction, Function& f, Args&&... args) {
    bot(transaction);
    f(std::forward<Args>(args)...);
    return commit(transaction);
  }
};
};  // namespace tatp
};  // namespace mv

namespace std {
template <unsigned int t>
struct hash<mv::tatp::StringStruct<t>> {
  uint64_t operator()(mv::tatp::StringStruct<t> const& s) {
    char ary[t + 1] = "";
    strncpy(ary, s.string, t);
    return std::hash<std::string>{}(ary);
  }
};
}  // namespace std
