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
#include "ds/extent_vector.hpp"
#include "mvcc/benchmarks/read_guard.hpp"
#include "mvcc/benchmarks/write_guard.hpp"
#include <iomanip>
#include <memory>
#include <random>
#include <tuple>
#include <unordered_set>
#include <vector>
#include <stdint.h>

namespace mv {
namespace ycsb {
enum Action { READ, WRITE, SCAN };

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

struct VersionUsertable {
  uint64_t key;
  StringStruct<100> f01;
  StringStruct<100> f02;
  StringStruct<100> f03;
  StringStruct<100> f04;
  StringStruct<100> f05;
  StringStruct<100> f06;
  StringStruct<100> f07;
  StringStruct<100> f08;
  StringStruct<100> f09;
  StringStruct<100> f10;

  uint64_t transaction;
  uint64_t epoch;
  bool commited;
  VersionUsertable* nxt;
  VersionUsertable* prv;
};

template <typename Locking = uint64_t>
struct Usertable {
  atom::ExtentVector<uint64_t> key;
  atom::ExtentVector<StringStruct<100>> f01;
  atom::ExtentVector<StringStruct<100>> f02;
  atom::ExtentVector<StringStruct<100>> f03;
  atom::ExtentVector<StringStruct<100>> f04;
  atom::ExtentVector<StringStruct<100>> f05;
  atom::ExtentVector<StringStruct<100>> f06;
  atom::ExtentVector<StringStruct<100>> f07;
  atom::ExtentVector<StringStruct<100>> f08;
  atom::ExtentVector<StringStruct<100>> f09;
  atom::ExtentVector<StringStruct<100>> f10;

  atom::AtomicExtentVector<uint64_t> lsn;

  atom::AtomicExtentVector<VersionUsertable*> version_chain;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> rw_table;

  static void copyBackOnAbort(Usertable& u, uint64_t offset, VersionUsertable* vu) {
    u.key.replace(offset, vu->key);
    u.f01.replace(offset, vu->f01);
    u.f02.replace(offset, vu->f02);
    u.f03.replace(offset, vu->f03);
    u.f04.replace(offset, vu->f04);
    u.f05.replace(offset, vu->f05);
    u.f06.replace(offset, vu->f06);
    u.f07.replace(offset, vu->f07);
    u.f08.replace(offset, vu->f08);
    u.f09.replace(offset, vu->f09);
    u.f10.replace(offset, vu->f10);
  }

  static void copyOnWrite(Usertable& u, uint64_t offset, VersionUsertable* vu) {
    vu->key = u.key[offset];
    vu->f01 = u.f01[offset];
    vu->f02 = u.f02[offset];
    vu->f03 = u.f03[offset];
    vu->f04 = u.f04[offset];
    vu->f05 = u.f05[offset];
    vu->f06 = u.f06[offset];
    vu->f07 = u.f07[offset];
    vu->f08 = u.f08[offset];
    vu->f09 = u.f09[offset];
    vu->f10 = u.f10[offset];
  }

  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

template <typename TC, typename WM, typename Locking = uint64_t>
struct Database {
  common::ChunkAllocator ca{};
  atom::EpochManagerBase<common::ChunkAllocator> emp{&ca};
  TC tc;
  common::DetailCollector global_details_collector;

  std::atomic<uint64_t> active_thr_;

  WM wm_;
  tbb::spin_mutex mut;

  Usertable<Locking> usertable;

  uint64_t queriesPerTransaction = 16;
  double readPercentage = 0.5;
  double writePercentage = 0.5;
  double scanPercentage = 0;
  double scanLength = 0.01;
  double theta = 0.9;
  double zeta_2_theta;
  double denom;
  uint64_t population;

  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           uint64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::ChunkAllocator>>
      key_map;

 public:
  Database(uint64_t qyPerTx, double readPct, double scanPct, double theta, bool online = false)
      : tc(&ca, &emp, online),
        active_thr_(0),
        wm_(std::thread::hardware_concurrency()),
        queriesPerTransaction(qyPerTx),
        readPercentage(readPct),
        writePercentage(1.0 - readPct),
        scanPercentage(scanPct),
        theta(theta) {
    zeta_2_theta = 0;
    for (uint64_t i = 1; i <= 2; i++)
      zeta_2_theta += pow(1.0 / i, theta);
  }

  double zeta() {
    double sum = 0;
    for (uint64_t i = 1; i <= population; i++)
      sum += pow(1.0 / i, theta);
    return sum;
  }

  uint64_t zipf(std::mt19937& gen) {
    uint64_t population = this->population - 1;
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / population, 1 - theta)) / (1 - zeta_2_theta / zetan);
    std::uniform_real_distribution<double> dis(0, 1);
    double u = dis(gen);
    double uz = u * zetan;
    if (uz < 1)
      return 1;
    if (uz < 1 + pow(0.5, theta))
      return 2;
    return 1 + (uint64_t)(population * pow(eta * u - eta + 1, alpha));
  }

  void generateRandomString(char* v, uint8_t length, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, 26);
    uint64_t pseudo = dis(gen);
    for (uint8_t i = 0; i < length; ++i) {
      v[i] = 65 + ((pseudo + i) % 26);
    }
  }

  void generateTransaction(std::vector<uint64_t>& key, std::vector<Action>& query, std::mt19937& gen) {
    std::uniform_real_distribution<double> dis(0, 1);
    double isScan = dis(gen);
    if (isScan < scanPercentage) {
      key.push_back(zipf(gen));
      query.push_back(Action::SCAN);
      return;
    }
    for (auto i = 0u; i < queriesPerTransaction; i++) {
      double isRead = dis(gen);
      if (isRead < readPercentage) {
        key.push_back(zipf(gen));
        query.push_back(Action::READ);
      } else if (isRead < writePercentage + readPercentage) {
        key.push_back(zipf(gen));
        query.push_back(Action::WRITE);
      }
    }
  }

  void deleteDatabase() {
    for (uint64_t i = 0; i < usertable.rw_table.size(); i++) {
      if (usertable.rw_table[i]->size() > 0) {
        std::cout << i << ": ";
        for (auto s : *usertable.rw_table[i]) {
          std::cout << s << " -> ";
        }
        std::cout << std::endl;
      }
      delete usertable.rw_table[i];
    }
  }

  static void client(Database<TC, WM, Locking>& db, uint32_t population, int max_transactions, uint8_t core_id) {
    clientMulti<true>(db, population, max_transactions, core_id);
  }

  template <bool OLAPScan>
  static void clientMulti(Database<TC, WM, Locking>& db, uint32_t population, int max_transactions, uint8_t core_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    // uint64_t transactions[10];
    while ((core_id % std::thread::hardware_concurrency()) != (unsigned)sched_getcpu()) {
    }

    db.active_thr_++;

    common::DetailCollector dc;
    dc.startWorker();

    std::vector<uint64_t> randomKey;
    std::vector<Action> randomAction;

    VersionUsertable us{};
    std::vector<VersionUsertable> usv;
    usv.reserve(db.scanLength * db.population);

    for (int i = 0; i < max_transactions; ++i) {
      randomKey.clear();
      randomAction.clear();
      bool restart = false;
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
      dc.startLatency();
      db.generateTransaction(randomKey, randomAction, gen);
    restart:
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;
      dc.startTX();
      bool olap = false;

      for (auto j = 0u; j < randomKey.size(); ++j) {
        if (randomAction[j] == Action::READ) {
          res = db.readData(transaction, randomKey[j], us);
        } else if (randomAction[j] == Action::WRITE) {
          db.generateRandomString(us.f01.string, 100, gen);
          res = db.writeData(transaction, randomKey[j], us);
        } else if (randomAction[j] == Action::SCAN) {
          if (OLAPScan)
            olap = true;
          res = db.scanData<OLAPScan>(transaction, randomKey[j], db.population * db.scanLength, usv);
        }

        if (res != 1)
          break;
      }

      if (restart != 0) {
        restart = false;
        db.wm_.release(old_transaction, aborted_transaction);
      }

      if (res == 0) {
        dc.notFound(olap);
        db.abort(transaction);
        dc.startCommit();
        db.commit(transaction, aborted_transaction);
        dc.stopCommit(olap);
      } else {
        dc.startCommit();
        bool comres = db.commit(transaction, aborted_transaction);
        dc.stopCommit(olap);
        if (comres && res == 1) {
          dc.commit(olap);
        } else {
          dc.abort(olap);
          restart = true;
          dc.startWaitManager();
          db.wm_.wait(transaction, aborted_transaction);
          dc.stopWaitManager(olap);
          old_transaction = transaction;
          goto restart;
        }
      }
      dc.stopLatency(dc.stopTX(olap), olap);
    }
    db.active_thr_--;
    dc.stopWorker();

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

  void populateDatabase(uint64_t population) {
    std::random_device rd;
    std::mt19937 random_gen(rd());

    this->population = population;
    denom = zeta();

    usertable.key.reserve(population);
    usertable.f01.reserve(population);
    usertable.f02.reserve(population);
    usertable.f03.reserve(population);
    usertable.f04.reserve(population);
    usertable.f05.reserve(population);
    usertable.f06.reserve(population);
    usertable.f07.reserve(population);
    usertable.f08.reserve(population);
    usertable.f09.reserve(population);
    usertable.f10.reserve(population);
    usertable.lsn.reserve(population);
    usertable.locked.reserve(population);
    usertable.rw_table.reserve(population);
    usertable.version_chain.reserve(population);

    key_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(
        population << 4, &ca, &emp);

    for (uint64_t i = 1; i <= population; ++i) {
      key_map->insert(i, usertable.key.size());

      usertable.key.push_back(i);
      StringStruct<100> stringstruct_100;
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f01.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f02.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f03.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f04.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f05.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f06.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f07.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f08.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f09.push_back(stringstruct_100);
      generateRandomString(stringstruct_100.string, 100, random_gen);
      usertable.f10.push_back(stringstruct_100);
      usertable.lsn.push_back(0);
      usertable.locked.push_back(static_cast<Locking>(0));
      usertable.version_chain.push_back(nullptr);
      usertable.rw_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
    }
    usertable.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};
  }

  template <bool OLAPScan>
  int scanData(uint64_t transaction, uint64_t startKey, uint64_t length, std::vector<VersionUsertable>& result) {
    if (startKey + length >= population)
      return 0;

    if (OLAPScan) {
      mv::ReadGuard<TC, VersionUsertable, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>,
                    true>
          rg{&tc, usertable.version_chain, usertable.rw_table, usertable.locked, usertable.lsn, 0, transaction};
      for (uint64_t i = startKey; i < startKey + length; i++) {
        rg.readOLAP(result[i - startKey].key, usertable.key, [](VersionUsertable* vu) { return vu->key; }, i);
        rg.readOLAP(result[i - startKey].f01, usertable.f01, [](VersionUsertable* vu) { return vu->f01; }, i);
        rg.readOLAP(result[i - startKey].f02, usertable.f02, [](VersionUsertable* vu) { return vu->f02; }, i);
        rg.readOLAP(result[i - startKey].f03, usertable.f03, [](VersionUsertable* vu) { return vu->f03; }, i);
        rg.readOLAP(result[i - startKey].f04, usertable.f04, [](VersionUsertable* vu) { return vu->f04; }, i);
        rg.readOLAP(result[i - startKey].f05, usertable.f05, [](VersionUsertable* vu) { return vu->f05; }, i);
        rg.readOLAP(result[i - startKey].f06, usertable.f06, [](VersionUsertable* vu) { return vu->f06; }, i);
        rg.readOLAP(result[i - startKey].f07, usertable.f07, [](VersionUsertable* vu) { return vu->f07; }, i);
        rg.readOLAP(result[i - startKey].f08, usertable.f08, [](VersionUsertable* vu) { return vu->f08; }, i);
        rg.readOLAP(result[i - startKey].f09, usertable.f09, [](VersionUsertable* vu) { return vu->f09; }, i);
        rg.readOLAP(result[i - startKey].f10, usertable.f10, [](VersionUsertable* vu) { return vu->f10; }, i);
      }
      return 1;
    } else {
      for (uint64_t i = startKey; i < length; i++) {
        auto res = readData(transaction, i, result[i - startKey]);
        if (res != 1)
          return res;
      }
    }
    return 1;
  }

  int readData(uint64_t transaction, uint64_t key, VersionUsertable& result) {
    uint64_t offset = 0;
    bool found =
        usertable.opl->lookup(key, offset, *key_map, [](auto& index, auto& o, auto& s) { return index.lookup(s, o); });

    if (!found)
      return 0;

    mv::ReadGuard<TC, VersionUsertable, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
        &tc, usertable.version_chain, usertable.rw_table, usertable.locked, usertable.lsn, offset, transaction};

    if (!rg.wasSuccessful()) {
      return -1;
    }
    rg.read(result.key, usertable.key, [](VersionUsertable* vu) { return vu->key; });
    rg.read(result.f01, usertable.f01, [](VersionUsertable* vu) { return vu->f01; });
    rg.read(result.f02, usertable.f02, [](VersionUsertable* vu) { return vu->f02; });
    rg.read(result.f03, usertable.f03, [](VersionUsertable* vu) { return vu->f03; });
    rg.read(result.f04, usertable.f04, [](VersionUsertable* vu) { return vu->f04; });
    rg.read(result.f05, usertable.f05, [](VersionUsertable* vu) { return vu->f05; });
    rg.read(result.f06, usertable.f06, [](VersionUsertable* vu) { return vu->f06; });
    rg.read(result.f07, usertable.f07, [](VersionUsertable* vu) { return vu->f07; });
    rg.read(result.f08, usertable.f08, [](VersionUsertable* vu) { return vu->f08; });
    rg.read(result.f09, usertable.f09, [](VersionUsertable* vu) { return vu->f09; });
    rg.read(result.f10, usertable.f10, [](VersionUsertable* vu) { return vu->f10; });
    return 1;
  }

  int writeData(uint64_t transaction, uint64_t key, VersionUsertable& val) {
    uint64_t offset = 0;
    bool found =
        usertable.opl->lookup(key, offset, *key_map, [](auto& index, auto& o, auto& s) { return index.lookup(s, o); });

    if (!found)
      return 0;

    {
      auto cow = [&](VersionUsertable* vu, uint64_t offset) { Usertable<Locking>::copyOnWrite(usertable, offset, vu); };
      auto coa = [&](VersionUsertable* vu, uint64_t offset) {
        Usertable<Locking>::copyBackOnAbort(usertable, offset, vu);
      };

      mv::WriteGuard<TC, VersionUsertable, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc,        usertable.version_chain, usertable.rw_table, usertable.locked, usertable.lsn, cow, coa, offset,
             transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(val.f01, usertable.f01);
      /*wg.write(val.f02, usertable.f02);
      wg.write(val.f03, usertable.f03);
      wg.write(val.f04, usertable.f04);
      wg.write(val.f05, usertable.f05);
      wg.write(val.f06, usertable.f06);
      wg.write(val.f07, usertable.f07);
      wg.write(val.f08, usertable.f08);
      wg.write(val.f09, usertable.f09);
      wg.write(val.f10, usertable.f10);*/
    }
    return 1;
  }

  void printMemoryDetails() { ca.printDetails(); }

  template <typename Function, typename... Args>
  bool async(uint64_t transaction, Function& f, Args&&... args) {
    bot(transaction);
    f(std::forward<Args>(args)...);
    return commit(transaction);
  }
};

};  // namespace ycsb
};  // namespace mv

namespace std {
template <unsigned int t>
struct hash<mv::ycsb::StringStruct<t>> {
  uint64_t operator()(mv::ycsb::StringStruct<t> const& s) {
    char ary[t + 1] = "";
    strncpy(ary, s.string, t);
    return std::hash<std::string>{}(ary);
  }
};
}  // namespace std
