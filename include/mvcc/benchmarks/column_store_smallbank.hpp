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
#include "common/details_collector.hpp"
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
#include <tbb/spin_mutex.h>
#include <unistd.h>

namespace mv {
namespace smallbank {
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

struct VersionAccount {
  StringStruct<20> name;
  uint64_t customer_id;

  uint64_t transaction;
  uint64_t epoch;
  bool commited;
  VersionAccount* nxt;
  VersionAccount* prv;
};

struct VersionSaving {
  uint64_t customer_id;
  double balance;

  uint64_t transaction;
  uint64_t epoch;
  bool commited;
  VersionSaving* nxt;
  VersionSaving* prv;
};

struct VersionChecking {
  uint64_t customer_id;
  double balance;

  uint64_t transaction;
  uint64_t epoch;
  bool commited;
  VersionChecking* nxt;
  VersionChecking* prv;
};

template <typename Locking = uint64_t>
struct Account {
  atom::ExtentVector<StringStruct<20>> name;
  atom::ExtentVector<uint64_t> customer_id;

  atom::AtomicExtentVector<uint64_t> lsn;

  atom::AtomicExtentVector<VersionAccount*> version_chain;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> rw_table;

  static void copyBackOnAbort(Account& a, uint64_t offset, VersionAccount* va) {
    a.customer_id.replace(offset, va->customer_id);
    a.name.replace(offset, va->name);
  }

  static void copyOnWrite(Account& a, uint64_t offset, VersionAccount* va) {
    va->customer_id = a.customer_id[offset];
    va->name = a.name[offset];
  }
};

template <typename Locking = uint64_t>
struct Saving {
  atom::ExtentVector<uint64_t> customer_id;
  atom::ExtentVector<double> balance;

  atom::AtomicExtentVector<uint64_t> lsn;

  atom::AtomicExtentVector<VersionSaving*> version_chain;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> rw_table;

  static void copyBackOnAbort(Saving& s, uint64_t offset, VersionSaving* vs) {
    s.customer_id.replace(offset, vs->customer_id);
    s.balance.replace(offset, vs->balance);
  }

  static void copyOnWrite(Saving& s, uint64_t offset, VersionSaving* vs) {
    vs->customer_id = s.customer_id[offset];
    vs->balance = s.balance[offset];
  }
};

template <typename Locking = uint64_t>
struct Checking {
  atom::ExtentVector<uint64_t> customer_id;
  atom::ExtentVector<double> balance;

  atom::AtomicExtentVector<uint64_t> lsn;

  atom::AtomicExtentVector<VersionChecking*> version_chain;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> rw_table;

  static void copyBackOnAbort(Checking& c, uint64_t offset, VersionChecking* vc) {
    c.customer_id.replace(offset, vc->customer_id);
    c.balance.replace(offset, vc->balance);
  }

  static void copyOnWrite(Checking& c, uint64_t offset, VersionChecking* vc) {
    vc->customer_id = c.customer_id[offset];
    vc->balance = c.balance[offset];
  }
};

template <typename TC, typename WM, typename Locking = uint64_t>
struct Database {
  common::ChunkAllocator ca{};
  atom::EpochManagerBase<common::ChunkAllocator> emp{&ca};
  TC tc;
  common::DetailCollector global_details_collector;

  Account<Locking> a;
  Saving<Locking> s;
  Checking<Locking> c;

  std::atomic<uint64_t> active_thr_;

  WM wm_;
  tbb::spin_mutex mut;

  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           StringStruct<20>,
                                           atom::AtomicUnorderedMapBucket<uint64_t, StringStruct<20>>,
                                           common::ChunkAllocator>>
      name_map;

  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           uint64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::ChunkAllocator>>
      saving_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           uint64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::ChunkAllocator>>
      checking_map;

 public:
  Database(bool online = false) : tc(&ca, &emp, online), active_thr_(0), wm_(std::thread::hardware_concurrency()) {}

  void printMemoryDetails() { ca.printDetails(); }

  void removeTables() {
    a.~Account();
    s.~Saving();
    c.~Checking();
    name_map = nullptr;
    saving_map = nullptr;
    checking_map = nullptr;
  }

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
      StringStruct<20> name1;
      StringStruct<20> name2;
      db.getRandomName(name1, population, 25, 100, gen);
      db.getRandomName(name2, population, 25, 100, gen);
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
      dc.startLatency();
    restart:
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;
      dc.startTX();
      if (transaction_select < 25) {
        res = db.sendPayment(transaction, name1, name2, 5.0);
      } else if (transaction_select < 40) {
        double balance = 0;
        res = db.getBalance(transaction, name1, balance);

      } else if (transaction_select < 55) {
        res = db.depositChecking(transaction, name1, 1.3);

      } else if (transaction_select < 70) {
        res = db.transactSaving(transaction, name1, 20.20);

      } else if (transaction_select < 85) {
        res = db.writeCheck(transaction, name1, 5.0);

      } else {
        res = db.amalgamate(transaction, name1, name2);
      }

      if (restart != 0) {
        restart = false;
        db.wm_.release(old_transaction, aborted_transaction);
      }

      if (res == 0) {
        dc.notFound();
        db.abort(transaction);
        dc.startCommit();
        db.commit(transaction, aborted_transaction);
        dc.stopCommit();
      } else {
        dc.startCommit();
        bool comres = db.commit(transaction, aborted_transaction);
        dc.stopCommit();
        if (comres && res == 1) {
          dc.commit();
        } else {
          dc.abort();
          restart = true;
          dc.startWaitManager();
          db.wm_.wait(transaction, aborted_transaction);
          dc.stopWaitManager();
          old_transaction = transaction;
          goto restart;
        }
      }
      dc.stopLatency(dc.stopTX());
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

  static void clientHighContention(Database<TC, WM, Locking>& db,
                                   uint32_t population,
                                   int max_transactions,
                                   uint8_t core_id) {
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
      uint16_t transaction_select[8];
      StringStruct<20> name1[8];
      StringStruct<20> name2[8];
      for (auto i = 0; i < 8; i++) {
        transaction_select[i] = dis(gen) % 100;
        db.getRandomName(name1[i], population, 25, 100, gen);
        db.getRandomName(name2[i], population, 25, 100, gen);
      }
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
      dc.startLatency();
    restart:
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;
      dc.startTX();
      for (auto i = 0; i < 8; i++) {
        if (transaction_select[i] < 25) {
          res = db.sendPayment(transaction, name1[i], name2[i], 5.0);
        } else if (transaction_select[i] < 40) {
          double balance = 0;
          res = db.getBalance(transaction, name1[i], balance);

        } else if (transaction_select[i] < 55) {
          res = db.depositChecking(transaction, name1[i], 1.3);

        } else if (transaction_select[i] < 70) {
          res = db.transactSaving(transaction, name1[i], 20.20);

        } else if (transaction_select[i] < 85) {
          res = db.writeCheck(transaction, name1[i], 5.0);

        } else {
          res = db.amalgamate(transaction, name1[i], name2[i]);
        }
      }

      if (restart != 0) {
        restart = false;
        db.wm_.release(old_transaction, aborted_transaction);
      }

      if (res == 0) {
        dc.notFound();
        db.abort(transaction);
        dc.startCommit();
        db.commit(transaction, aborted_transaction);
        dc.stopCommit();
      } else {
        dc.startCommit();
        bool comres = db.commit(transaction, aborted_transaction);
        dc.stopCommit();
        if (comres && res == 1) {
          dc.commit();
        } else {
          dc.abort();
          restart = true;
          dc.startWaitManager();
          db.wm_.wait(transaction, aborted_transaction);
          dc.stopWaitManager();
          old_transaction = transaction;
          goto restart;
        }
      }
      dc.stopLatency(dc.stopTX());
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

  template <int test = 0, bool OLAP = false>
  static void clientTest(Database<TC, WM, Locking>& db, uint32_t population, int max_transactions, uint8_t core_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    // uint64_t transactions[10];
    while ((core_id % std::thread::hardware_concurrency()) != (unsigned)sched_getcpu()) {
    }
    double total_old = 0, total = 0;

    db.active_thr_++;

    common::DetailCollector dc;
    dc.startWorker();

    for (int i = 0; i < max_transactions; ++i) {
      bool restart = false;
      uint16_t transaction_select = dis(gen) % 100;
      StringStruct<20> name1;
      StringStruct<20> name2;
      db.getRandomName(name1, population, 25, 100, gen);
      db.getRandomName(name2, population, 25, 100, gen);
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
    restart:
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;

      /// NOCHANGE
      if (test == 0) {
        if (transaction_select < 70) {
          res = db.sendPayment(transaction, name1, name2, 500);
        } else if (transaction_select < 90) {
          double balance = 0;
          res = db.getBalance(transaction, name1, balance);
        } else {
          total = 0;
          res = db.getTotalChecking<OLAP>(transaction, total);
        }
      }
      // Monotone Increasing
      else if (test == 1) {
        if (transaction_select < 70) {
          res = db.sendPayment(transaction, name1, name2, 500);
        } else if (transaction_select < 80) {
          double balance = 0;
          res = db.getBalance(transaction, name1, balance);
        } else if (transaction_select < 90) {
          res = db.depositChecking(transaction, name1, 1300);
        } else {
          total = 0;
          res = db.getTotalChecking<OLAP>(transaction, total);
        }
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
          if (total != 0 && test == 0) {
            if (res > 0 && total_old != total && total_old != 0) {
              std::cout << "same result test failed: " << std::setprecision(32) << total << " vs. "
                        << std::setprecision(32) << total_old << std::endl;
              db.printTable();
              exit(-1);
            }
          } else if (total != 0 && test == 1) {
            if (total < total_old) {
              std::cerr << "monotone increasing test failed" << std::endl;
              exit(-1);
            }
          }
          total_old = total;

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

  static void clientScan(Database<TC, WM, Locking>& db, uint32_t population, int max_transactions, uint8_t core_id) {
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
      StringStruct<20> name1;
      StringStruct<20> name2;
      db.getRandomName(name1, population, 25, 100, gen);
      db.getRandomName(name2, population, 25, 100, gen);
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
      dc.startLatency();
    restart:
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;
      bool olap = false;
      dc.startTX();
      if (transaction_select < 24) {
        res = db.sendPayment(transaction, name1, name2, 5.0);

      } else if (transaction_select < 38) {
        double balance;
        res = db.getBalance(transaction, name1, balance);

      } else if (transaction_select < 52) {
        res = db.depositChecking(transaction, name1, 1.3);

      } else if (transaction_select < 66) {
        res = db.transactSaving(transaction, name1, 20.20);

      } else if (transaction_select < 80) {
        res = db.writeCheck(transaction, name1, 5.0);

      } else if (transaction_select < 95) {
        res = db.amalgamate(transaction, name1, name2);
      } else {
        double total;
        olap = true;
        res = db.getTotalChecking(transaction, total);
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

    db.waitAndTidy();
    while (db.active_thr_ > 0) {
      db.waitAndTidy();
    }

    db.mut.lock();
    db.global_details_collector.merge(dc);
    db.mut.unlock();

    db.emp.remove();
  }

  static void clienOLAPOnly(Database<TC, WM, Locking>& db, uint32_t population, int oltp_worker, uint8_t core_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    // uint64_t transactions[10];
    while ((core_id % std::thread::hardware_concurrency()) != (unsigned)sched_getcpu()) {
    }

    while (db.active_thr_ < (unsigned)oltp_worker) {
    }

    common::DetailCollector dc;
    dc.startWorker();

    while (db.active_thr_ >= (unsigned)oltp_worker) {
      bool restart = false;
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
      dc.startLatency();
    restart:
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;
      dc.startTX();

      double total = 0;
      bool olap = true;
      res = db.getTotalChecking(transaction, total);

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

  static void clientReadOnly(Database<TC, WM, Locking>& db,
                             uint32_t population,
                             int max_transactions,
                             uint8_t core_id) {
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
      StringStruct<20> name1;
      StringStruct<20> name2;
      db.getRandomName(name1, population, 25, 100, gen);
      db.getRandomName(name2, population, 25, 100, gen);
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
    restart:
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;
      if (transaction_select < 0) {
        double balance = 0;
        StringStruct<20> name1;
        db.getRandomName(name1, population, 25, 100, gen);
        res = db.getBalance(transaction, name1, balance);

      } else {
        double total = 0;
        res = db.getTotalChecking(transaction, total);
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
  bool commit(uint64_t transaction, std::unordered_set<uint64_t>& abort_transaction) {
    return tc.commit(transaction, abort_transaction);
  }
  void abort(uint64_t transaction) { tc.abort(transaction); }

  void populateDatabase(uint32_t population) {
    const uint64_t min_balance = 10000;
    const uint64_t max_balance = 50000;

    std::random_device rd;
    std::mt19937 random_gen(rd());
    std::uniform_int_distribution<unsigned int> dis(min_balance, max_balance);

    a.name.reserve(population);
    a.customer_id.reserve(population);
    a.version_chain.reserve(population);
    a.lsn.reserve(population);
    a.locked.reserve(population);
    a.rw_table.reserve(population);

    s.customer_id.reserve(population);
    s.balance.reserve(population);
    s.version_chain.reserve(population);
    s.lsn.reserve(population);
    s.locked.reserve(population);
    s.rw_table.reserve(population);

    c.customer_id.reserve(population);
    c.balance.reserve(population);
    c.version_chain.reserve(population);
    c.lsn.reserve(population);
    c.locked.reserve(population);
    c.rw_table.reserve(population);

    name_map = std::make_unique<
        atom::AtomicUnorderedMap<uint64_t, StringStruct<20>, atom::AtomicUnorderedMapBucket<uint64_t, StringStruct<20>>,
                                 common::ChunkAllocator>>(population, &ca, &emp);

    saving_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(population,
                                                                                                         &ca, &emp);

    checking_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(population,
                                                                                                         &ca, &emp);

    for (uint64_t cust_id = 1; cust_id <= population; ++cust_id) {
      // initilize subscriber

      StringStruct<20> stringstruct;
      char buffer[21];
      sprintf(buffer, "%.20lu", cust_id);
      strncpy(stringstruct.string, buffer, 20);

      name_map->insert(stringstruct, a.customer_id.size());

      a.name.push_back(stringstruct);
      a.customer_id.push_back(cust_id);
      a.version_chain.push_back(nullptr);
      a.lsn.push_back(0);
      a.locked.push_back(static_cast<Locking>(0));
      a.rw_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

      saving_map->insert(cust_id, s.customer_id.size());
      s.customer_id.push_back(cust_id);
      s.balance.push_back(dis(random_gen));
      s.version_chain.push_back(nullptr);
      s.lsn.push_back(0);
      s.locked.push_back(static_cast<Locking>(0));
      s.rw_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

      checking_map->insert(cust_id, c.customer_id.size());
      c.customer_id.push_back(cust_id);
      c.balance.push_back(dis(random_gen));
      c.version_chain.push_back(nullptr);
      c.lsn.push_back(0);
      c.locked.push_back(static_cast<Locking>(0));
      c.rw_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
    }
  }

  void printTable() {
    uint64_t sum = 0;
    std::cout << "CHECKING" << std::endl;
    for (uint32_t i = 0; i < c.customer_id.size(); ++i) {
      std::cout << "offset: " << i << " | " << c.balance[i];
      auto elem = c.version_chain[i];
      while (elem != nullptr) {
        std::cout << " -> " << elem->balance << " (" << elem->epoch << ")";
        elem = elem->nxt;
      }
      std::cout << std::endl;
      sum += c.balance[i];
    }
    std::cout << "Sum: " << sum << std::endl;
    /*
    std::cout << "SAVING" << std::endl;
    for (uint32_t i = 0; i <= s.customer_id.size(); ++i) {
      std::cout << "offset: " << i << " | " << s.balance[i];
      auto elem = s.version_chain[i];
      while (elem != nullptr) {
        std::cout << " -> " << elem->balance << " (" << elem->epoch << ")";
      }
      std::cout << std::endl;
    }*/
  }

  void deleteDatabase() {
    printMemoryDetails();
    for (uint64_t i = 0; i < a.rw_table.size(); i++) {
      if (a.rw_table[i]->size() > 0) {
        std::cout << i << ": ";
        for (auto a : *a.rw_table[i]) {
          std::cout << a << " -> ";
        }
        std::cout << std::endl;
      }
      delete a.rw_table[i];
    }
    for (uint64_t i = 0; i < s.rw_table.size(); i++)
      delete s.rw_table[i];
    for (uint64_t i = 0; i < c.rw_table.size(); i++)
      delete c.rw_table[i];
  }

  void getRandomName(StringStruct<20>& name,
                     uint32_t population,
                     int hotspot_percent,
                     int hotspot_size,
                     std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> pop_dis(1, population);

    int64_t hotspot = pop_dis(gen) % 100;
    uint64_t cust_id = 0;

    if (hotspot <= hotspot_percent) {
      cust_id = (pop_dis(gen) % hotspot_size) + 1;
    } else {
      cust_id = pop_dis(gen);
    }

    char buffer[21];
    sprintf(buffer, "%.20lu", cust_id);
    strncpy(name.string, buffer, 20);
  }

  void waitAndTidy() { tc.waitAndTidy(); }

  int getBalance(uint64_t transaction, StringStruct<20>& name, double& summed_balance) {
    uint64_t offset;
    bool found = name_map->lookup(name, offset);
    if (!found) {
      return 0;
    }
    uint64_t cust_id;
    {
      mv::ReadGuard<TC, VersionAccount, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, a.version_chain, a.rw_table, a.locked, a.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(cust_id, a.customer_id, [](VersionAccount* va) { return va->customer_id; });
    }

    double savings;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionSaving, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, s.version_chain, s.rw_table, s.locked, s.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(savings, s.balance, [](VersionSaving* vs) { return vs->balance; });
    }

    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, c.version_chain, c.rw_table, c.locked, c.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(checking, c.balance, [](VersionChecking* vc) { return vc->balance; });
    }

    summed_balance = checking + savings;

    return 1;
  }

  template <bool OLAP = true>
  int getTotalChecking(uint64_t transaction, double& summed_balance) {
    double checking = 0;
    bool check;
    if (OLAP) {
      {
        mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>,
                      true>
            rg{&tc, c.version_chain, c.rw_table, c.locked, c.lsn, 0, transaction};
        for (uint64_t i = 0; i < c.customer_id.size(); i++) {
          check = rg.readOLAP(checking, c.balance, [](VersionChecking* vc) { return vc->balance; }, i);
          if (check)
            summed_balance += checking;
        }
        return 1;
      }
    } else {
      for (uint64_t i = 0; i < c.customer_id.size(); i++) {
        {
          mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>>
              rg{&tc, c.version_chain, c.rw_table, c.locked, c.lsn, i, transaction};
          if (!rg.wasSuccessful()) {
            return -1;
          }
          rg.read(checking, c.balance, [](VersionChecking* vc) { return vc->balance; });
        }
        summed_balance += checking;
      }
      return 1;
    }
  }

  int depositChecking(uint64_t transaction, StringStruct<20>& name, double amount) {
    if (amount < 0)
      return -1;

    uint64_t offset;
    bool found = name_map->lookup(name, offset);
    if (!found) {
      return 0;
    }

    uint64_t cust_id;
    {
      mv::ReadGuard<TC, VersionAccount, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, a.version_chain, a.rw_table, a.locked, a.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(cust_id, a.customer_id, [](VersionAccount* va) { return va->customer_id; });
    }

    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, c.version_chain, c.rw_table, c.locked, c.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(checking, c.balance, [](VersionChecking* vc) { return vc->balance; });
    }

    checking += amount;
    {
      auto cow = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyOnWrite(c, offset, vc); };
      auto coa = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyBackOnAbort(c, offset, vc); };

      mv::WriteGuard<TC, VersionChecking, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, c.version_chain, c.rw_table, c.locked, c.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(checking, c.balance);
    }

    return 1;
  }

  int transactSaving(uint64_t transaction, StringStruct<20>& name, double amount) {
    uint64_t offset;
    bool found = name_map->lookup(name, offset);
    if (!found) {
      return 0;
    }

    uint64_t cust_id;
    {
      mv::ReadGuard<TC, VersionAccount, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, a.version_chain, a.rw_table, a.locked, a.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(cust_id, a.customer_id, [](VersionAccount* va) { return va->customer_id; });
    }
    double saving;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionSaving, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, s.version_chain, s.rw_table, s.locked, s.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(saving, s.balance, [](VersionSaving* vs) { return vs->balance; });
    }
    saving += amount;

    if (saving < 0) {
      return 0;
    }
    {
      auto cow = [&](VersionSaving* vs, uint64_t offset) { Saving<Locking>::copyOnWrite(s, offset, vs); };
      auto coa = [&](VersionSaving* vs, uint64_t offset) { Saving<Locking>::copyBackOnAbort(s, offset, vs); };

      mv::WriteGuard<TC, VersionSaving, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, s.version_chain, s.rw_table, s.locked, s.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(saving, s.balance);
    }
    return 1;
  }

  int amalgamate(uint64_t transaction, StringStruct<20>& name1, StringStruct<20>& name2) {
    uint64_t offset;
    bool found = name_map->lookup(name1, offset);
    if (!found) {
      return 0;
    }

    uint64_t cust_id;
    {
      mv::ReadGuard<TC, VersionAccount, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, a.version_chain, a.rw_table, a.locked, a.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(cust_id, a.customer_id, [](VersionAccount* va) { return va->customer_id; });
    }

    double zero = 0;

    double savings;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionSaving, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, s.version_chain, s.rw_table, s.locked, s.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(savings, s.balance, [](VersionSaving* vs) { return vs->balance; });
    }
    {
      auto cow = [&](VersionSaving* vs, uint64_t offset) { Saving<Locking>::copyOnWrite(s, offset, vs); };
      auto coa = [&](VersionSaving* vs, uint64_t offset) { Saving<Locking>::copyBackOnAbort(s, offset, vs); };

      mv::WriteGuard<TC, VersionSaving, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, s.version_chain, s.rw_table, s.locked, s.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(zero, s.balance);
    }

    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, c.version_chain, c.rw_table, c.locked, c.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(checking, c.balance, [](VersionChecking* vc) { return vc->balance; });
    }

    {
      auto cow = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyOnWrite(c, offset, vc); };
      auto coa = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyBackOnAbort(c, offset, vc); };

      mv::WriteGuard<TC, VersionChecking, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, c.version_chain, c.rw_table, c.locked, c.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(zero, c.balance);
    }

    double summed_balance = checking + savings;

    found = name_map->lookup(name2, offset);
    if (!found) {
      return 0;
    }

    {
      mv::ReadGuard<TC, VersionAccount, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, a.version_chain, a.rw_table, a.locked, a.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(cust_id, a.customer_id, [](VersionAccount* va) { return va->customer_id; });
    }

    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, c.version_chain, c.rw_table, c.locked, c.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(checking, c.balance, [](VersionChecking* vc) { return vc->balance; });
    }

    checking += summed_balance;
    {
      auto cow = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyOnWrite(c, offset, vc); };
      auto coa = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyBackOnAbort(c, offset, vc); };

      mv::WriteGuard<TC, VersionChecking, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, c.version_chain, c.rw_table, c.locked, c.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(checking, c.balance);
    }
    return 1;
  }

  int writeCheck(uint64_t transaction, StringStruct<20>& name, double amount) {
    uint64_t offset;
    bool found = name_map->lookup(name, offset);
    if (!found) {
      return 0;
    }

    uint64_t cust_id;
    {
      mv::ReadGuard<TC, VersionAccount, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, a.version_chain, a.rw_table, a.locked, a.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(cust_id, a.customer_id, [](VersionAccount* va) { return va->customer_id; });
    }
    double savings;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionSaving, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, s.version_chain, s.rw_table, s.locked, s.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(savings, s.balance, [](VersionSaving* vs) { return vs->balance; });
    }

    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, c.version_chain, c.rw_table, c.locked, c.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(checking, c.balance, [](VersionChecking* vc) { return vc->balance; });
    }

    double summed_balance = checking + savings;

    if (summed_balance < amount)
      amount += 1;

    checking -= amount;
    {
      auto cow = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyOnWrite(c, offset, vc); };
      auto coa = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyBackOnAbort(c, offset, vc); };

      mv::WriteGuard<TC, VersionChecking, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, c.version_chain, c.rw_table, c.locked, c.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(checking, c.balance);
    }
    return 1;
  }

  int sendPayment(uint64_t transaction, StringStruct<20>& name1, StringStruct<20>& name2, double amount) {
    uint64_t offset;
    bool found = name_map->lookup(name1, offset);
    if (!found) {
      return 0;
    }

    uint64_t cust_id;
    {
      mv::ReadGuard<TC, VersionAccount, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, a.version_chain, a.rw_table, a.locked, a.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(cust_id, a.customer_id, [](VersionAccount* va) { return va->customer_id; });
    }
    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, c.version_chain, c.rw_table, c.locked, c.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(checking, c.balance, [](VersionChecking* vc) { return vc->balance; });
    }

    checking -= amount;

    if (checking < 0) {
      return 0;
    }

    {
      auto cow = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyOnWrite(c, offset, vc); };
      auto coa = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyBackOnAbort(c, offset, vc); };

      mv::WriteGuard<TC, VersionChecking, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, c.version_chain, c.rw_table, c.locked, c.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(checking, c.balance);
    }

    found = name_map->lookup(name2, offset);
    if (!found) {
      return 0;
    }

    {
      mv::ReadGuard<TC, VersionAccount, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, a.version_chain, a.rw_table, a.locked, a.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(cust_id, a.customer_id, [](VersionAccount* va) { return va->customer_id; });
    }

    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }
    {
      mv::ReadGuard<TC, VersionChecking, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList<uint64_t>> rg{
          &tc, c.version_chain, c.rw_table, c.locked, c.lsn, offset, transaction};
      if (!rg.wasSuccessful()) {
        return -1;
      }
      rg.read(checking, c.balance, [](VersionChecking* vc) { return vc->balance; });
    }

    checking += amount;

    {
      auto cow = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyOnWrite(c, offset, vc); };
      auto coa = [&](VersionChecking* vc, uint64_t offset) { Checking<Locking>::copyBackOnAbort(c, offset, vc); };

      mv::WriteGuard<TC, VersionChecking, Locking, decltype(cow), decltype(coa), atom::AtomicExtentVector,
                     atom::AtomicSinglyLinkedList<uint64_t>>
          wg{&tc, c.version_chain, c.rw_table, c.locked, c.lsn, cow, coa, offset, transaction};
      if (!wg.wasSuccessful()) {
        return -1;
      }
      wg.write(checking, c.balance);
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

};  // namespace smallbank
};  // namespace mv

namespace std {
template <unsigned int t>
struct hash<mv::smallbank::StringStruct<t>> {
  uint64_t operator()(mv::smallbank::StringStruct<t> const& s) {
    char ary[t + 1] = "";
    strncpy(ary, s.string, t);
    return std::hash<std::string>{}(ary);
  }
};
}  // namespace std
