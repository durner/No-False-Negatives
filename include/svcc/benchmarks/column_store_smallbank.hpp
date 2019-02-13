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
#include "ds/atomic_extent_vector.hpp"
#include "ds/atomic_singly_linked_list.hpp"
#include "ds/atomic_unordered_map.hpp"
#include "ds/extent_vector.hpp"
#include <iomanip>
#include <memory>
#include <random>
#include <tuple>
#include <unordered_set>
#include <vector>
#include <stdint.h>

namespace sv {
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

template <typename Locking = uint64_t>
struct Account {
  atom::ExtentVector<StringStruct<20>> name;
  atom::ExtentVector<uint64_t> customer_id;

  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
};

template <typename Locking = uint64_t>
struct Saving {
  atom::ExtentVector<uint64_t> customer_id;
  atom::ExtentVector<double> balance;

  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
};

template <typename Locking = uint64_t>
struct Checking {
  atom::ExtentVector<uint64_t> customer_id;
  atom::ExtentVector<double> balance;

  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
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

  Account<Locking> a;
  Saving<Locking> s;
  Checking<Locking> c;

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
      dc.startTX();
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;
      bool olap = false;
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
        olap = true;
        double total;
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

    db.mut.lock();
    db.global_details_collector.merge(dc);
    db.mut.unlock();

    db.emp.remove();
  }

  template <int test = 0>
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
          res = db.getTotalChecking(transaction, total);
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
          res = db.getTotalChecking(transaction, total);
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
              db.tc.~TC();
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
          total = 0;
          restart = true;
          db.wm_.wait(transaction, aborted_transaction);
          old_transaction = transaction;
          goto restart;
        }
      }
    }
    db.active_thr_--;
    dc.stopWorker();

    db.mut.lock();
    db.global_details_collector.merge(dc);
    db.mut.unlock();

    db.emp.remove();
  }

  static void clientOLAPOnly(Database<TC, WM, Locking>& db, uint32_t population, int oltp_worker, uint8_t core_id) {
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

    db.mut.lock();
    db.global_details_collector.merge(dc);
    db.mut.unlock();

    db.emp.remove();
  }

  void bot(uint64_t transaction) { tc.bot(transaction); }
  bool commit(uint64_t transaction, std::unordered_set<uint64_t>& oset) { return tc.commit(transaction, oset); }
  void abort(uint64_t transaction) { tc.abort(transaction); }

  void printTable() {
    uint64_t sum = 0;
    std::cout << "CHECKING" << std::endl;
    for (uint32_t i = 0; i < c.customer_id.size(); ++i) {
      std::cout << "offset: " << i << " | " << c.balance[i];
      std::cout << std::endl;
      sum += c.balance[i];
    }
  }

  void populateDatabase(uint32_t population) {
    const uint64_t min_balance = 10000;
    const uint64_t max_balance = 50000;

    std::random_device rd;
    std::mt19937 random_gen(rd());
    std::uniform_int_distribution<unsigned int> dis(min_balance, max_balance);

    a.name.reserve(population);
    a.customer_id.reserve(population);
    a.lsn.reserve(population);
    a.locked.reserve(population);
    a.read_write_table.reserve(population);

    s.customer_id.reserve(population);
    s.balance.reserve(population);
    s.lsn.reserve(population);
    s.locked.reserve(population);
    s.read_write_table.reserve(population);

    c.customer_id.reserve(population);
    c.balance.reserve(population);
    c.lsn.reserve(population);
    c.locked.reserve(population);
    c.read_write_table.reserve(population);

    name_map = std::make_unique<
        atom::AtomicUnorderedMap<uint64_t, StringStruct<20>, atom::AtomicUnorderedMapBucket<uint64_t, StringStruct<20>>,
                                 common::ChunkAllocator>>(population, &ca, &emp);

    saving_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(population,
                                                                                                         &ca, &emp);

    checking_map = std::make_unique<atom::AtomicUnorderedMap<
        uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>, common::ChunkAllocator>>(population,
                                                                                                         &ca, &emp);

    for (uint32_t cust_id = 1; cust_id <= population; ++cust_id) {
      // initilize subscriber

      std::stringstream ss;
      ss << std::setfill('0') << std::setw(20) << cust_id;
      StringStruct<20> stringstruct;
      strncpy(stringstruct.string, ss.str().c_str(), 20);

      name_map->insert(stringstruct, s.customer_id.size());

      a.name.push_back(stringstruct);
      a.customer_id.push_back(cust_id);
      a.lsn.push_back(0);
      a.locked.push_back(static_cast<Locking>(0));
      a.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

      saving_map->insert(cust_id, s.customer_id.size());
      s.customer_id.push_back(cust_id);
      s.balance.push_back(dis(random_gen));
      s.lsn.push_back(0);
      s.locked.push_back(static_cast<Locking>(0));
      s.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

      checking_map->insert(cust_id, c.customer_id.size());
      c.customer_id.push_back(cust_id);
      c.balance.push_back(dis(random_gen));
      c.lsn.push_back(0);
      c.locked.push_back(static_cast<Locking>(0));
      c.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
    }
  }

  void printMemoryDetails() { ca.printDetails(); }

  void deleteDatabase() {
    printMemoryDetails();
    for (uint64_t i = 0; i < a.read_write_table.size(); i++)
      delete a.read_write_table[i];
    for (uint64_t i = 0; i < s.read_write_table.size(); i++)
      delete s.read_write_table[i];
    for (uint64_t i = 0; i < c.read_write_table.size(); i++)
      delete c.read_write_table[i];
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

  int getBalance(uint64_t transaction, StringStruct<20>& name, double& summed_balance) {
    uint64_t offset;
    bool found = name_map->lookup(name, offset);
    if (!found) {
      return 0;
    }

    uint64_t cust_id;
    bool check = tc.readValue(cust_id, a.customer_id, a.lsn, a.read_write_table, a.locked, offset, transaction);
    if (!check)
      return -1;

    double savings;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(savings, s.balance, s.lsn, s.read_write_table, s.locked, offset, transaction);
    if (!check)
      return -1;

    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    summed_balance = checking + savings;

    return 1;
  }

  int getTotalChecking(uint64_t transaction, double& summed_balance) {
    bool check;
    double savings;
    for (uint64_t i = 0; i < c.customer_id.size(); i++) {
      check = tc.readValue(savings, c.balance, c.lsn, c.read_write_table, c.locked, i, transaction);
      if (check)
        summed_balance += savings;
    }

    return 1;
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
    bool check = tc.readValue(cust_id, a.customer_id, a.lsn, a.read_write_table, a.locked, offset, transaction);
    if (!check)
      return -1;

    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    checking += amount;

    check = tc.writeValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    return 1;
  }

  int transactSaving(uint64_t transaction, StringStruct<20>& name, double amount) {
    uint64_t offset;
    bool found = name_map->lookup(name, offset);
    if (!found) {
      return 0;
    }
    uint64_t cust_id;
    bool check = tc.readValue(cust_id, a.customer_id, a.lsn, a.read_write_table, a.locked, offset, transaction);
    if (!check)
      return -1;

    double saving;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(saving, s.balance, s.lsn, s.read_write_table, s.locked, offset, transaction);
    if (!check)
      return -1;

    saving += amount;

    if (saving < 0) {
      return 0;
    }

    check = tc.writeValue(saving, s.balance, s.lsn, s.read_write_table, s.locked, offset, transaction);
    if (!check)
      return -1;

    return 1;
  }

  int amalgamate(uint64_t transaction, StringStruct<20>& name1, StringStruct<20>& name2) {
    uint64_t offset;
    bool found = name_map->lookup(name1, offset);
    if (!found) {
      return 0;
    }

    uint64_t cust_id;
    bool check = tc.readValue(cust_id, a.customer_id, a.lsn, a.read_write_table, a.locked, offset, transaction);
    if (!check)
      return -1;

    double zero = 0;

    double savings;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(savings, s.balance, s.lsn, s.read_write_table, s.locked, offset, transaction);
    if (!check)
      return -1;

    check = tc.writeValue(zero, s.balance, s.lsn, s.read_write_table, s.locked, offset, transaction);
    if (!check)
      return -1;

    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    check = tc.writeValue(zero, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    double summed_balance = checking + savings;

    found = name_map->lookup(name2, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(cust_id, a.customer_id, a.lsn, a.read_write_table, a.locked, offset, transaction);
    if (!check)
      return -1;

    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    checking += summed_balance;

    check = tc.writeValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    return 1;
  }

  int writeCheck(uint64_t transaction, StringStruct<20>& name, double amount) {
    uint64_t offset;
    bool found = name_map->lookup(name, offset);
    if (!found) {
      return 0;
    }

    uint64_t cust_id;
    bool check = tc.readValue(cust_id, a.customer_id, a.lsn, a.read_write_table, a.locked, offset, transaction);
    if (!check)
      return -1;

    double savings;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(savings, s.balance, s.lsn, s.read_write_table, s.locked, offset, transaction);
    if (!check)
      return -1;

    double checking;
    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    double summed_balance = checking + savings;

    if (summed_balance < amount)
      amount += 1;

    checking -= amount;

    check = tc.writeValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    return 1;
  }

  int sendPayment(uint64_t transaction, StringStruct<20>& name1, StringStruct<20>& name2, double amount) {
    uint64_t offset;
    bool found = name_map->lookup(name1, offset);
    if (!found) {
      return 0;
    }
    uint64_t cust_id;
    bool check = tc.readValue(cust_id, a.customer_id, a.lsn, a.read_write_table, a.locked, offset, transaction);
    if (!check)
      return -1;

    double checking;
    found = saving_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    checking -= amount;

    if (checking < 0) {
      return 0;
    }

    check = tc.writeValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    found = name_map->lookup(name2, offset);
    if (!found) {
      return 0;
    }
    check = tc.readValue(cust_id, a.customer_id, a.lsn, a.read_write_table, a.locked, offset, transaction);
    if (!check)
      return -1;

    found = checking_map->lookup(cust_id, offset);
    if (!found) {
      return 0;
    }

    check = tc.readValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

    checking += amount;

    check = tc.writeValue(checking, c.balance, c.lsn, c.read_write_table, c.locked, offset, transaction);
    if (!check)
      return -1;

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
};  // namespace sv

namespace std {
template <unsigned int t>
struct hash<sv::smallbank::StringStruct<t>> {
  uint64_t operator()(sv::smallbank::StringStruct<t> const& s) {
    char ary[t + 1] = "";
    strncpy(ary, s.string, t);
    return std::hash<std::string>{}(ary);
  }
};
}  // namespace std
