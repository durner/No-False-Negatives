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
#include "common/epoch_manager.hpp"
#include "common/optimistic_predicate_locking.hpp"
#include "ds/atomic_extent_vector.hpp"
#include "ds/atomic_singly_linked_list.hpp"
#include "ds/atomic_unordered_map.hpp"
#include "ds/atomic_unordered_multimap.hpp"
#include "ds/extent_vector.hpp"
#include "svcc/benchmarks/read_guard.hpp"
#include <algorithm>
#include <random>
#include <unordered_set>

namespace sv {
namespace tpcc {
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

using namespace std;

template <typename Locking = uint64_t>
struct Warehouse {
  // primary key w_id
  atom::ExtentVector<int64_t> w_id;
  atom::ExtentVector<StringStruct<10>> w_name;
  atom::ExtentVector<StringStruct<20>> w_street_1;
  atom::ExtentVector<StringStruct<20>> w_street_2;
  atom::ExtentVector<StringStruct<20>> w_city;
  atom::ExtentVector<StringStruct<2>> w_state;
  atom::ExtentVector<StringStruct<9>> w_zip;
  atom::ExtentVector<int8_t> w_tax;   // signed num(4,4)
  atom::ExtentVector<int16_t> w_ytd;  // signed num (12,2)

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyWarehouse {
  // primary key w_id
  int64_t w_id;
  StringStruct<10> w_name;
  StringStruct<20> w_street_1;
  StringStruct<20> w_street_2;
  StringStruct<20> w_city;
  StringStruct<2> w_state;
  StringStruct<9> w_zip;
  int8_t w_tax;   // signed num(4,4)
  int16_t w_ytd;  // signed num (12,2)
};

template <typename Locking = uint64_t>
struct District {
  // primary key (d_w_id, d_id)
  // d_w_id foreign key to w_id
  atom::ExtentVector<int64_t> d_id;
  atom::ExtentVector<int64_t> d_w_id;
  atom::ExtentVector<StringStruct<10>> d_name;
  atom::ExtentVector<StringStruct<20>> d_street_1;
  atom::ExtentVector<StringStruct<20>> d_street_2;
  atom::ExtentVector<StringStruct<20>> d_city;
  atom::ExtentVector<StringStruct<2>> d_state;
  atom::ExtentVector<StringStruct<9>> d_zip;
  atom::ExtentVector<int8_t> d_tax;   // signed num(4,4)
  atom::ExtentVector<int16_t> d_ytd;  // signed num (12,2)
  atom::ExtentVector<int64_t> d_next_o_id;

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyDistrict {
  // primary key (d_w_id, d_id)
  // d_w_id foreign key to w_id
  int64_t d_id;
  int64_t d_w_id;
  StringStruct<10> d_name;
  StringStruct<20> d_street_1;
  StringStruct<20> d_street_2;
  StringStruct<20> d_city;
  StringStruct<2> d_state;
  StringStruct<9> d_zip;
  int8_t d_tax;   // signed num(4,4)
  int16_t d_ytd;  // signed num (12,2)
  int64_t d_next_o_id;
};

template <typename Locking = uint64_t>
struct Customer {
  // primary key (c_w_id, c_d_id, c_id)
  // (c_w_id, c_d_id) foreign key to (d_w_id, d_id)
  atom::ExtentVector<int64_t> c_id;
  atom::ExtentVector<int64_t> c_d_id;
  atom::ExtentVector<int64_t> c_w_id;
  atom::ExtentVector<StringStruct<16>> c_first;
  atom::ExtentVector<StringStruct<2>> c_middle;
  atom::ExtentVector<StringStruct<16>> c_last;
  atom::ExtentVector<StringStruct<20>> c_street_1;
  atom::ExtentVector<StringStruct<20>> c_street_2;
  atom::ExtentVector<StringStruct<20>> c_city;
  atom::ExtentVector<StringStruct<2>> c_state;
  atom::ExtentVector<StringStruct<9>> c_zip;
  atom::ExtentVector<StringStruct<16>> c_phone;
  atom::ExtentVector<int32_t> c_since;  // date and time aka timestamp
  atom::ExtentVector<StringStruct<2>> c_credit;
  atom::ExtentVector<int16_t> c_credit_lim;   // signed num(12,2)
  atom::ExtentVector<int8_t> c_discount;      // signed num(4,4)
  atom::ExtentVector<int16_t> c_balance;      // signed num(12,2)
  atom::ExtentVector<int16_t> c_ytd_payment;  // signed num(12,2)
  atom::ExtentVector<int8_t> c_payment_cnt;   // num(4)
  atom::ExtentVector<int8_t> c_delivery_cnt;  // num(4)
  atom::ExtentVector<StringStruct<128>> c_data;

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyCustomer {
  // primary key (c_w_id, c_d_id, c_id)
  // (c_w_id, c_d_id) foreign key to (d_w_id, d_id)
  int64_t c_id;
  int64_t c_d_id;
  int64_t c_w_id;
  StringStruct<16> c_first;
  StringStruct<2> c_middle;
  StringStruct<16> c_last;
  StringStruct<20> c_street_1;
  StringStruct<20> c_street_2;
  StringStruct<20> c_city;
  StringStruct<2> c_state;
  StringStruct<9> c_zip;
  StringStruct<16> c_phone;
  int32_t c_since;  // date and time aka timestamp
  StringStruct<2> c_credit;
  int16_t c_credit_lim;   // signed num(12,2)
  int8_t c_discount;      // signed num(4,4)
  int16_t c_balance;      // signed num(12,2)
  int16_t c_ytd_payment;  // signed num(12,2)
  int8_t c_payment_cnt;   // num(4)
  int8_t c_delivery_cnt;  // num(4)
  StringStruct<128> c_data;
};

template <typename Locking = uint64_t>
struct History {
  atom::ExtentVector<int64_t> h_c_id;
  atom::ExtentVector<int8_t> h_c_d_id;
  atom::ExtentVector<int64_t> h_c_w_id;
  atom::ExtentVector<int8_t> h_d_id;
  atom::ExtentVector<int64_t> h_w_id;
  atom::ExtentVector<int64_t> h_date;
  atom::ExtentVector<double> h_amount;
  atom::ExtentVector<StringStruct<24>> h_data;

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyHistory {
  int64_t h_c_id;
  int8_t h_c_d_id;
  int64_t h_c_w_id;
  int8_t h_d_id;
  int64_t h_w_id;
  int64_t h_date;
  double h_amount;
  StringStruct<24> h_data;
};

template <typename Locking = uint64_t>
struct NewOrder {
  atom::ExtentVector<int64_t> no_o_id;
  atom::ExtentVector<int8_t> no_d_id;
  atom::ExtentVector<int64_t> no_w_id;

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyNewOrder {
  int64_t no_o_id;
  int8_t no_d_id;
  int64_t no_w_id;
};

template <typename Locking = uint64_t>
struct Order {
  atom::ExtentVector<int64_t> o_id;
  atom::ExtentVector<int64_t> o_c_id;
  atom::ExtentVector<int8_t> o_d_id;
  atom::ExtentVector<int64_t> o_w_id;
  atom::ExtentVector<int64_t> o_entry_d;
  atom::ExtentVector<int64_t> o_carrier_id;
  atom::ExtentVector<int8_t> o_ol_cnt;
  atom::ExtentVector<int8_t> o_all_local;

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyOrder {
  int64_t o_id;
  int64_t o_c_id;
  int8_t o_d_id;
  int64_t o_w_id;
  int64_t o_entry_d;
  int64_t o_carrier_id;
  int8_t o_ol_cnt;
  int8_t o_all_local;
};

template <typename Locking = uint64_t>
struct OrderLine {
  atom::ExtentVector<int64_t> ol_o_id;
  atom::ExtentVector<int8_t> ol_d_id;
  atom::ExtentVector<int64_t> ol_w_id;
  atom::ExtentVector<int8_t> ol_number;
  atom::ExtentVector<int64_t> ol_i_id;
  atom::ExtentVector<int64_t> ol_supply_w_id;
  atom::ExtentVector<int64_t> ol_delivery_d;
  atom::ExtentVector<int8_t> ol_quantity;
  atom::ExtentVector<double> ol_amount;
  atom::ExtentVector<StringStruct<24>> ol_dist_info;

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyOrderLine {
  int64_t ol_o_id;
  int8_t ol_d_id;
  int64_t ol_w_id;
  int8_t ol_number;
  int64_t ol_i_id;
  int64_t ol_supply_w_id;
  int64_t ol_delivery_d;
  int8_t ol_quantity;
  double ol_amount;
  StringStruct<24> ol_dist_info;
};

template <typename Locking = uint64_t>
struct Item {
  atom::ExtentVector<int64_t> i_id;
  atom::ExtentVector<int64_t> i_im_id;
  atom::ExtentVector<StringStruct<24>> i_name;
  atom::ExtentVector<double> i_price;
  atom::ExtentVector<StringStruct<50>> i_data;

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyItem {
  int64_t i_id;
  int64_t i_im_id;
  StringStruct<24> i_name;
  double i_price;
  StringStruct<50> i_data;
};

template <typename Locking = uint64_t>
struct Stock {
  atom::ExtentVector<int64_t> s_i_id;
  atom::ExtentVector<int64_t> s_w_id;
  atom::ExtentVector<int8_t> s_quantity;
  atom::ExtentVector<StringStruct<24>> s_dist_01;
  atom::ExtentVector<StringStruct<24>> s_dist_02;
  atom::ExtentVector<StringStruct<24>> s_dist_03;
  atom::ExtentVector<StringStruct<24>> s_dist_04;
  atom::ExtentVector<StringStruct<24>> s_dist_05;
  atom::ExtentVector<StringStruct<24>> s_dist_06;
  atom::ExtentVector<StringStruct<24>> s_dist_07;
  atom::ExtentVector<StringStruct<24>> s_dist_08;
  atom::ExtentVector<StringStruct<24>> s_dist_09;
  atom::ExtentVector<StringStruct<24>> s_dist_10;
  atom::ExtentVector<int64_t> s_ytd;
  atom::ExtentVector<int64_t> s_order_cnt;
  atom::ExtentVector<int64_t> s_remote_cnt;
  atom::ExtentVector<StringStruct<50>> s_data;

  atom::AtomicExtentVector<Locking> locked;
  atom::AtomicExtentVector<uint64_t> lsn;
  atom::AtomicExtentVector<atom::AtomicSinglyLinkedList<uint64_t>*> read_write_table;
  common::OptimisticPredicateLocking<common::ChunkAllocator>* opl;
};

struct SinglyStock {
  int64_t s_i_id;
  int64_t s_w_id;
  int8_t s_quantity;
  StringStruct<24> s_dist_01;
  StringStruct<24> s_dist_02;
  StringStruct<24> s_dist_03;
  StringStruct<24> s_dist_04;
  StringStruct<24> s_dist_05;
  StringStruct<24> s_dist_06;
  StringStruct<24> s_dist_07;
  StringStruct<24> s_dist_08;
  StringStruct<24> s_dist_09;
  StringStruct<24> s_dist_10;
  int64_t s_ytd;
  int64_t s_order_cnt;
  int64_t s_remote_cnt;
  StringStruct<50> s_data;
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

  Warehouse<Locking> warehouse;
  Item<Locking> item;
  District<Locking> district;
  Customer<Locking> customer;
  History<Locking> history;
  NewOrder<Locking> neworder;
  Order<Locking> order;
  OrderLine<Locking> orderline;
  Stock<Locking> stock;

  uint64_t number_warehouses = 1;
  uint64_t max_items = 100000;
  uint64_t dist_per_warehouse = 10;
  uint64_t cust_per_dist = 3000;
  double percent_payment = 0.5;
  bool insert_ = false;

  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           int64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                           common::ChunkAllocator>>
      item_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           int64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                           common::ChunkAllocator>>
      warehouse_map;

  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           int64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                           common::ChunkAllocator>>
      stock_map;

  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           int64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                           common::ChunkAllocator>>
      district_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           int64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                           common::ChunkAllocator>>
      customer_id_map;
  std::unique_ptr<atom::AtomicUnorderedMultiMap<uint64_t,
                                                int64_t,
                                                atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                common::ChunkAllocator>>
      customer_last_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           int64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                           common::ChunkAllocator>>
      order_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           int64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                           common::ChunkAllocator>>
      orderline_map;
  std::unique_ptr<atom::AtomicUnorderedMap<uint64_t,
                                           int64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                           common::ChunkAllocator>>
      orderline_wd_map;

  Database(uint64_t number_warehouses = 4, bool online = false)
      : tc(&ca, &emp, online),
        active_thr_(),
        wm_(std::thread::hardware_concurrency()),
        number_warehouses(number_warehouses) {}

  void bot(uint64_t transaction) { tc.bot(transaction); }
  void abort(uint64_t transaction) { tc.abort(transaction); }
  bool commit(uint64_t transaction, std::unordered_set<uint64_t>& oset) {
    bool commit = tc.commit(transaction, oset);
    common::OptimisticPredicateLocking<common::ChunkAllocator>::finishTransaction();
    return commit;
  }

  void generateRandomString(char* v, uint8_t length, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, 26);
    for (uint8_t i = 0; i < length; ++i) {
      v[i] = 65 + dis(gen);
    }
  }

  static void client(Database<TC, WM, Locking>& db, uint32_t population, int max_transactions, uint8_t core_id) {
    clientMultiRead<true>(db, population, max_transactions, core_id);
  }

  template <bool MultiReadPossible>
  static void clientMultiRead(Database<TC, WM, Locking>& db,
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

    SinglyWarehouse wh;
    SinglyCustomer c;
    SinglyDistrict d;
    SinglyStock s;
    SinglyItem si;
    PaymentVar payment{};
    NewOrderVar neworder{};

    for (int i = 0; i < max_transactions; ++i) {
      bool restart = false;
      uint64_t transaction = 0, old_transaction = 0;
      std::unordered_set<uint64_t> aborted_transaction;
      dc.startLatency();
      int tx = dis(gen) % 100;
      if (tx < 50) {
        db.genPayment(payment, core_id, gen);
      } else {
        db.genNewOrder(neworder, core_id, gen);
      }
    restart:
      transaction = db.tc.start();
      db.bot(transaction);
      int res = 0;
      dc.startTX();
      bool olap = false;

      if (tx < 50) {
        res = db.execPayment<MultiReadPossible>(payment, wh, d, c, transaction, gen);
      } else {
        res = db.execNewOrder<MultiReadPossible>(neworder, d, c, si, s, transaction, gen);
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

  void populateDatabase(uint64_t database_size) {
    std::random_device rd;
    std::mt19937 gen(rd());
    reserveSlots(gen);
    loadItems(gen);
    for (auto w = 1u; w <= number_warehouses; w++) {
      loadWarehouse(w, gen);
      loadStock(w, gen);
      loadDistricts(w, gen);
      for (uint64_t d = 1; d <= dist_per_warehouse; d++) {
        loadCustomers(w, d, gen);
        loadOrders(w, d, gen);
        for (uint64_t c = 1; c <= cust_per_dist; c++) {
          loadHistory(w, d, c, gen);
        }
      }
    }
  }

  void reserveSlots(std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());

    item.i_data.reserve(max_items);
    item.i_id.reserve(max_items);
    item.i_name.reserve(max_items);
    item.i_im_id.reserve(max_items);
    item.i_price.reserve(max_items);
    item.lsn.reserve(max_items);
    item.locked.reserve(max_items);
    item.read_write_table.reserve(max_items);
    item.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    item_map =
        std::make_unique<atom::AtomicUnorderedMap<uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                  common::ChunkAllocator>>(max_items, &ca, &emp);
    warehouse.w_id.reserve(number_warehouses);
    warehouse.w_name.reserve(number_warehouses);
    warehouse.w_street_1.reserve(number_warehouses);
    warehouse.w_street_2.reserve(number_warehouses);
    warehouse.w_city.reserve(number_warehouses);
    warehouse.w_state.reserve(number_warehouses);
    warehouse.w_zip.reserve(number_warehouses);
    warehouse.w_tax.reserve(number_warehouses);
    warehouse.w_ytd.reserve(number_warehouses);
    warehouse.lsn.reserve(number_warehouses);
    warehouse.locked.reserve(number_warehouses);
    warehouse.read_write_table.reserve(number_warehouses);
    warehouse.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    warehouse_map =
        std::make_unique<atom::AtomicUnorderedMap<uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                  common::ChunkAllocator>>(number_warehouses, &ca, &emp);
    stock.s_i_id.reserve(max_items * number_warehouses);
    stock.s_w_id.reserve(max_items * number_warehouses);
    stock.s_quantity.reserve(max_items * number_warehouses);
    stock.s_dist_01.reserve(max_items * number_warehouses);
    stock.s_dist_02.reserve(max_items * number_warehouses);
    stock.s_dist_03.reserve(max_items * number_warehouses);
    stock.s_dist_04.reserve(max_items * number_warehouses);
    stock.s_dist_05.reserve(max_items * number_warehouses);
    stock.s_dist_06.reserve(max_items * number_warehouses);
    stock.s_dist_07.reserve(max_items * number_warehouses);
    stock.s_dist_08.reserve(max_items * number_warehouses);
    stock.s_dist_09.reserve(max_items * number_warehouses);
    stock.s_dist_10.reserve(max_items * number_warehouses);
    stock.s_ytd.reserve(max_items * number_warehouses);
    stock.s_order_cnt.reserve(max_items * number_warehouses);
    stock.s_remote_cnt.reserve(max_items * number_warehouses);
    stock.s_data.reserve(max_items * number_warehouses);
    stock.lsn.reserve(max_items * number_warehouses);
    stock.locked.reserve(max_items * number_warehouses);
    stock.read_write_table.reserve(max_items * number_warehouses);
    stock.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    stock_map =
        std::make_unique<atom::AtomicUnorderedMap<uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                  common::ChunkAllocator>>(max_items * number_warehouses, &ca, &emp);

    district.d_id.reserve(dist_per_warehouse * number_warehouses);
    district.d_w_id.reserve(dist_per_warehouse * number_warehouses);
    district.d_name.reserve(dist_per_warehouse * number_warehouses);
    district.d_street_1.reserve(dist_per_warehouse * number_warehouses);
    district.d_street_2.reserve(dist_per_warehouse * number_warehouses);
    district.d_city.reserve(dist_per_warehouse * number_warehouses);
    district.d_state.reserve(dist_per_warehouse * number_warehouses);
    district.d_zip.reserve(dist_per_warehouse * number_warehouses);
    district.d_tax.reserve(dist_per_warehouse * number_warehouses);
    district.d_ytd.reserve(dist_per_warehouse * number_warehouses);
    district.d_next_o_id.reserve(dist_per_warehouse * number_warehouses);
    district.lsn.reserve(dist_per_warehouse * number_warehouses);
    district.locked.reserve(dist_per_warehouse * number_warehouses);
    district.read_write_table.reserve(dist_per_warehouse * number_warehouses);
    district.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    district_map =
        std::make_unique<atom::AtomicUnorderedMap<uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                  common::ChunkAllocator>>(dist_per_warehouse * number_warehouses, &ca,
                                                                           &emp);

    customer.c_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_d_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_w_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_first.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_middle.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_last.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_street_1.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_street_2.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_city.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_state.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_zip.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_phone.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_since.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);  // date and time aka timestamp
    customer.c_credit.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.c_credit_lim.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);    // signed num(12,2)
    customer.c_discount.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);      // signed num(4,4)
    customer.c_balance.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);       // signed num(12,2)
    customer.c_ytd_payment.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);   // signed num(12,2)
    customer.c_payment_cnt.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);   // num(4)
    customer.c_delivery_cnt.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);  // num(4)
    customer.c_data.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.lsn.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.locked.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.read_write_table.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    customer.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    customer_id_map =
        std::make_unique<atom::AtomicUnorderedMap<uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                  common::ChunkAllocator>>(
            cust_per_dist * dist_per_warehouse * number_warehouses, &ca, &emp);
    customer_last_map = std::make_unique<atom::AtomicUnorderedMultiMap<
        uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>, common::ChunkAllocator>>(
        cust_per_dist * dist_per_warehouse * number_warehouses, &ca, &emp);

    history.h_c_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.h_c_d_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.h_c_w_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.h_d_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.h_w_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.h_date.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.h_amount.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.h_data.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.lsn.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.locked.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.read_write_table.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    history.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    order.o_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.o_c_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.o_d_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.o_w_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.o_entry_d.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.o_carrier_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.o_ol_cnt.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.o_all_local.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.lsn.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.locked.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.read_write_table.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    order.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    order_map =
        std::make_unique<atom::AtomicUnorderedMap<uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                  common::ChunkAllocator>>(
            cust_per_dist * dist_per_warehouse * number_warehouses, &ca, &emp);

    orderline.ol_o_id.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_d_id.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_w_id.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_number.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_i_id.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_supply_w_id.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_delivery_d.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_quantity.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_amount.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.ol_dist_info.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.lsn.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.locked.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.read_write_table.reserve(10 * cust_per_dist * dist_per_warehouse * number_warehouses);
    orderline.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};

    orderline_map =
        std::make_unique<atom::AtomicUnorderedMap<uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                  common::ChunkAllocator>>(
            10 * cust_per_dist * dist_per_warehouse * number_warehouses, &ca, &emp);

    orderline_wd_map =
        std::make_unique<atom::AtomicUnorderedMap<uint64_t, int64_t, atom::AtomicUnorderedMapBucket<uint64_t, int64_t>,
                                                  common::ChunkAllocator>>(
            10 * cust_per_dist * dist_per_warehouse * number_warehouses, &ca, &emp);

    neworder.no_o_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    neworder.no_d_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    neworder.no_w_id.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    neworder.lsn.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    neworder.locked.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    neworder.read_write_table.reserve(cust_per_dist * dist_per_warehouse * number_warehouses);
    neworder.opl = new common::OptimisticPredicateLocking<common::ChunkAllocator>{&ca, &emp};
  }

  uint64_t getLastname(uint64_t random, char* name) {
    memset(name, '\0', 16);
    static const char* n[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
    strcpy(name, n[random / 100]);
    strcat(name, n[(random / 10) % 10]);
    strcat(name, n[random % 10]);
    return strlen(name);
  }

  uint64_t nonUniformRandom(uint64_t A, uint64_t C, uint64_t minV, uint64_t maxV, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(minV, maxV + 1);
    std::uniform_int_distribution<unsigned int> disA(0, A + 1);

    return (((disA(gen) | dis(gen)) + C) % (maxV - minV + 1)) + minV;
  }

  int64_t stockKey(int64_t i, int64_t w) { return w * max_items + i; }
  int64_t distKey(int64_t d, int64_t w) { return w * dist_per_warehouse + d; }
  int64_t custKey(int64_t c, int64_t d, int64_t w) { return (distKey(d, w) * cust_per_dist + c); }
  int64_t orderPrimaryKey(uint64_t w, int64_t d, int64_t o) { return (distKey(d, w) << 32) + o; }
  int64_t custNPKey(char* c, int64_t d, int64_t w) {
    int64_t key = 0;
    char offset = 'A';
    for (uint32_t i = 0; i < 16; i++)
      key = (key << 2) + (c[i] - offset);
    key = key << 3;
    key += w * dist_per_warehouse + d;
    return key;
  }

  void loadItems(std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    for (uint64_t i = 1; i <= max_items; ++i) {
      auto id = item.i_id.push_back(i);
      StringStruct<24> stringstruct_24;
      generateRandomString(stringstruct_24.string, 24, gen);
      item.i_name.push_back(stringstruct_24);
      item.i_price.push_back((100.0 + (dis(gen) % 900)) / 100.0);
      item.i_im_id.push_back(1 + (dis(gen) % 10000));
      StringStruct<50> stringstruct_50;
      generateRandomString(stringstruct_50.string, dis(gen) % 24 + 26, gen);
      if (dis(gen) % 10 == 0)
        strcpy(stringstruct_50.string + dis(gen) % 12, "original");
      item.i_data.push_back(stringstruct_50);

      item.lsn.push_back(0);
      item.locked.push_back(static_cast<Locking>(0));
      item.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
      item_map->insert(i, id);
    }
  }

  void loadStock(uint64_t w, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    for (uint64_t i = 1; i <= max_items; ++i) {
      auto id = stock.s_i_id.push_back(i);
      stock.s_w_id.push_back(w);
      stock.s_ytd.push_back(0);
      stock.s_order_cnt.push_back(0);
      stock.s_remote_cnt.push_back(0);
      stock.s_quantity.push_back(10 + (dis(gen) % 90));
      StringStruct<50> stringstruct_50;
      generateRandomString(stringstruct_50.string, dis(gen) % 24 + 26, gen);
      if (dis(gen) % 10 == 0)
        strcpy(stringstruct_50.string + dis(gen) % 12, "original");
      stock.s_data.push_back(stringstruct_50);
      stock.lsn.push_back(0);
      stock.locked.push_back(static_cast<Locking>(0));
      stock.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
      stock_map->insert(stockKey(i, w), id);
    }
  }

  void loadWarehouse(uint64_t w, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    auto id = warehouse.w_id.push_back(w);
    StringStruct<10> stringstruct_10;
    generateRandomString(stringstruct_10.string, dis(gen) % 6 + 4, gen);
    warehouse.w_name.push_back(stringstruct_10);
    StringStruct<20> stringstruct_20;
    generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
    warehouse.w_street_1.push_back(stringstruct_20);
    generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
    warehouse.w_street_2.push_back(stringstruct_20);
    generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
    warehouse.w_city.push_back(stringstruct_20);
    StringStruct<2> stringstruct_2;
    generateRandomString(stringstruct_2.string, 2, gen);
    warehouse.w_state.push_back(stringstruct_2);
    StringStruct<9> stringstruct_9;
    getNumberString(dis(gen), stringstruct_9.string, 9);
    warehouse.w_zip.push_back(stringstruct_9);
    warehouse.w_ytd.push_back(30000.00);
    warehouse.w_tax.push_back(((double)(dis(gen) % 200)) / 1000.0);
    warehouse.lsn.push_back(0);
    warehouse.locked.push_back(static_cast<Locking>(0));
    warehouse.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

    warehouse_map->insert(w, id);
  }

  void loadDistricts(uint64_t w, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    for (uint64_t i = 1; i <= dist_per_warehouse; ++i) {
      auto id = district.d_id.push_back(i);
      district.d_w_id.push_back(w);
      StringStruct<10> stringstruct_10;
      generateRandomString(stringstruct_10.string, dis(gen) % 6 + 4, gen);
      district.d_name.push_back(stringstruct_10);
      StringStruct<20> stringstruct_20;
      generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
      district.d_street_1.push_back(stringstruct_20);
      generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
      district.d_street_2.push_back(stringstruct_20);
      generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
      district.d_city.push_back(stringstruct_20);
      StringStruct<2> stringstruct_2;
      generateRandomString(stringstruct_2.string, 2, gen);
      district.d_state.push_back(stringstruct_2);
      StringStruct<9> stringstruct_9;
      getNumberString(dis(gen), stringstruct_9.string, 9);
      district.d_zip.push_back(stringstruct_9);
      district.d_ytd.push_back(30000.00);
      district.d_tax.push_back(((double)(dis(gen) % 200)) / 1000.0);
      district.d_next_o_id.push_back(cust_per_dist + 1);

      district.lsn.push_back(0);
      district.locked.push_back(static_cast<Locking>(0));
      district.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

      district_map->insert(distKey(i, w), id);
    }
  }

  void loadCustomers(uint64_t w, uint64_t d, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    for (uint64_t i = 1; i <= cust_per_dist; ++i) {
      auto id = customer.c_id.push_back(i);
      customer.c_w_id.push_back(w);
      customer.c_d_id.push_back(d);
      StringStruct<16> stringstruct_16;
      generateRandomString(stringstruct_16.string, dis(gen) % 8 + 8, gen);
      customer.c_first.push_back(stringstruct_16);
      if (i <= 1000) {
        getLastname(i - 1, stringstruct_16.string);
      } else {
        getLastname(nonUniformRandom(255, 157, 0, 999, gen), stringstruct_16.string);
      }
      customer.c_last.push_back(stringstruct_16);
      customer.c_discount.push_back((float)((dis(gen) % 5000) / 10000.0));
      StringStruct<2> stringstruct_2;
      if (dis(gen) % 100 <= 10) {
        stringstruct_2.string[0] = 'B';
      } else {
        stringstruct_2.string[0] = 'G';
      }
      stringstruct_2.string[1] = 'C';
      customer.c_credit.push_back(stringstruct_2);

      customer.c_credit_lim.push_back(50000);

      customer.c_balance.push_back(-10);
      customer.c_ytd_payment.push_back(10);
      customer.c_payment_cnt.push_back(1);
      customer.c_delivery_cnt.push_back(0);

      StringStruct<20> stringstruct_20;
      generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
      customer.c_street_1.push_back(stringstruct_20);
      generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
      customer.c_street_2.push_back(stringstruct_20);
      generateRandomString(stringstruct_20.string, dis(gen) % 10 + 10, gen);
      customer.c_city.push_back(stringstruct_20);
      generateRandomString(stringstruct_2.string, 2, gen);
      customer.c_state.push_back(stringstruct_2);
      StringStruct<9> stringstruct_9;
      getNumberString(dis(gen), stringstruct_9.string, 9);
      customer.c_zip.push_back(stringstruct_9);

      stringstruct_2.string[0] = 'O';
      stringstruct_2.string[1] = 'E';
      customer.c_middle.push_back(stringstruct_2);
      StringStruct<128> stringstruct_128;
      generateRandomString(stringstruct_128.string, dis(gen) % 64 + 64, gen);
      customer.c_data.push_back(stringstruct_128);
      customer.c_since.push_back((unsigned)time(NULL));

      customer.lsn.push_back(0);
      customer.locked.push_back(static_cast<Locking>(0));
      customer.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

      customer_id_map->insert(custKey(i, d, w), id);
      customer_last_map->insert(custNPKey(stringstruct_16.string, d, w), id);
    }
  }

  void loadOrders(uint64_t w, uint64_t d, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    uint64_t perm[cust_per_dist];
    for (uint64_t i = 1; i <= cust_per_dist; i++)
      perm[i - 1] = i;
    std::random_shuffle(perm, perm + cust_per_dist);

    for (uint64_t i = 1; i <= cust_per_dist; ++i) {
      auto id = order.o_id.push_back(i);
      order.o_c_id.push_back(perm[i - 1]);
      order.o_d_id.push_back(d);
      order.o_w_id.push_back(w);
      unsigned o_entry = (unsigned)time(NULL);
      order.o_entry_d.push_back(o_entry);
      if (i < 2101)
        order.o_carrier_id.push_back(dis(gen) % 9 + 1);
      else
        order.o_carrier_id.push_back(0);
      uint64_t ol_cnt = dis(gen) % 5 + 10;
      order.o_ol_cnt.push_back(ol_cnt);
      order.o_all_local.push_back(1);

      order.lsn.push_back(0);
      order.locked.push_back(static_cast<Locking>(0));
      order.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});

      order_map->insert(orderPrimaryKey(w, d, i), id);

      for (uint32_t ol = 1; ol <= ol_cnt; ol++) {
        orderline.ol_o_id.push_back(i);
        orderline.ol_d_id.push_back(d);
        orderline.ol_w_id.push_back(w);
        orderline.ol_number.push_back(ol);
        orderline.ol_i_id.push_back(dis(gen) % 100000 + 1);
        orderline.ol_supply_w_id.push_back(w);
        if (i < 2101) {
          orderline.ol_delivery_d.push_back(o_entry);
          orderline.ol_amount.push_back(0);
        } else {
          orderline.ol_delivery_d.push_back(0);
          orderline.ol_amount.push_back((double)(dis(gen) % 999999 + 1) / 100.0);
        }
        orderline.ol_quantity.push_back(5);
        StringStruct<24> stringstruct_24;
        generateRandomString(stringstruct_24.string, 24, gen);
        orderline.ol_dist_info.push_back(stringstruct_24);

        orderline.lsn.push_back(0);
        orderline.locked.push_back(static_cast<Locking>(0));
        orderline.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
      }

      if (i > 2100) {
        neworder.no_d_id.push_back(d);
        neworder.no_w_id.push_back(w);
        neworder.no_o_id.push_back(i);

        neworder.lsn.push_back(0);
        neworder.locked.push_back(static_cast<Locking>(0));
        neworder.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
      }
    }
  }

  void loadHistory(uint64_t w, uint64_t d, uint64_t c, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    history.h_c_id.push_back(c);
    history.h_c_d_id.push_back(d);
    history.h_c_w_id.push_back(w);
    history.h_d_id.push_back(d);
    history.h_w_id.push_back(w);
    history.h_date.push_back((unsigned)time(NULL));
    history.h_amount.push_back(10);
    StringStruct<24> stringstruct_24;
    generateRandomString(stringstruct_24.string, dis(gen) % 10 + 14, gen);
    history.h_data.push_back(stringstruct_24);

    history.lsn.push_back(0);
    history.locked.push_back(static_cast<Locking>(0));
    history.read_write_table.push_back(new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp});
  }

  void deleteDatabase() {
    for (uint64_t i = 0; i < item.read_write_table.size(); i++)
      delete item.read_write_table[i];
    for (uint64_t i = 0; i < stock.read_write_table.size(); i++)
      delete stock.read_write_table[i];
    for (uint64_t i = 0; i < history.read_write_table.size(); i++)
      delete history.read_write_table[i];
    for (uint64_t i = 0; i < customer.read_write_table.size(); i++)
      delete customer.read_write_table[i];
    for (uint64_t i = 0; i < district.read_write_table.size(); i++)
      delete district.read_write_table[i];
    for (uint64_t i = 0; i < warehouse.read_write_table.size(); i++)
      delete warehouse.read_write_table[i];
    for (uint64_t i = 0; i < order.read_write_table.size(); i++)
      delete order.read_write_table[i];
    for (uint64_t i = 0; i < orderline.read_write_table.size(); i++)
      delete orderline.read_write_table[i];
    for (uint64_t i = 0; i < neworder.read_write_table.size(); i++)
      delete neworder.read_write_table[i];
  }

  struct PaymentVar {
    int64_t w, d, c, c_d, c_w;
    int16_t amount;
    StringStruct<16> lastname;
    bool by_lastname;
  };

  void genPayment(PaymentVar& payment, uint32_t core_id, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    payment.w = core_id % number_warehouses + 1;
    payment.d = dis(gen) % dist_per_warehouse + 1;
    uint64_t x = dis(gen) % 100;
    uint64_t y = dis(gen) % 100;
    payment.amount = dis(gen) % 5000 + 1;
    if (x < 85) {
      payment.c_d = payment.d;
      payment.c_w = payment.w;
    } else {
      payment.c_d = dis(gen) % dist_per_warehouse + 1;
      do {
        payment.c_w = dis(gen) % number_warehouses + 1;
      } while (payment.c_w == payment.w && number_warehouses > 1);
    }

    if (y < 60) {
      payment.by_lastname = true;
      getLastname(nonUniformRandom(255, 223, 0, 999, gen), payment.lastname.string);
    } else {
      payment.by_lastname = false;
      payment.c = nonUniformRandom(255, 259, 1, cust_per_dist, gen);
    }
  }

  template <bool T, typename std::enable_if_t<T>* = nullptr>
  int execPayment(PaymentVar& payment,
                  SinglyWarehouse& wh,
                  SinglyDistrict& d,
                  SinglyCustomer& c,
                  uint64_t transaction,
                  std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());

    uint64_t offset;
    int64_t wk = payment.w;
    bool found = warehouse.opl->lookup(wk, offset, *warehouse_map,
                                       [](auto& wmap, auto& o, auto& w) { return wmap.lookup(w, o); });
    if (!found)
      return 0;

    {
      sv::ReadGuard<TC, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList> rg{
          &tc, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset, transaction};

      if (!rg.wasSuccessful()) {
        return -1;
      } else {
        wh.w_ytd = warehouse.w_ytd[offset];
        wh.w_street_1 = warehouse.w_street_1[offset];
        wh.w_street_2 = warehouse.w_street_2[offset];
        wh.w_city = warehouse.w_city[offset];
        wh.w_state = warehouse.w_state[offset];
        wh.w_zip = warehouse.w_zip[offset];
        wh.w_name = warehouse.w_name[offset];
        wh.w_id = warehouse.w_id[offset];
      }
    }

    wh.w_ytd += payment.amount;
    auto check = tc.writeValue(wh.w_ytd, warehouse.w_ytd, warehouse.lsn, warehouse.read_write_table, warehouse.locked,
                               offset, transaction);
    if (!check) {
      return -1;
    }

    int64_t dk = distKey(payment.d, payment.w);
    found =
        district.opl->lookup(dk, offset, *district_map, [](auto& wmap, auto& o, auto& w) { return wmap.lookup(w, o); });
    if (!found)
      return 0;

    {
      sv::ReadGuard<TC, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList> rg{
          &tc, district.lsn, district.read_write_table, district.locked, offset, transaction};

      if (!rg.wasSuccessful()) {
        return -1;
      } else {
        d.d_ytd = district.d_ytd[offset];
        d.d_street_1 = district.d_street_1[offset];
        d.d_street_2 = district.d_street_2[offset];
        d.d_city = district.d_city[offset];
        d.d_state = district.d_state[offset];
        d.d_zip = district.d_zip[offset];
        d.d_name = district.d_name[offset];
        d.d_w_id = district.d_w_id[offset];
        d.d_id = district.d_id[offset];
      }
    }

    d.d_ytd += payment.amount;
    check = tc.writeValue(d.d_ytd, district.d_ytd, district.lsn, district.read_write_table, district.locked, offset,
                          transaction);
    if (!check) {
      return -1;
    }

    if (payment.by_lastname) {
      std::vector<uint64_t> offsetv;
      int64_t ck = custNPKey(payment.lastname.string, payment.d, payment.w);
      found = customer.opl->lookup(ck, offsetv, *customer_last_map,
                                   [](auto& cmap, auto& o, auto& w) { return cmap.lookup(w, o); });
      if (!found)
        return 0;

      int index = offsetv.size() / 2;
      if (offsetv.size() % 2 == 0) {
        index -= 1;
      }
      offset = offsetv[index];
    } else {
      int64_t ck = custKey(payment.c, payment.d, payment.w);
      found = customer.opl->lookup(ck, offset, *customer_id_map,
                                   [](auto& cmap, auto& o, auto& w) { return cmap.lookup(w, o); });
      if (!found)
        return 0;
    }

    {
      sv::ReadGuard<TC, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList> rg{
          &tc, customer.lsn, customer.read_write_table, customer.locked, offset, transaction};

      if (!rg.wasSuccessful()) {
        return -1;
      } else {
        c.c_street_1 = customer.c_street_1[offset];
        c.c_street_2 = customer.c_street_2[offset];
        c.c_city = customer.c_city[offset];
        c.c_state = customer.c_state[offset];
        c.c_zip = customer.c_zip[offset];
        c.c_first = customer.c_first[offset];
        c.c_last = customer.c_last[offset];
        c.c_credit = customer.c_credit[offset];
        c.c_credit_lim = customer.c_credit_lim[offset];
        c.c_balance = customer.c_balance[offset];
        c.c_since = customer.c_since[offset];
        c.c_discount = customer.c_discount[offset];
        c.c_ytd_payment = customer.c_ytd_payment[offset];
        c.c_payment_cnt = customer.c_payment_cnt[offset];
      }
    }

    c.c_balance -= payment.amount;
    c.c_ytd_payment += payment.amount;
    c.c_payment_cnt++;
    check = tc.writeValue(c.c_balance, customer.c_balance, customer.lsn, customer.read_write_table, customer.locked,
                          offset, transaction);
    check &= tc.writeValue(c.c_ytd_payment, customer.c_balance, customer.lsn, customer.read_write_table,
                           customer.locked, offset, transaction);
    check &= tc.writeValue(c.c_payment_cnt, customer.c_payment_cnt, customer.lsn, customer.read_write_table,
                           customer.locked, offset, transaction);

    if (c.c_credit.string[1] == 'B') {
      // bad credit
      generateRandomString(c.c_data.string, dis(gen) % 64 + 64, gen);
      check &= tc.writeValue(c.c_data, customer.c_data, customer.lsn, customer.read_write_table, customer.locked,
                             offset, transaction);
    }

    if (!check) {
      return -1;
    }

    if (insert_) {
      uint64_t main_id = history.h_c_d_id.push_back(payment.c_d);
      uint64_t sub_id = history.h_c_w_id.push_back(payment.c_w);
      if (main_id != sub_id) {
        while (!history.h_c_w_id.isAlive(main_id)) {
        }
        history.h_c_w_id.replace(main_id, payment.c_w);
      }

      sub_id = history.h_c_id.push_back(payment.c);
      if (main_id != sub_id) {
        while (!history.h_c_id.isAlive(main_id)) {
        }
        history.h_c_id.replace(main_id, payment.c);
      }

      sub_id = history.h_d_id.push_back(payment.d);
      if (main_id != sub_id) {
        while (!history.h_d_id.isAlive(main_id)) {
        }
        history.h_d_id.replace(main_id, payment.d);
      }

      sub_id = history.h_w_id.push_back(payment.w);
      if (main_id != sub_id) {
        while (!history.h_w_id.isAlive(main_id)) {
        }
        history.h_w_id.replace(main_id, payment.w);
      }

      sub_id = history.h_date.push_back(time(NULL));
      if (main_id != sub_id) {
        while (!history.h_date.isAlive(main_id)) {
        }
        history.h_date.replace(main_id, time(NULL));
      }

      sub_id = history.h_amount.push_back(payment.amount);
      if (main_id != sub_id) {
        while (!history.h_amount.isAlive(main_id)) {
        }
        history.h_amount.replace(main_id, payment.amount);
      }

      sub_id = history.lsn.push_back(0);
      if (main_id != sub_id) {
        while (!history.lsn.isAlive(main_id)) {
        }
        history.lsn.atomic_replace(main_id, 0);
      }

      sub_id = history.locked.push_back(static_cast<Locking>(0));
      if (main_id != sub_id) {
        while (!history.locked.isAlive(main_id)) {
        }
        history.locked.atomic_replace(main_id, static_cast<Locking>(0));
      }

      auto ptr = new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp};
      sub_id = history.read_write_table.push_back(ptr);
      if (main_id != sub_id) {
        while (!history.read_write_table.isAlive(main_id)) {
        }
        history.read_write_table.atomic_replace(main_id, ptr);
      }
    }

    return 1;
  };

  template <bool T, typename std::enable_if_t<!T>* = nullptr>
  int execPayment(PaymentVar& payment,
                  SinglyWarehouse& wh,
                  SinglyDistrict& d,
                  SinglyCustomer& c,
                  uint64_t transaction,
                  std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    uint64_t offset;
    int64_t wk = payment.w;
    bool found = warehouse.opl->lookup(wk, offset, *warehouse_map,
                                       [](auto& wmap, auto& o, auto& w) { return wmap.lookup(w, o); });
    if (!found)
      return 0;

    {
      bool check = false;
      while (!check) {
        auto ret = tc.read(warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset, transaction);
        tc.pureValue(wh.w_ytd, warehouse.w_ytd, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset,
                     transaction);
        tc.pureValue(wh.w_street_1, warehouse.w_street_1, warehouse.lsn, warehouse.read_write_table, warehouse.locked,
                     offset, transaction);
        tc.pureValue(wh.w_street_1, warehouse.w_street_1, warehouse.lsn, warehouse.read_write_table, warehouse.locked,
                     offset, transaction);
        tc.pureValue(wh.w_city, warehouse.w_city, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset,
                     transaction);
        tc.pureValue(wh.w_state, warehouse.w_state, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset,
                     transaction);
        tc.pureValue(wh.w_zip, warehouse.w_zip, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset,
                     transaction);
        tc.pureValue(wh.w_name, warehouse.w_name, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset,
                     transaction);
        tc.pureValue(wh.w_id, warehouse.w_id, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset,
                     transaction);
        check = tc.readUndo(ret, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset, transaction);
      }
    }

    wh.w_ytd += payment.amount;
    auto check = tc.writeValue(wh.w_ytd, warehouse.w_ytd, warehouse.lsn, warehouse.read_write_table, warehouse.locked,
                               offset, transaction);
    if (!check) {
      return -1;
    }

    int64_t dk = distKey(payment.d, payment.w);
    found =
        district.opl->lookup(dk, offset, *district_map, [](auto& wmap, auto& o, auto& w) { return wmap.lookup(w, o); });
    if (!found)
      return 0;

    {
      bool check = false;
      while (!check) {
        auto ret = tc.read(district.lsn, district.read_write_table, district.locked, offset, transaction);
        tc.pureValue(d.d_ytd, district.d_ytd, district.lsn, district.read_write_table, district.locked, offset,
                     transaction);
        tc.pureValue(d.d_street_1, district.d_street_1, district.lsn, district.read_write_table, district.locked,
                     offset, transaction);
        tc.pureValue(d.d_street_1, district.d_street_1, district.lsn, district.read_write_table, district.locked,
                     offset, transaction);
        tc.pureValue(d.d_city, district.d_city, district.lsn, district.read_write_table, district.locked, offset,
                     transaction);
        tc.pureValue(d.d_state, district.d_state, district.lsn, district.read_write_table, district.locked, offset,
                     transaction);
        tc.pureValue(d.d_zip, district.d_zip, district.lsn, district.read_write_table, district.locked, offset,
                     transaction);
        tc.pureValue(d.d_name, district.d_name, district.lsn, district.read_write_table, district.locked, offset,
                     transaction);
        tc.pureValue(d.d_id, district.d_id, district.lsn, district.read_write_table, district.locked, offset,
                     transaction);
        check = tc.readUndo(ret, district.lsn, district.read_write_table, district.locked, offset, transaction);
      }
    }

    d.d_ytd += payment.amount;
    check = tc.writeValue(d.d_ytd, district.d_ytd, district.lsn, district.read_write_table, district.locked, offset,
                          transaction);
    if (!check) {
      return -1;
    }

    if (payment.by_lastname) {

      std::vector<uint64_t> offsetv;
      int64_t ck = custNPKey(payment.lastname.string, payment.d, payment.w);
      found = customer.opl->lookup(ck, offsetv, *customer_last_map,
                                   [](auto& cmap, auto& o, auto& w) { return cmap.lookup(w, o); });
      if (!found)
        return 0;

      int index = offsetv.size() / 2;
      if (offsetv.size() % 2 == 0) {
        index -= 1;
      }
      offset = offsetv[index];
    } else {
      int64_t ck = custKey(payment.c, payment.d, payment.w);
      found = customer.opl->lookup(ck, offset, *customer_id_map,
                                   [](auto& cmap, auto& o, auto& w) { return cmap.lookup(w, o); });
      if (!found)
        return 0;
    }

    {
      bool check = false;
      while (!check) {
        auto ret = tc.read(customer.lsn, customer.read_write_table, customer.locked, offset, transaction);
        tc.pureValue(c.c_ytd_payment, customer.c_ytd_payment, customer.lsn, customer.read_write_table, customer.locked,
                     offset, transaction);
        tc.pureValue(c.c_street_1, customer.c_street_1, customer.lsn, customer.read_write_table, customer.locked,
                     offset, transaction);
        tc.pureValue(c.c_street_1, customer.c_street_1, customer.lsn, customer.read_write_table, customer.locked,
                     offset, transaction);
        tc.pureValue(c.c_city, customer.c_city, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_state, customer.c_state, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_zip, customer.c_zip, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_first, customer.c_first, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_last, customer.c_last, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_credit, customer.c_credit, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_credit_lim, customer.c_credit_lim, customer.lsn, customer.read_write_table, customer.locked,
                     offset, transaction);
        tc.pureValue(c.c_balance, customer.c_balance, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_since, customer.c_since, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_discount, customer.c_discount, customer.lsn, customer.read_write_table, customer.locked,
                     offset, transaction);
        tc.pureValue(c.c_payment_cnt, customer.c_payment_cnt, customer.lsn, customer.read_write_table, customer.locked,
                     offset, transaction);
        check = tc.readUndo(ret, customer.lsn, customer.read_write_table, customer.locked, offset, transaction);
      }
    }

    c.c_balance -= payment.amount;
    c.c_ytd_payment += payment.amount;
    c.c_payment_cnt++;
    check = tc.writeValue(c.c_balance, customer.c_balance, customer.lsn, customer.read_write_table, customer.locked,
                          offset, transaction);
    check &= tc.writeValue(c.c_ytd_payment, customer.c_balance, customer.lsn, customer.read_write_table,
                           customer.locked, offset, transaction);
    check &= tc.writeValue(c.c_payment_cnt, customer.c_payment_cnt, customer.lsn, customer.read_write_table,
                           customer.locked, offset, transaction);

    if (c.c_credit.string[1] == 'B') {
      // bad credit
      generateRandomString(c.c_data.string, dis(gen) % 64 + 64, gen);
      check &= tc.writeValue(c.c_data, customer.c_data, customer.lsn, customer.read_write_table, customer.locked,
                             offset, transaction);
    }

    if (!check) {
      return -1;
    }

    if (insert_) {
      uint64_t main_id = history.h_c_d_id.push_back(payment.c_d);
      uint64_t sub_id = history.h_c_w_id.push_back(payment.c_w);
      if (main_id != sub_id) {
        while (!history.h_c_w_id.isAlive(main_id)) {
        }
        history.h_c_w_id.replace(main_id, payment.c_w);
      }

      sub_id = history.h_c_id.push_back(payment.c);
      if (main_id != sub_id) {
        while (!history.h_c_id.isAlive(main_id)) {
        }
        history.h_c_id.replace(main_id, payment.c);
      }

      sub_id = history.h_d_id.push_back(payment.d);
      if (main_id != sub_id) {
        while (!history.h_d_id.isAlive(main_id)) {
        }
        history.h_d_id.replace(main_id, payment.d);
      }

      sub_id = history.h_w_id.push_back(payment.w);
      if (main_id != sub_id) {
        while (!history.h_w_id.isAlive(main_id)) {
        }
        history.h_w_id.replace(main_id, payment.w);
      }

      sub_id = history.h_date.push_back(time(NULL));
      if (main_id != sub_id) {
        while (!history.h_date.isAlive(main_id)) {
        }
        history.h_date.replace(main_id, time(NULL));
      }

      sub_id = history.h_amount.push_back(payment.amount);
      if (main_id != sub_id) {
        while (!history.h_amount.isAlive(main_id)) {
        }
        history.h_amount.replace(main_id, payment.amount);
      }

      sub_id = history.lsn.push_back(0);
      if (main_id != sub_id) {
        while (!history.lsn.isAlive(main_id)) {
        }
        history.lsn.atomic_replace(main_id, 0);
      }

      sub_id = history.locked.push_back(static_cast<Locking>(0));
      if (main_id != sub_id) {
        while (!history.locked.isAlive(main_id)) {
        }
        history.locked.atomic_replace(main_id, static_cast<Locking>(0));
      }

      auto ptr = new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp};
      sub_id = history.read_write_table.push_back(ptr);
      if (main_id != sub_id) {
        while (!history.read_write_table.isAlive(main_id)) {
        }
        history.read_write_table.atomic_replace(main_id, ptr);
      }
    }
    return 1;
  };

  struct NewOrderVar {
    int64_t w, d, c, num;
    int64_t items[16];
    int64_t suppliers[16];
    int16_t quantities[16];
    bool alllocal;
  };

  void genNewOrder(NewOrderVar& neworder, uint32_t core_id, std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    neworder.w = core_id % number_warehouses + 1;
    neworder.d = dis(gen) % dist_per_warehouse + 1;
    neworder.c = nonUniformRandom(1023, 259, 1, cust_per_dist, gen);
    neworder.num = dis(gen) % 10 + 5;
    neworder.alllocal = true;

    for (int i = 0; i < neworder.num; i++) {
      neworder.items[i] = nonUniformRandom(8191, 7911, 1, max_items, gen);
      if (dis(gen) % 100 > 0) {
        neworder.suppliers[i] = neworder.w;
      } else {
        do {
          neworder.suppliers[i] = dis(gen) % number_warehouses + 1;
        } while (neworder.suppliers[i] == neworder.w && number_warehouses > 1);
        neworder.alllocal = false;
      }
      neworder.quantities[i] = dis(gen) % 10 + 1;
    }
  }

  template <bool T, typename std::enable_if_t<T>* = nullptr>
  int execNewOrder(NewOrderVar& neworder,
                   SinglyDistrict& d,
                   SinglyCustomer& c,
                   SinglyItem& i,
                   SinglyStock& s,
                   uint64_t transaction,
                   std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    uint64_t offset;
    int64_t wk = neworder.w;
    bool found = warehouse.opl->lookup(wk, offset, *warehouse_map,
                                       [](auto& wmap, auto& o, auto& w) { return wmap.lookup(w, o); });
    if (!found)
      return 0;

    int8_t tax;
    bool check = tc.readValue(tax, warehouse.w_tax, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset,
                              transaction);

    if (!check)
      return -1;

    int64_t ck = custKey(neworder.c, neworder.d, neworder.w);
    found = customer.opl->lookup(ck, offset, *customer_id_map,
                                 [](auto& cmap, auto& o, auto& w) { return cmap.lookup(w, o); });
    if (!found)
      return 0;

    {
      sv::ReadGuard<TC, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList> rg{
          &tc, customer.lsn, customer.read_write_table, customer.locked, offset, transaction};

      if (!rg.wasSuccessful()) {
        return -1;
      } else {
        c.c_last = customer.c_last[offset];
        c.c_credit = customer.c_credit[offset];
        c.c_discount = customer.c_discount[offset];
      }
    }

    int64_t dk = distKey(neworder.d, neworder.w);
    found =
        district.opl->lookup(dk, offset, *district_map, [](auto& wmap, auto& o, auto& w) { return wmap.lookup(w, o); });
    if (!found)
      return 0;

    {
      sv::ReadGuard<TC, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList> rg{
          &tc, district.lsn, district.read_write_table, district.locked, offset, transaction};

      if (!rg.wasSuccessful()) {
        return -1;
      } else {
        d.d_next_o_id = district.d_next_o_id[offset];
        d.d_tax = district.d_tax[offset];
      }
    }

    d.d_next_o_id += 1;
    check = tc.writeValue(d.d_next_o_id, district.d_next_o_id, district.lsn, district.read_write_table, district.locked,
                          offset, transaction);
    if (!check) {
      return -1;
    }

    int returnValue = -1;
    uint64_t removeIds[neworder.num]{};
    uint64_t order_insert = 0;

    if (insert_) {
      order_insert = order.o_id.push_back(d.d_next_o_id);
      uint64_t sub_id = order.o_d_id.push_back(neworder.d);
      if (order_insert != sub_id) {
        while (!order.o_d_id.isAlive(order_insert)) {
        }
        order.o_d_id.replace(order_insert, neworder.d);
      }

      sub_id = order.o_w_id.push_back(neworder.w);
      if (order_insert != sub_id) {
        while (!order.o_w_id.isAlive(order_insert)) {
        }
        order.o_w_id.replace(order_insert, neworder.w);
      }

      sub_id = order.o_c_id.push_back(neworder.c);
      if (order_insert != sub_id) {
        while (!order.o_c_id.isAlive(order_insert)) {
        }
        order.o_c_id.replace(order_insert, neworder.c);
      }

      sub_id = order.o_entry_d.push_back(time(NULL));
      if (order_insert != sub_id) {
        while (!order.o_entry_d.isAlive(order_insert)) {
        }
        order.o_entry_d.replace(order_insert, time(NULL));
      }

      sub_id = order.o_ol_cnt.push_back(neworder.num);
      if (order_insert != sub_id) {
        while (!order.o_ol_cnt.isAlive(order_insert)) {
        }
        order.o_ol_cnt.replace(order_insert, neworder.num);
      }

      sub_id = order.o_all_local.push_back(neworder.alllocal);
      if (order_insert != sub_id) {
        while (!order.o_all_local.isAlive(order_insert)) {
        }
        order.o_all_local.replace(order_insert, neworder.alllocal);
      }

      sub_id = order.lsn.push_back(0);
      if (order_insert != sub_id) {
        while (!order.lsn.isAlive(order_insert)) {
        }
        order.lsn.atomic_replace(order_insert, 0);
      }

      sub_id = order.locked.push_back(static_cast<Locking>(0));
      if (order_insert != sub_id) {
        while (!order.locked.isAlive(order_insert)) {
        }
        order.locked.atomic_replace(order_insert, static_cast<Locking>(0));
      }

      auto ptr = new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp};
      sub_id = order.read_write_table.push_back(ptr);
      if (order_insert != sub_id) {
        while (!order.read_write_table.isAlive(order_insert)) {
        }
        order.read_write_table.atomic_replace(order_insert, ptr);
      }

      check = tc.writeValue(d.d_next_o_id, order.o_id, order.lsn, order.read_write_table, order.locked, order_insert,
                            transaction);
      if (check) {
        int64_t opk = orderPrimaryKey(neworder.w, neworder.d, d.d_next_o_id);
        check = order.opl->insert(opk, order_insert, *order_map,
                                  [](auto& cfmap, auto& o, auto& s) { return cfmap.insert(s, o); });
      }

      if (!check) {
        goto abortInsert;
      }
    }

    for (auto ocnt = 0; ocnt < neworder.num; ocnt++) {
      int64_t ik = neworder.items[ocnt];
      found = item.opl->lookup(ik, offset, *item_map, [](auto& imap, auto& o, auto& w) { return imap.lookup(w, o); });
      if (!found)
        goto notFoundInsert;

      {
        sv::ReadGuard<TC, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList> rg{
            &tc, item.lsn, item.read_write_table, item.locked, offset, transaction};

        if (!rg.wasSuccessful()) {
          goto abortInsert;
        } else {
          i.i_price = item.i_price[offset];
          i.i_name = item.i_name[offset];
          i.i_data = item.i_data[offset];
        }
      }

      int64_t sk = stockKey(neworder.items[ocnt], neworder.suppliers[ocnt]);
      found = stock.opl->lookup(sk, offset, *stock_map, [](auto& imap, auto& o, auto& w) { return imap.lookup(w, o); });
      if (!found)
        goto notFoundInsert;

      {
        sv::ReadGuard<TC, Locking, atom::AtomicExtentVector, atom::AtomicSinglyLinkedList> rg{
            &tc, stock.lsn, stock.read_write_table, stock.locked, offset, transaction};

        if (!rg.wasSuccessful()) {
          goto abortInsert;
        } else {
          s.s_ytd = stock.s_ytd[offset];
          s.s_remote_cnt = stock.s_remote_cnt[offset];
          s.s_order_cnt = stock.s_order_cnt[offset];
          s.s_quantity = stock.s_quantity[offset];
          s.s_data = stock.s_data[offset];
          s.s_dist_01 = stock.s_dist_01[offset];
          s.s_dist_02 = stock.s_dist_02[offset];
          s.s_dist_03 = stock.s_dist_03[offset];
          s.s_dist_04 = stock.s_dist_04[offset];
          s.s_dist_05 = stock.s_dist_05[offset];
          s.s_dist_06 = stock.s_dist_06[offset];
          s.s_dist_07 = stock.s_dist_07[offset];
          s.s_dist_08 = stock.s_dist_08[offset];
          s.s_dist_09 = stock.s_dist_09[offset];
          s.s_dist_10 = stock.s_dist_10[offset];
        }
      }

      if (s.s_quantity - neworder.quantities[ocnt] >= 10) {
        s.s_quantity -= neworder.quantities[ocnt];
      } else {
        s.s_quantity += -neworder.quantities[ocnt] + 91;
      }

      if (neworder.suppliers[ocnt] != neworder.w) {
        s.s_remote_cnt += 1;
      }

      s.s_ytd += neworder.quantities[ocnt];

      check = tc.writeValue(s.s_quantity, stock.s_quantity, stock.lsn, stock.read_write_table, stock.locked, offset,
                            transaction);
      check &= tc.writeValue(s.s_remote_cnt, stock.s_remote_cnt, stock.lsn, stock.read_write_table, stock.locked,
                             offset, transaction);
      check &=
          tc.writeValue(s.s_ytd, stock.s_ytd, stock.lsn, stock.read_write_table, stock.locked, offset, transaction);

      if (!check)
        goto abortInsert;

      if (insert_) {
        removeIds[ocnt] = orderline.ol_o_id.push_back(d.d_next_o_id);
        uint64_t sub_id = orderline.ol_d_id.push_back(neworder.d);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_d_id.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_d_id.replace(removeIds[ocnt], neworder.d);
        }

        sub_id = orderline.ol_w_id.push_back(neworder.w);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_w_id.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_w_id.replace(removeIds[ocnt], neworder.w);
        }

        sub_id = orderline.ol_number.push_back(ocnt);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_number.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_number.replace(removeIds[ocnt], ocnt);
        }

        sub_id = orderline.ol_i_id.push_back(neworder.items[ocnt]);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_i_id.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_i_id.replace(removeIds[ocnt], neworder.items[ocnt]);
        }

        sub_id = orderline.ol_supply_w_id.push_back(neworder.suppliers[ocnt]);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_supply_w_id.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_supply_w_id.replace(removeIds[ocnt], neworder.suppliers[ocnt]);
        }

        sub_id = orderline.ol_quantity.push_back(neworder.quantities[ocnt]);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_quantity.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_quantity.replace(removeIds[ocnt], neworder.quantities[ocnt]);
        }

        sub_id = orderline.ol_amount.push_back((double)(neworder.quantities[ocnt]) * i.i_price);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_amount.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_amount.replace(removeIds[ocnt], (double)(neworder.quantities[ocnt]) * i.i_price);
        }

        StringStruct<24> ol_dist_info;
        switch ((int)neworder.d) {
          case 1:
            ol_dist_info = s.s_dist_01;
            break;
          case 2:
            ol_dist_info = s.s_dist_02;
            break;
          case 3:
            ol_dist_info = s.s_dist_03;
            break;
          case 4:
            ol_dist_info = s.s_dist_04;
            break;
          case 5:
            ol_dist_info = s.s_dist_05;
            break;
          case 6:
            ol_dist_info = s.s_dist_06;
            break;
          case 7:
            ol_dist_info = s.s_dist_07;
            break;
          case 8:
            ol_dist_info = s.s_dist_08;
            break;
          case 9:
            ol_dist_info = s.s_dist_09;
            break;
          case 10:
            ol_dist_info = s.s_dist_10;
            break;
        }

        sub_id = orderline.ol_dist_info.push_back(ol_dist_info);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_dist_info.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_dist_info.replace(removeIds[ocnt], ol_dist_info);
        }

        sub_id = orderline.lsn.push_back(0);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.lsn.isAlive(removeIds[ocnt])) {
          }
          orderline.lsn.atomic_replace(removeIds[ocnt], 0);
        }

        sub_id = orderline.locked.push_back(static_cast<Locking>(0));
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.locked.isAlive(removeIds[ocnt])) {
          }
          orderline.locked.atomic_replace(removeIds[ocnt], static_cast<Locking>(0));
        }

        auto ptr = new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp};
        sub_id = orderline.read_write_table.push_back(ptr);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.read_write_table.isAlive(removeIds[ocnt])) {
          }
          orderline.read_write_table.atomic_replace(removeIds[ocnt], ptr);
        }

        check = tc.writeValue(d.d_next_o_id, orderline.ol_o_id, orderline.lsn, orderline.read_write_table,
                              orderline.locked, removeIds[ocnt], transaction);

        if (!check) {
          goto abortInsert;
        }
      }
    }

    return 1;

  notFoundInsert:
    returnValue = 0;
  abortInsert:
    if (insert_) {
      order_map->erase(orderPrimaryKey(neworder.w, neworder.d, d.d_next_o_id));
      order.o_id.erase(order_insert);
      order.o_d_id.erase(order_insert);
      order.o_w_id.erase(order_insert);
      order.o_c_id.erase(order_insert);
      order.o_entry_d.erase(order_insert);
      order.o_ol_cnt.erase(order_insert);
      order.o_all_local.erase(order_insert);

      order.locked.erase(order_insert);
      order.lsn.erase(order_insert);
      order.read_write_table.erase(order_insert);

      for (auto ocnt = 0; ocnt < neworder.num; ocnt++) {
        if (removeIds[ocnt] == 0)
          break;
        orderline.ol_o_id.erase(removeIds[ocnt]);
        orderline.ol_d_id.erase(removeIds[ocnt]);
        orderline.ol_w_id.erase(removeIds[ocnt]);
        orderline.ol_i_id.erase(removeIds[ocnt]);
        orderline.ol_number.erase(removeIds[ocnt]);
        orderline.ol_supply_w_id.erase(removeIds[ocnt]);
        orderline.ol_quantity.erase(removeIds[ocnt]);
        orderline.ol_amount.erase(removeIds[ocnt]);
        orderline.ol_dist_info.erase(removeIds[ocnt]);

        orderline.locked.erase(removeIds[ocnt]);
        orderline.lsn.erase(removeIds[ocnt]);
        orderline.read_write_table.erase(removeIds[ocnt]);
      }
    }

    return returnValue;
  };

  template <bool T, typename std::enable_if_t<!T>* = nullptr>
  int execNewOrder(NewOrderVar& neworder,
                   SinglyDistrict& d,
                   SinglyCustomer& c,
                   SinglyItem& i,
                   SinglyStock& s,
                   uint64_t transaction,
                   std::mt19937& gen) {
    std::uniform_int_distribution<unsigned int> dis(0, std::numeric_limits<unsigned int>::max());
    uint64_t offset;
    int64_t wk = neworder.w;
    bool found = warehouse.opl->lookup(wk, offset, *warehouse_map,
                                       [](auto& wmap, auto& o, auto& w) { return wmap.lookup(w, o); });
    if (!found)
      return 0;

    int8_t tax;
    bool check = tc.readValue(tax, warehouse.w_tax, warehouse.lsn, warehouse.read_write_table, warehouse.locked, offset,
                              transaction);

    if (!check)
      return -1;

    int64_t ck = custKey(neworder.c, neworder.d, neworder.w);
    found = customer.opl->lookup(ck, offset, *customer_id_map,
                                 [](auto& cmap, auto& o, auto& w) { return cmap.lookup(w, o); });
    if (!found)
      return 0;

    {
      bool check = false;
      while (!check) {
        auto ret = tc.read(customer.lsn, customer.read_write_table, customer.locked, offset, transaction);
        tc.pureValue(c.c_last, customer.c_last, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_credit, customer.c_credit, customer.lsn, customer.read_write_table, customer.locked, offset,
                     transaction);
        tc.pureValue(c.c_discount, customer.c_discount, customer.lsn, customer.read_write_table, customer.locked,
                     offset, transaction);
        check = tc.readUndo(ret, customer.lsn, customer.read_write_table, customer.locked, offset, transaction);
      }
    }

    int64_t dk = distKey(neworder.d, neworder.w);
    found =
        district.opl->lookup(dk, offset, *district_map, [](auto& wmap, auto& o, auto& w) { return wmap.lookup(w, o); });
    if (!found)
      return 0;

    {
      bool check = false;
      while (!check) {
        auto ret = tc.read(district.lsn, district.read_write_table, district.locked, offset, transaction);
        tc.pureValue(d.d_next_o_id, district.d_next_o_id, district.lsn, district.read_write_table, district.locked,
                     offset, transaction);
        tc.pureValue(d.d_tax, district.d_tax, district.lsn, district.read_write_table, district.locked, offset,
                     transaction);
        check = tc.readUndo(ret, district.lsn, district.read_write_table, district.locked, offset, transaction);
      }
    }

    d.d_next_o_id += 1;
    check = tc.writeValue(d.d_next_o_id, district.d_next_o_id, district.lsn, district.read_write_table, district.locked,
                          offset, transaction);
    if (!check) {
      return -1;
    }

    int returnValue = -1;
    uint64_t removeIds[neworder.num]{};
    uint64_t order_insert = 0;

    if (insert_) {
      order_insert = order.o_id.push_back(d.d_next_o_id);
      uint64_t sub_id = order.o_d_id.push_back(neworder.d);
      if (order_insert != sub_id) {
        while (!order.o_d_id.isAlive(order_insert)) {
        }
        order.o_d_id.replace(order_insert, neworder.d);
      }

      sub_id = order.o_w_id.push_back(neworder.w);
      if (order_insert != sub_id) {
        while (!order.o_w_id.isAlive(order_insert)) {
        }
        order.o_w_id.replace(order_insert, neworder.w);
      }

      sub_id = order.o_c_id.push_back(neworder.c);
      if (order_insert != sub_id) {
        while (!order.o_c_id.isAlive(order_insert)) {
        }
        order.o_c_id.replace(order_insert, neworder.c);
      }

      sub_id = order.o_entry_d.push_back(time(NULL));
      if (order_insert != sub_id) {
        while (!order.o_entry_d.isAlive(order_insert)) {
        }
        order.o_entry_d.replace(order_insert, time(NULL));
      }

      sub_id = order.o_ol_cnt.push_back(neworder.num);
      if (order_insert != sub_id) {
        while (!order.o_ol_cnt.isAlive(order_insert)) {
        }
        order.o_ol_cnt.replace(order_insert, neworder.num);
      }

      sub_id = order.o_all_local.push_back(neworder.alllocal);
      if (order_insert != sub_id) {
        while (!order.o_all_local.isAlive(order_insert)) {
        }
        order.o_all_local.replace(order_insert, neworder.alllocal);
      }

      sub_id = order.lsn.push_back(0);
      if (order_insert != sub_id) {
        while (!order.lsn.isAlive(order_insert)) {
        }
        order.lsn.atomic_replace(order_insert, 0);
      }

      sub_id = order.locked.push_back(static_cast<Locking>(0));
      if (order_insert != sub_id) {
        while (!order.locked.isAlive(order_insert)) {
        }
        order.locked.atomic_replace(order_insert, static_cast<Locking>(0));
      }

      auto ptr = new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp};
      sub_id = order.read_write_table.push_back(ptr);
      if (order_insert != sub_id) {
        while (!order.read_write_table.isAlive(order_insert)) {
        }
        order.read_write_table.atomic_replace(order_insert, ptr);
      }

      check = tc.writeValue(d.d_next_o_id, order.o_id, order.lsn, order.read_write_table, order.locked, order_insert,
                            transaction);

      if (check) {
        int64_t opk = orderPrimaryKey(neworder.w, neworder.d, d.d_next_o_id);
        // check = order.opl->insert(opk, order_insert, *order_map,
        //                          [](auto& cfmap, auto& o, auto& s) { return cfmap.insert(s, o); });
        order_map->insert(opk, order_insert);
      }

      if (!check) {
        goto abortInsert;
      }
    }

    for (auto ocnt = 0; ocnt < neworder.num; ocnt++) {
      int64_t ik = neworder.items[ocnt];
      found = item.opl->lookup(ik, offset, *item_map, [](auto& imap, auto& o, auto& w) { return imap.lookup(w, o); });
      if (!found)
        goto notFoundInsert;

      {
        bool check = false;
        while (!check) {
          auto ret = tc.read(item.lsn, item.read_write_table, item.locked, offset, transaction);
          tc.pureValue(i.i_price, item.i_price, item.lsn, item.read_write_table, item.locked, offset, transaction);
          tc.pureValue(i.i_name, item.i_name, item.lsn, item.read_write_table, item.locked, offset, transaction);
          tc.pureValue(i.i_data, item.i_data, item.lsn, item.read_write_table, item.locked, offset, transaction);
          check = tc.readUndo(ret, item.lsn, item.read_write_table, item.locked, offset, transaction);
        }
      }

      int64_t sk = stockKey(neworder.items[ocnt], neworder.suppliers[ocnt]);
      found = stock.opl->lookup(sk, offset, *stock_map, [](auto& imap, auto& o, auto& w) { return imap.lookup(w, o); });
      if (!found)
        goto notFoundInsert;

      {
        bool check = false;
        while (!check) {
          auto ret = tc.read(stock.lsn, stock.read_write_table, stock.locked, offset, transaction);
          tc.pureValue(s.s_ytd, stock.s_ytd, stock.lsn, stock.read_write_table, stock.locked, offset, transaction);
          tc.pureValue(s.s_remote_cnt, stock.s_remote_cnt, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_order_cnt, stock.s_order_cnt, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_quantity, stock.s_quantity, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_data, stock.s_data, stock.lsn, stock.read_write_table, stock.locked, offset, transaction);
          tc.pureValue(s.s_dist_01, stock.s_dist_01, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_02, stock.s_dist_02, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_03, stock.s_dist_03, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_04, stock.s_dist_04, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_05, stock.s_dist_05, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_06, stock.s_dist_06, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_07, stock.s_dist_07, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_08, stock.s_dist_08, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_09, stock.s_dist_09, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          tc.pureValue(s.s_dist_10, stock.s_dist_10, stock.lsn, stock.read_write_table, stock.locked, offset,
                       transaction);
          check = tc.readUndo(ret, stock.lsn, stock.read_write_table, stock.locked, offset, transaction);
        }
      }

      if (s.s_quantity - neworder.quantities[ocnt] >= 10) {
        s.s_quantity -= neworder.quantities[ocnt];
      } else {
        s.s_quantity += -neworder.quantities[ocnt] + 91;
      }

      if (neworder.suppliers[ocnt] != neworder.w) {
        s.s_remote_cnt += 1;
      }

      s.s_ytd += neworder.quantities[ocnt];

      check = tc.writeValue(s.s_quantity, stock.s_quantity, stock.lsn, stock.read_write_table, stock.locked, offset,
                            transaction);
      check &= tc.writeValue(s.s_remote_cnt, stock.s_remote_cnt, stock.lsn, stock.read_write_table, stock.locked,
                             offset, transaction);
      check &=
          tc.writeValue(s.s_ytd, stock.s_ytd, stock.lsn, stock.read_write_table, stock.locked, offset, transaction);

      if (!check)
        goto abortInsert;

      if (insert_) {
        removeIds[ocnt] = orderline.ol_o_id.push_back(d.d_next_o_id);
        uint64_t sub_id = orderline.ol_d_id.push_back(neworder.d);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_d_id.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_d_id.replace(removeIds[ocnt], neworder.d);
        }

        sub_id = orderline.ol_w_id.push_back(neworder.w);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_w_id.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_w_id.replace(removeIds[ocnt], neworder.w);
        }

        sub_id = orderline.ol_number.push_back(ocnt);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_number.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_number.replace(removeIds[ocnt], ocnt);
        }

        sub_id = orderline.ol_i_id.push_back(neworder.items[ocnt]);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_i_id.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_i_id.replace(removeIds[ocnt], neworder.items[ocnt]);
        }

        sub_id = orderline.ol_supply_w_id.push_back(neworder.suppliers[ocnt]);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_supply_w_id.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_supply_w_id.replace(removeIds[ocnt], neworder.suppliers[ocnt]);
        }

        sub_id = orderline.ol_quantity.push_back(neworder.quantities[ocnt]);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_quantity.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_quantity.replace(removeIds[ocnt], neworder.quantities[ocnt]);
        }

        sub_id = orderline.ol_amount.push_back((double)(neworder.quantities[ocnt]) * i.i_price);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_amount.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_amount.replace(removeIds[ocnt], (double)(neworder.quantities[ocnt]) * i.i_price);
        }

        StringStruct<24> ol_dist_info;
        switch ((int)neworder.d) {
          case 1:
            ol_dist_info = s.s_dist_01;
            break;
          case 2:
            ol_dist_info = s.s_dist_02;
            break;
          case 3:
            ol_dist_info = s.s_dist_03;
            break;
          case 4:
            ol_dist_info = s.s_dist_04;
            break;
          case 5:
            ol_dist_info = s.s_dist_05;
            break;
          case 6:
            ol_dist_info = s.s_dist_06;
            break;
          case 7:
            ol_dist_info = s.s_dist_07;
            break;
          case 8:
            ol_dist_info = s.s_dist_08;
            break;
          case 9:
            ol_dist_info = s.s_dist_09;
            break;
          case 10:
            ol_dist_info = s.s_dist_10;
            break;
        }

        sub_id = orderline.ol_dist_info.push_back(ol_dist_info);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.ol_dist_info.isAlive(removeIds[ocnt])) {
          }
          orderline.ol_dist_info.replace(removeIds[ocnt], ol_dist_info);
        }

        sub_id = orderline.lsn.push_back(0);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.lsn.isAlive(removeIds[ocnt])) {
          }
          orderline.lsn.atomic_replace(removeIds[ocnt], 0);
        }

        sub_id = orderline.locked.push_back(static_cast<Locking>(0));
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.locked.isAlive(removeIds[ocnt])) {
          }
          orderline.locked.atomic_replace(removeIds[ocnt], static_cast<Locking>(0));
        }

        auto ptr = new atom::AtomicSinglyLinkedList<uint64_t>{&ca, &emp};
        sub_id = orderline.read_write_table.push_back(ptr);
        if (removeIds[ocnt] != sub_id) {
          while (!orderline.read_write_table.isAlive(removeIds[ocnt])) {
          }
          orderline.read_write_table.atomic_replace(removeIds[ocnt], ptr);
        }

        check = tc.writeValue(d.d_next_o_id, orderline.ol_o_id, orderline.lsn, orderline.read_write_table,
                              orderline.locked, removeIds[ocnt], transaction);

        if (!check) {
          goto abortInsert;
        }
      }
    }

    return 1;

  notFoundInsert:
    returnValue = 0;
  abortInsert:
    if (insert_) {  // order_map->erase(orderPrimaryKey(neworder.w, neworder.d, d.d_next_o_id));
      order.o_id.erase(order_insert);
      order.o_d_id.erase(order_insert);
      order.o_w_id.erase(order_insert);
      order.o_c_id.erase(order_insert);
      order.o_entry_d.erase(order_insert);
      order.o_ol_cnt.erase(order_insert);
      order.o_all_local.erase(order_insert);

      order.locked.erase(order_insert);
      order.lsn.erase(order_insert);
      order.read_write_table.erase(order_insert);

      for (auto ocnt = 0; ocnt < neworder.num; ocnt++) {
        if (removeIds[ocnt] == 0)
          break;
        orderline.ol_o_id.erase(removeIds[ocnt]);
        orderline.ol_d_id.erase(removeIds[ocnt]);
        orderline.ol_w_id.erase(removeIds[ocnt]);
        orderline.ol_i_id.erase(removeIds[ocnt]);
        orderline.ol_number.erase(removeIds[ocnt]);
        orderline.ol_supply_w_id.erase(removeIds[ocnt]);
        orderline.ol_quantity.erase(removeIds[ocnt]);
        orderline.ol_amount.erase(removeIds[ocnt]);
        orderline.ol_dist_info.erase(removeIds[ocnt]);

        orderline.locked.erase(removeIds[ocnt]);
        orderline.lsn.erase(removeIds[ocnt]);
        orderline.read_write_table.erase(removeIds[ocnt]);
      }
    }

    return returnValue;
  };

  void getNumberString(uint32_t sid, char* stringstruct, uint32_t length) {
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
};

};  // namespace tpcc
};  // namespace sv
