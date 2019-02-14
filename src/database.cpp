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

#include "database.hpp"

#include "common/mutex_wait_manager.hpp"
#include "common/no_wait_manager.hpp"

#include "mvcc/benchmarks/column_store_smallbank.hpp"
#include "mvcc/benchmarks/column_store_tatp.hpp"
#include "mvcc/benchmarks/column_store_tpcc.hpp"
#include "mvcc/benchmarks/column_store_ycsb.hpp"
#include "svcc/benchmarks/column_store_smallbank.hpp"
#include "svcc/benchmarks/column_store_tatp.hpp"
#include "svcc/benchmarks/column_store_tpcc.hpp"
#include "svcc/benchmarks/column_store_ycsb.hpp"

#include "svcc/cc/2pl/transaction_coordinator.hpp"
#include "svcc/cc/2pl_table/transaction_coordinator.hpp"
#include "svcc/cc/locked/transaction_coordinator.hpp"
#include "svcc/cc/nofalsenegatives/transaction_coordinator.hpp"
#include "svcc/cc/step/transaction_coordinator.hpp"
#include "svcc/cc/tictoc/transaction_coordinator.hpp"

#include "mvcc/cc/mvocc/transaction_coordinator.hpp"
#include "mvcc/cc/nofalsenegatives/transaction_coordinator.hpp"

#include <string>
bool writeHeader = true;

int main(int argc, char** argv) {
  if (argc < 6) {
    printf("Usage: %s benchmark algorithm database_size transaction_iterations cores test/scanners\n", argv[0]);
    return 1;
  }

  char* benchmark = argv[1];
  char* algorithm = argv[2];
  uint64_t database_size = atoi(argv[3]);           // 1000000
  uint64_t transaction_iterations = atoi(argv[4]);  // 1000000
  uint64_t cores = atoi(argv[5]);                   // 100
  uint64_t custom1 = 0;
  double custom2 = 0, custom3 = 0, custom4 = 0;

  if (argc >= 7) {
    custom1 = atoi(argv[6]);

    if (argc >= 8) {
      custom2 = atof(argv[7]);

      if (argc >= 9) {
        custom3 = atof(argv[8]);

        if (argc >= 10) {
          custom4 = atof(argv[9]);
        }
      }
    }
  }

  cpu_set_t cpuset;
  pthread_t pthread = pthread_self();

  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);

  int s = pthread_setaffinity_np(pthread, sizeof(cpu_set_t), &cpuset);
  if (s != 0) {
    std::cout << "Error pthread_setaffinity_np" << std::endl;
  }

  s = pthread_getaffinity_np(pthread, sizeof(cpu_set_t), &cpuset);
  if (s != 0) {
    std::cout << "Error pthread_getaffinity_np" << std::endl;
  }

  tbb::task_scheduler_init init(std::thread::hardware_concurrency());

  if (!strcmp(benchmark, "svcc_smallbank")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "NoFalseNegatives_online")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{true};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based_online")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{true};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_locked")) {
      auto db = new sv::smallbank::Database<locked::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL")) {
      auto db = new sv::smallbank::Database<twopl::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL_table")) {
      auto db = new sv::smallbank::Database<twopl_table::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::NoWaitManager, std::pair<uint64_t, std::set<uint64_t>>*>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "TicToc")) {
      auto db = new sv::smallbank::Database<tictoc::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "svcc_smallbank_hc")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "NoFalseNegatives_online")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{true};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based_online")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{true};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_locked")) {
      auto db = new sv::smallbank::Database<locked::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL")) {
      auto db = new sv::smallbank::Database<twopl::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL_table")) {
      auto db = new sv::smallbank::Database<twopl_table::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager, std::pair<uint64_t, std::set<uint64_t>>*>{};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "TicToc")) {
      auto db = new sv::smallbank::Database<tictoc::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "svcc_smallbank_scan")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "NoFalseNegatives_online")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{true};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based_online")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{true};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_locked")) {
      auto db = new sv::smallbank::Database<locked::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL")) {
      auto db = new sv::smallbank::Database<twopl::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL_table")) {
      auto db = new sv::smallbank::Database<twopl_table::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager, std::pair<uint64_t, std::set<uint64_t>>*>{};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "TicToc")) {
      auto db = new sv::smallbank::Database<tictoc::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "svcc_smallbank_scanner")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clientOLAPOnly);
    } else if (!strcmp(algorithm, "NoFalseNegatives_online")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{true};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clientOLAPOnly);
    } else if (!strcmp(algorithm, "SGT_step_based")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clientOLAPOnly);
    } else if (!strcmp(algorithm, "SGT_step_based_online")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{true};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clientOLAPOnly);
    } else if (!strcmp(algorithm, "SGT_locked")) {
      auto db = new sv::smallbank::Database<locked::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clientOLAPOnly);
    } else if (!strcmp(algorithm, "2PL")) {
      auto db = new sv::smallbank::Database<twopl::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clientOLAPOnly);
    } else if (!strcmp(algorithm, "2PL_table")) {
      auto db = new sv::smallbank::Database<twopl_table::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager, std::pair<uint64_t, std::set<uint64_t>>*>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clientOLAPOnly);
    } else if (!strcmp(algorithm, "TicToc")) {
      auto db = new sv::smallbank::Database<tictoc::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clientOLAPOnly);
    }
  } else if (!strcmp(benchmark, "svcc_ycsb")) {
    std::string benchmark_detail = std::string(benchmark) + ";" + std::to_string(custom1) + ";" +
                                   std::to_string(custom2) + ";" + std::to_string(custom3) + ";" +
                                   std::to_string(custom4);
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new sv::ycsb::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{custom1, custom2, custom3, custom4};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "NoFalseNegatives_online")) {
      auto db = new sv::ycsb::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{custom1, custom2, custom3, custom4, true};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based")) {
      auto db = new sv::ycsb::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{custom1, custom2, custom3, custom4, true};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based_online")) {
      auto db = new sv::ycsb::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{custom1, custom2, custom3, custom4};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_locked")) {
      auto db = new sv::ycsb::Database<locked::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{custom1, custom2, custom3, custom4};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL")) {
      auto db = new sv::ycsb::Database<twopl::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{custom1, custom2, custom3, custom4};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL_table")) {
      auto db = new sv::ycsb::Database<twopl_table::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager, std::pair<uint64_t, std::set<uint64_t>>*>{
          custom1, custom2, custom3, custom4};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "TicToc")) {
      auto db = new sv::ycsb::Database<tictoc::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::NoWaitManager>{custom1, custom2, custom3, custom4};
      runBenchmark(db, db->clientMulti<false>, benchmark_detail.c_str(), algorithm, database_size,
                   transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "svcc_tatp")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new sv::tatp::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "NoFalseNegatives_online")) {
      auto db = new sv::tatp::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{true};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based")) {
      auto db = new sv::tatp::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{true};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based_online")) {
      auto db = new sv::tatp::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_locked")) {
      auto db = new sv::tatp::Database<locked::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL")) {
      auto db = new sv::tatp::Database<twopl::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL_table")) {
      auto db = new sv::tatp::Database<twopl_table::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager, std::pair<uint64_t, std::set<uint64_t>>*>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "TicToc")) {
      auto db = new sv::tatp::Database<tictoc::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::NoWaitManager>{};
      runBenchmark(db, db->clientMultiRead<false>, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "svcc_tpcc")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new sv::tpcc::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{database_size};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "NoFalseNegatives_online")) {
      auto db = new sv::tpcc::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{database_size, true};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based")) {
      auto db = new sv::tpcc::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{database_size, true};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based_online")) {
      auto db = new sv::tpcc::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{database_size};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_locked")) {
      auto db = new sv::tpcc::Database<locked::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{database_size};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL")) {
      auto db = new sv::tpcc::Database<twopl::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager>{database_size};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "2PL_table")) {
      auto db = new sv::tpcc::Database<twopl_table::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::MutexWaitManager, std::pair<uint64_t, std::set<uint64_t>>*>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "TicToc")) {
      auto db = new sv::tpcc::Database<tictoc::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                       common::NoWaitManager>{database_size};
      runBenchmark(db, db->clientMultiRead<false>, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "mvcc_smallbank")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::smallbank::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::smallbank::Database<mv::mvocc::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "mvcc_smallbank_hc")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::smallbank::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::smallbank::Database<mv::mvocc::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->clientHighContention, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "mvcc_ycsb")) {
    std::string benchmark_detail = std::string(benchmark) + ";" + std::to_string(custom1) + ";" +
                                   std::to_string(custom2) + ";" + std::to_string(custom3) + ";" +
                                   std::to_string(custom4);
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::ycsb::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                           atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                       common::MutexWaitManager>{custom1, custom2, custom3, custom4};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::ycsb::Database<mv::mvocc::transaction::TransactionCoordinator<
                                           atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                       common::NoWaitManager>{custom1, custom2, custom3, custom4};
      runBenchmark(db, db->client, benchmark_detail.c_str(), algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "mvcc_tatp")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::tatp::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                           atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                       common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::tatp::Database<mv::mvocc::transaction::TransactionCoordinator<
                                           atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                       common::NoWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "mvcc_tpcc")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::tpcc::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                           atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                       common::MutexWaitManager>{database_size};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::tpcc::Database<mv::mvocc::transaction::TransactionCoordinator<
                                           atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                       common::NoWaitManager>{database_size};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "mvcc_smallbank_scan")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::smallbank::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::smallbank::Database<mv::mvocc::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->clientScan, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "mvcc_smallbank_scanner")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::smallbank::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clienOLAPOnly);
      // std::cout << "AVG VC Length: " << db->tc.getAVGVC() << std::endl;
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::smallbank::Database<mv::mvocc::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->client, benchmark, algorithm, database_size, transaction_iterations, cores, custom1,
                   db->clienOLAPOnly);
    }
  } else if (!strcmp(benchmark, "mvcc_smallbank_read")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::smallbank::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      // p.timeAndProfile("bench", [&]() {
      runBenchmark(db, db->clientReadOnly, benchmark, algorithm, database_size, transaction_iterations, cores);
      //});
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::smallbank::Database<mv::mvocc::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      runBenchmark(db, db->clientReadOnly, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "mvcc_test")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db = new mv::smallbank::Database<mv::nofalsenegatives::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      if (custom1 == 2)
        runBenchmark(db, db->clientTest<0, true>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 3)
        runBenchmark(db, db->clientTest<1, true>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 0)
        runBenchmark(db, db->clientTest<0, false>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 1)
        runBenchmark(db, db->clientTest<1, false>, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "MVOCC")) {
      auto db = new mv::smallbank::Database<mv::mvocc::transaction::TransactionCoordinator<
                                                atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      if (custom1 == 2)
        runBenchmark(db, db->clientTest<0, true>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 3)
        runBenchmark(db, db->clientTest<1, true>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 0)
        runBenchmark(db, db->clientTest<0, false>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 1)
        runBenchmark(db, db->clientTest<1, false>, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  } else if (!strcmp(benchmark, "svcc_test")) {
    if (!strcmp(algorithm, "NoFalseNegatives")) {
      auto db =
          new sv::smallbank::Database<nofalsenegatives::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                      common::MutexWaitManager>{};
      if (custom1 == 0)
        runBenchmark(db, db->clientTest<0>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 1)
        runBenchmark(db, db->clientTest<1>, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_step_based")) {
      auto db = new sv::smallbank::Database<step::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      if (custom1 == 0)
        runBenchmark(db, db->clientTest<0>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 1)
        runBenchmark(db, db->clientTest<1>, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "SGT_locked")) {
      auto db = new sv::smallbank::Database<locked::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::MutexWaitManager>{};
      if (custom1 == 0)
        runBenchmark(db, db->clientTest<0>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 1)
        runBenchmark(db, db->clientTest<1>, benchmark, algorithm, database_size, transaction_iterations, cores);
    } else if (!strcmp(algorithm, "TicToc")) {
      auto db = new sv::smallbank::Database<tictoc::transaction::TransactionCoordinator<common::ChunkAllocator>,
                                            common::NoWaitManager>{};
      if (custom1 == 0)
        runBenchmark(db, db->clientTest<0>, benchmark, algorithm, database_size, transaction_iterations, cores);
      if (custom1 == 1)
        runBenchmark(db, db->clientTest<1>, benchmark, algorithm, database_size, transaction_iterations, cores);
    }
  }
  return 0;
}
