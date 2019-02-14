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
#include "common/chunk_allocator.hpp"
#include "common/csv_writer.hpp"
#include "common/thread_handler.hpp"
#include "perfevent/PerfEvent.hpp"
#include <chrono>
#include <future>
#include <iostream>
#include <sstream>
#include <thread>
#include <tbb/tbb.h>

//#define perf 1

int parseLine(char* line) {
  int i = strlen(line);
  const char* p = line;
  while (*p < '0' || *p > '9')
    p++;
  line[i - 3] = '\0';
  i = atoi(p);
  return i;
}

int getValue() {  // Note: this value is in KB!
  FILE* file = fopen("/proc/self/status", "r");
  int result = -1;
  char line[128];

  while (fgets(line, 128, file) != NULL) {
    if (strncmp(line, "VmSize:", 7) == 0) {
      result = parseLine(line);
      break;
    }
  }
  fclose(file);
  return result;
}

template <typename Database, typename Func>
void runBenchmark(Database* db,
                  Func client,
                  const char* benchmark,
                  const char* algorithm,
                  uint64_t database_size,
                  uint64_t transaction_iterations,
                  uint64_t cores,
                  uint64_t scanners = 0,
                  Func scan = nullptr) {
  db->populateDatabase(database_size);
  std::cout << "Memory Needed for Database population: " << getValue() / 1024.0 << "MB" << std::endl;

#ifdef perf
  PerfEvent p;
  p.startCounters();
#endif

  assert(cores - scanners >= scanners);

  auto start = std::chrono::steady_clock::now();
  common::ThreadHandler* threads[255];
  for (uint32_t i = 0; i < cores - scanners; ++i) {
    threads[i] = new common::ThreadHandler((1 * i) % std::thread::hardware_concurrency());
    threads[i]->run(client, std::ref(*db), database_size, transaction_iterations, (1 * i));
  }

  for (uint32_t i = cores - scanners; i < cores; ++i) {
    threads[i] = new common::ThreadHandler((1 * i) % std::thread::hardware_concurrency());
    threads[i]->run(scan, std::ref(*db), database_size, (cores - scanners), (1 * i));
  }

  for (uint32_t i = 0; i < cores - scanners; ++i) {
    threads[i]->join();
  }

  for (uint32_t i = cores - scanners; i < cores; ++i) {
    threads[i]->join();
  }

  auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
  std::cout << "Time needed (brutto): " << diff.count() << "ms" << std::endl;

#ifdef perf
  p.stopCounters();
  p.printReport(std::cout, transaction_iterations * cores);
#endif

  common::CSVWriter csvwriter;
  auto log = std::stringstream{};
  std::string bench{benchmark};
  std::string algo{algorithm};

  if (!algo.substr(0, 3).compare("SGT") && bench.front() == 'm') {
    algo = "M" + algo;
  }

  std::string ycsb{";0;0;0;0"};
  if (!bench.substr(5, 4).compare("ycsb")) {
    ycsb = bench.substr(9);
    bench = bench.substr(0, 9);
  }

  log << bench << ";" << algo << ";" << database_size << ";" << transaction_iterations << ";" << (cores - scanners)
      << ";" << scanners << ";" << diff.count();

  db->global_details_collector.writeCSV(log);

  log << ycsb;

  csvwriter.log(log.str());

  db->global_details_collector.printStatistics();

  std::cout << "Total Memory Needed: " << getValue() / 1024.0 << "MB" << std::endl;
  db->deleteDatabase();
  delete db;
}
