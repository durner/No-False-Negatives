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
#include "common/chunk_allocator.hpp"
#include "common/epoch_manager.hpp"
#include "common/global_logger.hpp"
#include "ds/atomic_unordered_map.hpp"
#include "ds/atomic_unordered_set.hpp"
#include <algorithm>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_set>

//#define LOGGER 1

namespace locked {
namespace serial {

struct Node {
  atom::AtomicUnorderedSet<uint64_t, atom::AtomicUnorderedSetBucket<uint64_t>, common::StdAllocator> outgoing_nodes_;
  atom::AtomicUnorderedSet<uint64_t, atom::AtomicUnorderedSetBucket<uint64_t>, common::StdAllocator> incoming_nodes_;

  std::atomic<uint64_t> transaction_;
  std::atomic<bool> abort_;
  std::atomic<bool> cascading_abort_;
  std::atomic<uint64_t> commit_ctr_;
  std::atomic<bool> commited_;
  std::atomic<uint64_t> last_alive_;

  Node(uint64_t setsize, common::StdAllocator* alloc, atom::EpochManagerBase<common::StdAllocator>* em)
      : outgoing_nodes_(setsize, alloc, em),
        incoming_nodes_(setsize, alloc, em),
        abort_(false),
        cascading_abort_(false),
        commit_ctr_(0),
        commited_(false),
        last_alive_(0) {}
};

/*
 * This is the main class for serializing the transaction with a graph. This graph accepts schedules \in CSR.
 */

class SerializationGraph {
  using Allocator = common::ChunkAllocator;
  using EMB = atom::EpochManagerBase<Allocator>;
  using EM = atom::EpochManager<Allocator>;

 private:
  // tbb::spin_mutex mut;
  common::StdAllocator std_alloc_;
  atom::EpochManagerBase<common::StdAllocator> std_emb_;
  atom::
      AtomicUnorderedMap<Node*, uint64_t, atom::AtomicUnorderedMapBucket<Node*, uint64_t>, common::StdAllocator, false>
          node_map_;
  common::GlobalLogger logger;
  Allocator* alloc_;
  tbb::spin_mutex mut;
  EMB* em_;
  static thread_local std::unordered_set<uint64_t> visited;
  static thread_local std::unordered_set<uint64_t> visit_path;

 public:
  // SerializationGraph(Allocator* alloc, EMB* em)
  //    : node_map_(std::thread::hardware_concurrency() << 4), alloc_(alloc), em_(em) {}
  //
  SerializationGraph(Allocator* alloc, EMB* em);
  ~SerializationGraph();

  const uint64_t size() const;
  void createNode(const uint64_t transaction);
  void cleanup(Node* node);
  bool find(atom::AtomicUnorderedSet<uint64_t, atom::AtomicUnorderedSetBucket<uint64_t>, common::StdAllocator>& nodes,
            uint64_t transaction) const;
  bool insert_and_check(const uint64_t this_transaction, const uint64_t from_transaction);
  bool cycleCheckExternal();
  bool cycleCheckNaive();
  bool cycleCheckExternal(uint64_t transaction);
  bool cycleCheckNaive(uint64_t transaction);
  bool cycleCheckNaive(Node& cur) const;
  bool needsAbort(uint64_t transaction);
  bool isCommited(uint64_t transaction);
  void abort(uint64_t transaction, std::unordered_set<uint64_t>& uset);
  bool checkCommited(uint64_t transaction);
  bool erase_graph_constraints(uint64_t transaction);
  std::string generateString();
  void print();
  void log(const common::LogInfo log_info);
  void log(const std::string log_info);
};
};  // namespace serial
};  // namespace locked
