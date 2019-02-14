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
#include "common/epoch_manager.hpp"
#include "common/global_logger.hpp"
#include "ds/atomic_unordered_map.hpp"
#include "ds/atomic_unordered_set.hpp"
#include "svcc/cc/step/step_manager.hpp"
#include <algorithm>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_set>

//#define SGLOGGER 1

namespace step {
namespace serial {

struct Node {
  using NodeMap = atom::AtomicUnorderedMap<uint64_t,
                                           uint64_t,
                                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                                           common::ChunkAllocator>;
  NodeMap* outgoing_nodes_;
  NodeMap* incoming_nodes_;

  std::atomic<uint64_t> transaction_;
  std::atomic<bool> abort_;
  std::atomic<bool> cascading_abort_;
  std::atomic<uint64_t> commit_ctr_;
  std::atomic<bool> commited_;
  std::atomic<uint64_t> last_alive_;

  Node(NodeMap* in, NodeMap* out)
      : outgoing_nodes_(in),
        incoming_nodes_(out),
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
  tbb::spin_mutex mut;
  atom::AtomicUnorderedMap<Node*,
                           uint64_t,
                           atom::AtomicUnorderedMapBucket<Node*, uint64_t>,
                           common::ChunkAllocator,
                           false>
      node_map_;
  atom::AtomicUnorderedMap<uint64_t,
                           uint64_t,
                           atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator,
                           false>
      order_map_;

  std::atomic<uint64_t> order_version_;
  std::atomic<bool> order_version_locked_;
  common::GlobalLogger logger;
  Allocator* alloc_;
  EMB* em_;
  StepManager sm_;
  bool online_;
  static thread_local std::unordered_set<uint64_t> visited;
  static thread_local std::unordered_set<uint64_t> visit_path;
  static thread_local std::queue<std::unique_ptr<Node::NodeMap>> empty_maps;
  static thread_local std::vector<std::pair<uint64_t, uint64_t>> dF;
  static thread_local std::vector<std::pair<uint64_t, uint64_t>> dB;

  static thread_local std::vector<uint64_t> L;
  static thread_local std::vector<std::pair<uint64_t, uint64_t>> R;

 public:
  // SerializationGraph(Allocator* alloc, EMB* em)
  //    : node_map_(std::thread::hardware_concurrency() << 4), alloc_(alloc), em_(em) {}
  //
  SerializationGraph(Allocator* alloc, EMB* em, bool online = false);
  ~SerializationGraph();

  const uint64_t size() const;
  void createNode(const uint64_t transaction);
  void cleanup(Node* node);
  bool find(Node::NodeMap& nodes, uint64_t transaction, uint64_t ctr) const;
  bool insert_and_check(const uint64_t this_transaction, const uint64_t from_transaction);
  bool cycleCheckExternal();
  bool cycleCheckOnline(const uint64_t this_transaction, const uint64_t from_transaction, uint64_t ctr);
  bool cycleCheckNaive(uint64_t ctr);
  bool cycleCheckExternal(uint64_t transaction);
  bool cycleCheckNaive(uint64_t ctr, uint64_t transaction);
  bool cycleCheckNaive(Node& cur, uint64_t ctr) const;
  bool needsAbort(uint64_t transaction);
  bool isCommited(uint64_t transaction);
  void abort(uint64_t transaction, std::unordered_set<uint64_t>& uset);
  bool checkCommited(uint64_t transaction);
  bool erase_graph_constraints(uint64_t transaction, uint64_t ctr);
  std::string generateString();
  void print();
  void log(const common::LogInfo log_info);
  void log(const std::string log_info);

  bool dfsF(uint64_t n, uint64_t ub) const;
  bool dfsB(uint64_t n, uint64_t lb) const;
  void reorder() const;
};
};  // namespace serial
};  // namespace step
