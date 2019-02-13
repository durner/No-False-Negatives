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
#include "common/shared_spin_mutex.hpp"
#include "ds/atomic_unordered_map.hpp"
#include "ds/atomic_unordered_set.hpp"
#include <algorithm>
#include <map>
#include <queue>
#include <shared_mutex>
#include <sstream>
#include <thread>
#include <unordered_set>

//#define SGLOGGER 1

namespace mv {
namespace nofalsenegatives {
namespace serial {

struct Node {
  using NodeSet = atom::AtomicUnorderedSet<Node*, atom::AtomicUnorderedSetBucket<Node*>, common::ChunkAllocator>;
  NodeSet* outgoing_nodes_;
  NodeSet* incoming_nodes_;

  std::atomic<uint64_t> transaction_;
  std::atomic<bool> abort_;
  std::atomic<bool> cascading_abort_;
  std::atomic<bool> commited_;
  std::atomic<bool> cleaned_;
  std::atomic<bool> checked_;
  std::atomic<uint64_t> abort_through_;
  common::SharedSpinMutex mut_;

  Node(NodeSet* outgoing, NodeSet* incoming)
      : outgoing_nodes_(outgoing),
        incoming_nodes_(incoming),
        abort_(false),
        cascading_abort_(false),
        commited_(false),
        cleaned_(false),
        checked_(false),
        abort_through_(0),
        mut_() {}
};

struct RecycledNodeSets {
  std::unique_ptr<std::vector<std::unique_ptr<Node::NodeSet>>> rns;

  RecycledNodeSets() : rns(new std::vector<std::unique_ptr<Node::NodeSet>>{}) {}
};

/* The lowest bit is used to determine whether an abort is needed */
inline static Node* accessEdge(const Node* node, const bool rw) {
  constexpr uint64_t lowestSet = 1;
  constexpr uint64_t lowestNotSet = ~(lowestSet);
  return reinterpret_cast<Node*>(rw ? lowestSet | reinterpret_cast<uintptr_t>(node)
                                    : lowestNotSet & reinterpret_cast<uintptr_t>(node));
}

inline static std::tuple<Node*, bool> findEdge(const Node* encoded_id) {
  constexpr uint64_t lowestSet = 1;
  constexpr uint64_t lowestNotSet = ~(lowestSet);
  return std::make_tuple(reinterpret_cast<Node*>(lowestNotSet & reinterpret_cast<uintptr_t>(encoded_id)),
                         reinterpret_cast<uintptr_t>(encoded_id) & lowestSet);
}

/*
 * This is the main class for serializing the transaction with a graph. This graph accepts schedules \in CSR.
 */

class SerializationGraph {
  using Allocator = common::ChunkAllocator;
  using EMB = atom::EpochManagerBase<Allocator>;
  using EM = atom::EpochManager<Allocator>;
  using NEMB = atom::EpochManagerBase<common::NoAllocator>;
  using NEM = atom::EpochManager<common::NoAllocator>;

 private:
  tbb::spin_mutex mut;
  common::GlobalLogger logger;
  Allocator* alloc_;
  EMB* em_;
  common::NoAllocator noalloc_;
  NEMB nem_;
  std::atomic<uint64_t> created_sets_;

  static thread_local std::unordered_set<Node*> visited;
  static thread_local std::unordered_set<Node*> visit_path;
  static thread_local RecycledNodeSets empty_sets;
  static thread_local Node* this_node;
  static thread_local atom::EpochGuard<NEMB, NEM>* neg_;

 public:
  SerializationGraph(Allocator* alloc, EMB* em);
  ~SerializationGraph();

  const uint64_t size() const;
  uintptr_t createNode();
  void setInactive();
  void waitAndTidy();
  void cleanup();
  bool insert_and_check(uintptr_t from_node, bool read_write_edge);

  bool find(Node::NodeSet& nodes, Node* transaction) const;
  bool cycleCheckNaive();
  bool cycleCheckNaive(Node* cur) const;
  bool needsAbort(uintptr_t node);
  bool isCommited(uintptr_t node);
  void abort(std::unordered_set<uint64_t>& uset);
  bool checkCommited();
  bool erase_graph_constraints();
  std::string generateString();
  void print();
  void log(const common::LogInfo log_info);
  void log(const std::string log_info);
};
};  // namespace serial
};  // namespace nofalsenegatives
};  // namespace mv
