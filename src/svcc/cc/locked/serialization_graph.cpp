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

#include "svcc/cc/locked/serialization_graph.hpp"

namespace locked {
namespace serial {

thread_local std::unordered_set<uint64_t> SerializationGraph::visited{std::thread::hardware_concurrency()};
thread_local std::unordered_set<uint64_t> SerializationGraph::visit_path{std::thread::hardware_concurrency() >= 32
                                                                             ? std::thread::hardware_concurrency() >> 4
                                                                             : std::thread::hardware_concurrency()};

SerializationGraph::SerializationGraph(Allocator* alloc, EMB* em)
    : std_alloc_(),
      std_emb_(&std_alloc_),
      node_map_(std::thread::hardware_concurrency() << 4, &std_alloc_, &std_emb_),
      alloc_(alloc),
      em_(em) {}

SerializationGraph::~SerializationGraph() {
}

const uint64_t SerializationGraph::size() const {
  return node_map_.size();
}

void SerializationGraph::createNode(const uint64_t transaction) {
  Node* this_node;
  bool found = node_map_.lookup(transaction, this_node);
  if (!found) {
    Node* this_node = alloc_->allocate<Node>(1);
    new (this_node) Node{std::thread::hardware_concurrency() >= 32 ? std::thread::hardware_concurrency() >> 4
                                                                   : std::thread::hardware_concurrency(),
                         &std_alloc_, &std_emb_};
    this_node->transaction_ = transaction;
    node_map_.insert(transaction, this_node);
  }
}

void SerializationGraph::cleanup(Node* node) {
  // atom::EpochGuard<EMB, EM> eg {em_};
  auto it = node->outgoing_nodes_.begin();
  while (it != node->outgoing_nodes_.end()) {
    Node* that_node;
    bool found = node_map_.lookup(*it, that_node);
    if (found) {
      if (node->abort_)
        that_node->cascading_abort_ = true;
      that_node->incoming_nodes_.erase(node->transaction_);
    }
    ++it;
  }

  auto it_out = node->incoming_nodes_.begin();
  while (it_out != node->incoming_nodes_.end()) {
    Node* that_node;
    bool found = node_map_.lookup(*it_out, that_node);
    if (found)
      that_node->outgoing_nodes_.erase(node->transaction_);

    ++it_out;
  }
  // delete node;
  // eg.add(node);
  alloc_->deallocate(node, 1);
}

bool SerializationGraph::find(
    atom::AtomicUnorderedSet<uint64_t, atom::AtomicUnorderedSetBucket<uint64_t>, common::StdAllocator>& nodes,
    uint64_t transaction) const {
  auto it = nodes.begin();
  while (it != nodes.end()) {
    if (*it == transaction) {
      return true;
    }

    ++it;
  }
  return false;
}

bool SerializationGraph::insert_and_check(const uint64_t this_transaction, const uint64_t from_transaction) {
  if (from_transaction == 0 || from_transaction == this_transaction) {
    return true;
  }
  std::lock_guard<tbb::spin_mutex> lock(mut);
  Node* this_node;
  bool found = node_map_.lookup(this_transaction, this_node);
  if (!found) {
    // std::cout << "missing transaction " << this_transaction << std::endl;
    assert(false);
  }

  if (!find(this_node->incoming_nodes_, from_transaction)) {
    Node* that_node;
    auto found = node_map_.lookup(from_transaction, that_node);
    if (!found) {
      // std::cout << "Node " << from_transaction << " already commited!" << std::endl;
      // already commited node wrote last time on transaction

      return true;
    }

    if (that_node->abort_ || that_node->cascading_abort_) {
      this_node->cascading_abort_ = true;
      return false;
    }

    this_node->incoming_nodes_.insert(from_transaction);
    that_node->outgoing_nodes_.insert(this_transaction);
    bool cycle = !cycleCheckNaive(this_transaction);

    return cycle;
  }

  return true;
}

bool SerializationGraph::cycleCheckExternal() {
  std::lock_guard<tbb::spin_mutex> lock(mut);
  return cycleCheckNaive();
}

bool SerializationGraph::cycleCheckNaive() {
  bool check = false;
  visited.clear();
  visit_path.clear();
  for (auto node : node_map_) {
    // std::cout << node_map_.size() << std::endl;
    if (std::end(visited) == std::find(std::begin(visited), std::end(visited), node->transaction_)) {
      check |= cycleCheckNaive(*node);
    }
  }
  return check;
}

bool SerializationGraph::cycleCheckExternal(uint64_t transaction) {
  std::lock_guard<tbb::spin_mutex> lock(mut);
  return cycleCheckNaive(transaction);
}

bool SerializationGraph::cycleCheckNaive(uint64_t transaction) {
  visited.clear();
  visit_path.clear();
  bool check = false;
  Node* node;
  bool lookup = node_map_.lookup(transaction, node);
  if (lookup) {
    // std::cout << node_map_.size() << std::endl;
    if (std::end(visited) == std::find(std::begin(visited), std::end(visited), node->transaction_)) {
      check |= cycleCheckNaive(*node);
    }
  }
  return check;
}

bool SerializationGraph::cycleCheckNaive(Node& cur) const {
  uint64_t transaction = cur.transaction_;

  visited.insert(transaction);
  visit_path.insert(transaction);

  auto it = cur.incoming_nodes_.begin();
  while (it != cur.incoming_nodes_.end()) {
    if (std::end(visit_path) != std::find(std::begin(visit_path), std::end(visit_path), *it))
      return true;
    else {
      Node* that_node;
      bool found = node_map_.lookup(*it, that_node);
      if (found && cycleCheckNaive(*that_node))
        return true;
    }
    ++it;
  }

  visit_path.erase(transaction);
  return false;
}

bool SerializationGraph::needsAbort(uint64_t transaction) {
  std::lock_guard<tbb::spin_mutex> lock(mut);
  Node* this_node;
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup)
    return (this_node->cascading_abort_ || this_node->abort_);
  return false;
}

bool SerializationGraph::isCommited(uint64_t transaction) {
  std::lock_guard<tbb::spin_mutex> lock(mut);
  Node* this_node;
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup)
    return this_node->commited_;
  return false;
}

void SerializationGraph::abort(uint64_t transaction, std::unordered_set<uint64_t>& oset) {
  std::lock_guard<tbb::spin_mutex> lock(mut);
  Node* this_node;
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup) {
    this_node->abort_ = true;
    // std::cout << "abort-rem: " << transaction << std::endl;

#ifdef SGLOGGER
    bool cascading = this_node->cascading_abort_;
    logger.log(generateString());
    if (cascading)
      logger.log(common::LogInfo{transaction, 0, 0, 0, 'x'});
    else
      logger.log(common::LogInfo{transaction, 0, 0, 0, 'a'});
#endif
    auto it_in = this_node->incoming_nodes_.begin();
    while (it_in != this_node->incoming_nodes_.end()) {
      oset.emplace(reinterpret_cast<uintptr_t>(*it_in));
      it_in++;
    }
    auto it_out = this_node->outgoing_nodes_.begin();
    while (it_out != this_node->outgoing_nodes_.end()) {
      oset.emplace(reinterpret_cast<uintptr_t>(*it_out));
      it_out++;
    }
    cleanup(this_node);
    node_map_.erase(transaction);
  } else {
    assert(false);
    std::cout << "Unlucky abort" << std::endl;
  }
}

bool SerializationGraph::checkCommited(uint64_t transaction) {
  std::lock_guard<tbb::spin_mutex> lock(mut);
  Node* this_node;
  bool lookup = node_map_.lookup(transaction, this_node);
  if (!lookup) {
    return false;
  }

  if (this_node->abort_ || this_node->cascading_abort_) {
    return false;
  }

  for (uint32_t i = 0; lookup && this_node->incoming_nodes_.size() != 0; i++) {
    if (i >= 1000) {
      return false;
    }
  }

  if (this_node->abort_ || this_node->cascading_abort_) {
    return false;
  }

  bool success = erase_graph_constraints(transaction);
  if (success) {
    cleanup(this_node);
    node_map_.erase(transaction);
  }
  return success;
}

bool SerializationGraph::erase_graph_constraints(uint64_t transaction) {
  Node* this_node;
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup) {
    if (cycleCheckNaive(transaction)) {
      return false;
    }
    this_node->commited_ = true;

#ifdef SGLOGGER
    logger.log(generateString());
    logger.log(common::LogInfo{transaction, 0, 0, 0, 'c'});
#endif
    return true;
  }

  // std::cout << "Incoming nodes for " << transaction << ": " << this_node->outgoing_nodes_.size() << std::endl;
  assert(false);
  return false;
}

std::string SerializationGraph::generateString() {
  std::stringstream s;
  s << "[";
  for (auto node : node_map_) {
    s << "\t{transaction: " << node->transaction_ << ", aborted: " << node->abort_ << ", alive: " << node->last_alive_
      << ", commited: " << node->commited_ << ", commit_ctr_: " << node->commit_ctr_
      << ", cascading_abort: " << node->cascading_abort_ << ", incoming_nodes_: [" << std::endl;
    auto it = node->incoming_nodes_.begin();
    while (it != node->incoming_nodes_.end()) {
      s << "\t\t{transaction: " << *it << "}, " << std::endl;
      ++it;
    }
    s << "\t], outgoing_nodes_: [" << std::endl;
    auto itt = node->outgoing_nodes_.begin();
    while (itt != node->outgoing_nodes_.end()) {
      s << "\t\t{transaction: " << *itt << "}, " << std::endl;
      ++itt;
    }
    s << "\t]}," << std::endl;
  }
  s << "]" << std::endl;
  return s.str();
}

void SerializationGraph::print() {
  std::lock_guard<tbb::spin_mutex> lock(mut);
  std::cout << generateString() << std::endl;
}

void SerializationGraph::log(const common::LogInfo log_info) {
  logger.log(log_info);
}

void SerializationGraph::log(const std::string log_info) {
  logger.log(log_info);
}
};  // namespace serial
};  // namespace locked
