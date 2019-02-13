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

#include "mvcc/cc/nofalsenegatives/serialization_graph.hpp"
#include <tuple>

namespace mv {
namespace nofalsenegatives {
namespace serial {

thread_local std::unordered_set<Node*> SerializationGraph::visited{std::thread::hardware_concurrency()};
thread_local std::unordered_set<Node*> SerializationGraph::visit_path{std::thread::hardware_concurrency() >= 32
                                                                          ? std::thread::hardware_concurrency() >> 4
                                                                          : std::thread::hardware_concurrency()};

thread_local RecycledNodeSets SerializationGraph::empty_sets{};
thread_local Node* SerializationGraph::this_node{};
thread_local atom::EpochGuard<SerializationGraph::NEMB, SerializationGraph::NEM>* SerializationGraph::neg_ = nullptr;

SerializationGraph::SerializationGraph(Allocator* alloc, EMB* em)
    : alloc_(alloc), em_(em), noalloc_(), nem_(&noalloc_), created_sets_(0) {}

SerializationGraph::~SerializationGraph() {
  auto a = new Node::NodeSet{std::thread::hardware_concurrency() >= 32 ? std::thread::hardware_concurrency() >> 4
                                                                       : std::thread::hardware_concurrency(),
                             alloc_, em_};
  std::cout << "size of sets: " << sizeof(*a) << std::endl;
  std::cout << "empty sets at the end: " << created_sets_ << std::endl;
  delete a;
}

uintptr_t SerializationGraph::createNode() {
  this_node = alloc_->allocate<Node>(1);

  Node::NodeSet* sets[2];

  uint8_t i = 0;
  for (; i < 2 && !empty_sets.rns->empty();) {
    sets[i] = empty_sets.rns->back().release();
    empty_sets.rns->pop_back();
    i++;
  }
  for (; i < 2; i++) {
    created_sets_++;
    sets[i] = new Node::NodeSet{std::thread::hardware_concurrency() >= 32 ? std::thread::hardware_concurrency() >> 4
                                                                          : std::thread::hardware_concurrency(),
                                alloc_, em_};
  }

  new (this_node) Node{sets[0], sets[1]};
  return reinterpret_cast<uintptr_t>(this_node);
}

void SerializationGraph::setInactive() {}

void SerializationGraph::waitAndTidy() {}

void SerializationGraph::cleanup() {
  this_node->mut_.lock_shared();
  this_node->cleaned_ = true;
  this_node->mut_.unlock_shared();

  // barrier for inserts
  this_node->mut_.lock();
  this_node->mut_.unlock();

  auto it = this_node->outgoing_nodes_->begin();
  while (it != this_node->outgoing_nodes_->end()) {
    auto that_node = std::get<0>(findEdge(*it));
    if (this_node->abort_ && !std::get<1>(findEdge(*it))) {
      that_node->cascading_abort_ = true;
      that_node->abort_through_ = reinterpret_cast<uintptr_t>(this_node);
    } else {
      that_node->mut_.lock_shared();
      if (!that_node->cleaned_)
        that_node->incoming_nodes_->erase(accessEdge(this_node, std::get<1>(findEdge(*it))));
      that_node->mut_.unlock_shared();
    }
    this_node->outgoing_nodes_->erase(*it);
    ++it;
  }

  if (this_node->abort_) {
    auto it_out = this_node->incoming_nodes_->begin();
    while (it_out != this_node->incoming_nodes_->end()) {
      this_node->incoming_nodes_->erase(*it_out);
      ++it_out;
    }
  }

  atom::EpochGuard<EMB, EM> eg{em_};

  this_node->mut_.lock();
  empty_sets.rns->emplace_back(this_node->outgoing_nodes_);
  empty_sets.rns->emplace_back(this_node->incoming_nodes_);

  if (this_node->outgoing_nodes_->size() > 0 || this_node->incoming_nodes_->size() > 0) {
    std::cout << "BROKEN" << std::endl;
  }

  this_node->outgoing_nodes_ = nullptr;
  this_node->incoming_nodes_ = nullptr;
  this_node->mut_.unlock();

  // delete node;
  eg.add(this_node);
  // alloc_->deallocate(node, 1);
}

bool SerializationGraph::find(Node::NodeSet& nodes, Node* transaction) const {
  auto it = nodes.begin();
  while (it != nodes.end()) {
    if (*it == transaction) {
      return true;
    }

    ++it;
  }
  return false;
}

bool SerializationGraph::insert_and_check(uintptr_t from_node, bool readwrite) {
  Node* that_node = reinterpret_cast<Node*>(from_node);
  if (from_node == 0 || that_node == this_node) {
    return true;
  }

  while (true) {
    if (!find(*this_node->incoming_nodes_, accessEdge(that_node, readwrite))) {
      if ((that_node->abort_ || that_node->cascading_abort_) && !readwrite) {
        this_node->cascading_abort_ = true;
        this_node->abort_through_ = from_node;
        return false;
      }

      that_node->mut_.lock_shared();
      if (that_node->cleaned_) {
        that_node->mut_.unlock_shared();
        return true;
      }

      if (that_node->checked_) {
        that_node->mut_.unlock_shared();
        continue;
      }

      this_node->incoming_nodes_->insert(accessEdge(that_node, readwrite));
      that_node->outgoing_nodes_->insert(accessEdge(this_node, readwrite));

      that_node->mut_.unlock_shared();

      bool cycle = !cycleCheckNaive();
      return cycle;
    }
    return true;
  }
}

bool SerializationGraph::cycleCheckNaive() {
  visited.clear();
  visit_path.clear();
  bool check = false;
  if (std::end(visited) == std::find(std::begin(visited), std::end(visited), this_node)) {
    check |= cycleCheckNaive(this_node);
  }
  return check;
}

bool SerializationGraph::cycleCheckNaive(Node* cur) const {
  visited.insert(cur);
  visit_path.insert(cur);

  cur->mut_.lock_shared();
  if (!cur->cleaned_) {
    auto it = cur->incoming_nodes_->begin();
    while (it != cur->incoming_nodes_->end()) {
      auto node = std::get<0>(findEdge(*it));
      if (std::end(visit_path) != std::find(std::begin(visit_path), std::end(visit_path), node)) {
        cur->mut_.unlock_shared();
        return true;
      } else {
        if (cycleCheckNaive(node)) {
          cur->mut_.unlock_shared();
          return true;
        }
      }
      ++it;
    }
  }

  cur->mut_.unlock_shared();
  visit_path.erase(cur);
  return false;
}

bool SerializationGraph::needsAbort(uintptr_t cur) {
  auto node = reinterpret_cast<Node*>(cur);
  return (node->cascading_abort_ || node->abort_);
}

bool SerializationGraph::isCommited(uintptr_t cur) {
  return reinterpret_cast<Node*>(cur)->commited_;
}

void SerializationGraph::abort(std::unordered_set<uint64_t>& oset) {
  this_node->abort_ = true;

#ifdef SGLOGGER
  bool cascading = this_node->cascading_abort_;
  // logger.log(generateString());
  if (cascading)
    logger.log(common::LogInfo{reinterpret_cast<uintptr_t>(this_node), 0, 0, 0, 'x'});
  else
    logger.log(common::LogInfo{reinterpret_cast<uintptr_t>(this_node), 0, 0, 0, 'a'});
#endif

  auto it_in = this_node->incoming_nodes_->begin();
  while (it_in != this_node->incoming_nodes_->end()) {
    if (!std::get<1>(findEdge(*it_in)))
      oset.emplace(reinterpret_cast<uintptr_t>(std::get<0>(findEdge(*it_in))));
    it_in++;
  }

  cleanup();

  oset.emplace(this_node->abort_through_);
}

bool SerializationGraph::checkCommited() {
  if (this_node->abort_ || this_node->cascading_abort_) {
    return false;
  }

  this_node->mut_.lock_shared();
  this_node->checked_ = true;
  this_node->mut_.unlock_shared();

  // barrier for inserts
  this_node->mut_.lock();
  this_node->mut_.unlock();

  this_node->mut_.lock_shared();
  if (this_node->incoming_nodes_->size() != 0) {
    this_node->checked_ = false;
    this_node->mut_.unlock_shared();
    return false;
  }
  this_node->mut_.unlock_shared();

  if (this_node->abort_ || this_node->cascading_abort_) {
    return false;
  }

  bool success = erase_graph_constraints();

  if (success) {
    cleanup();
  }
  return success;
}

bool SerializationGraph::erase_graph_constraints() {
  if (cycleCheckNaive()) {
    this_node->abort_ = true;
    return false;
  }

  this_node->commited_ = true;

#ifdef SGLOGGER
  // logger.log(generateString());
  logger.log(common::LogInfo{reinterpret_cast<uintptr_t>(this_node), 0, 0, 0, 'c'});
#endif
  return true;
}

std::string SerializationGraph::generateString() {
  std::stringstream s;
  s << "[";
  /*for (auto node : node_map_) {
    s << "\t{transaction: " << node->transaction_ << ", aborted: " << node->abort_ << ", alive: " << node->last_alive_
      << ", commited: " << node->commited_ << ", commit_ctr_: " << node->commit_ctr_
      << ", cascading_abort: " << node->cascading_abort_ << ", incoming_nodes_: [" << std::endl;
    auto it = node->incoming_nodes_->begin();
    while (it != node->incoming_nodes_->end()) {
      s << "\t\t{transaction: " << *it << "}, " << std::endl;
      ++it;
    }
    s << "\t], outgoing_nodes_: [" << std::endl;
    auto itt = node->outgoing_nodes_->begin();
    while (itt != node->outgoing_nodes_->end()) {
      s << "\t\t{transaction: " << *itt << "}, " << std::endl;
      ++itt;
    }
    s << "\t]}," << std::endl;
  }
  s << "]" << std::endl;*/
  return s.str();
}

void SerializationGraph::print() {
  atom::EpochGuard<EMB, EM> eg{em_};
  std::cout << generateString() << std::endl;
}

void SerializationGraph::log(const common::LogInfo log_info) {
  atom::EpochGuard<EMB, EM> eg{em_};
  logger.log(log_info);
}

void SerializationGraph::log(const std::string log_info) {
  atom::EpochGuard<EMB, EM> eg{em_};
  logger.log(log_info);
}
};  // namespace serial
};  // namespace nofalsenegatives
};  // namespace mv
