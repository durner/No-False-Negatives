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

#include "svcc/cc/step/serialization_graph.hpp"

namespace step {
namespace serial {

thread_local std::unordered_set<uint64_t> SerializationGraph::visited{std::thread::hardware_concurrency()};
thread_local std::unordered_set<uint64_t> SerializationGraph::visit_path{std::thread::hardware_concurrency() >= 32
                                                                             ? std::thread::hardware_concurrency() >> 4
                                                                             : std::thread::hardware_concurrency()};
thread_local std::vector<uint64_t> SerializationGraph::L{std::thread::hardware_concurrency()};
thread_local std::vector<std::pair<uint64_t, uint64_t>> SerializationGraph::R{std::thread::hardware_concurrency()};

thread_local std::vector<std::pair<uint64_t, uint64_t>> SerializationGraph::dF{std::thread::hardware_concurrency()};
thread_local std::vector<std::pair<uint64_t, uint64_t>> SerializationGraph::dB{std::thread::hardware_concurrency()};

thread_local std::queue<std::unique_ptr<Node::NodeMap>> SerializationGraph::empty_maps{};

SerializationGraph::SerializationGraph(Allocator* alloc, EMB* em, bool online)
    : node_map_(std::thread::hardware_concurrency() << 4, alloc, em),
      order_map_(std::thread::hardware_concurrency() << 4, alloc, em),
      order_version_(0),
      order_version_locked_(false),
      alloc_(alloc),
      em_(em),
      sm_(this, alloc_, em_),
      online_(online) {}

SerializationGraph::~SerializationGraph() {}

const uint64_t SerializationGraph::size() const {
  return node_map_.size();
}

void SerializationGraph::createNode(const uint64_t transaction) {
  Node* this_node;
  bool found = node_map_.lookup(transaction, this_node);
  if (!found) {
    Node* this_node = alloc_->allocate<Node>(1);
    if (empty_maps.size() < 2) {
      empty_maps.emplace(new Node::NodeMap{std::thread::hardware_concurrency() >= 32
                                               ? std::thread::hardware_concurrency() >> 4
                                               : std::thread::hardware_concurrency(),
                                           alloc_, em_});
      empty_maps.emplace(new Node::NodeMap{std::thread::hardware_concurrency() >= 32
                                               ? std::thread::hardware_concurrency() >> 4
                                               : std::thread::hardware_concurrency(),
                                           alloc_, em_});
    }
    auto in = empty_maps.front().release();
    empty_maps.pop();
    auto out = empty_maps.front().release();
    empty_maps.pop();

    new (this_node) Node{in, out};
    this_node->transaction_ = transaction;
    node_map_.insert(transaction, this_node);
    order_map_.insert(transaction, transaction);
  }
}

void SerializationGraph::cleanup(Node* node) {
  auto it = node->outgoing_nodes_->begin();
  while (it != node->outgoing_nodes_->end()) {
    Node* that_node;
    bool found = node_map_.lookup(it.getKey(), that_node);
    if (found) {
      if (node->abort_)
        that_node->cascading_abort_ = true;
      that_node->incoming_nodes_->erase(node->transaction_);
    }
    node->outgoing_nodes_->erase(it.getKey());
    ++it;
  }

  auto it_out = node->incoming_nodes_->begin();
  while (it_out != node->incoming_nodes_->end()) {
    Node* that_node;
    bool found = node_map_.lookup(it_out.getKey(), that_node);
    if (found)
      that_node->outgoing_nodes_->erase(node->transaction_);

    node->incoming_nodes_->erase(it_out.getKey());
    ++it_out;
  }

  empty_maps.emplace(node->outgoing_nodes_);
  empty_maps.emplace(node->incoming_nodes_);

  // delete node;
  // eg.add(node);
  node->outgoing_nodes_ = nullptr;
  node->incoming_nodes_ = nullptr;
  alloc_->deallocate(node, 1);
}

bool SerializationGraph::find(Node::NodeMap& nodes, uint64_t transaction, uint64_t ctr) const {
  auto it = nodes.begin();
  while (it != nodes.end()) {
    if (it.getKey() == transaction && *it <= ctr) {
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
  StepGuard sg{sm_};
  Node* this_node;
  bool found = node_map_.lookup(this_transaction, this_node);
  if (!found) {
    // std::cout << "missing transaction " << this_transaction << std::endl;
    assert(false);
  }

  if (!find(*this_node->incoming_nodes_, from_transaction, sg.getCtr())) {
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

    this_node->incoming_nodes_->insert(from_transaction, sg.getCtr());
    that_node->outgoing_nodes_->insert(this_transaction, sg.getCtr());

    bool cycle;

    if (online_)
      cycle = !cycleCheckOnline(this_transaction, from_transaction, sg.getCtr());
    else
      cycle = !cycleCheckNaive(sg.getCtr(), this_transaction);

    return cycle;
  }

  return true;
}

bool SerializationGraph::cycleCheckExternal() {
  StepGuard sg{sm_};
  return cycleCheckNaive(sg.getCtr());
}

bool SerializationGraph::cycleCheckOnline(const uint64_t this_transaction,
                                          const uint64_t from_transaction,
                                          uint64_t ctr) {
  bool version_match = false;
  while (!version_match) {
    auto version = order_version_.load();
    uint64_t lb = 0, ub = 0;
    bool lookup = order_map_.lookup(this_transaction, ub);
    lookup &= order_map_.lookup(from_transaction, lb);

    if (lookup) {
      if (lb < ub) {
        visited.clear();
        visit_path.clear();
        dF.clear();
        dB.clear();

        if (!dfsF(from_transaction, ub))
          return true;
        dfsB(this_transaction, lb);
        reorder();
        if (version == order_version_) {
          mut.lock();
          if (version != order_version_) {
            mut.unlock();
            continue;
          }
          order_version_locked_ = true;
          for (uint64_t i = 0; i < L.size(); ++i) {
            order_map_.replace(L[i], R[i].second);
          }
          order_version_++;
          order_version_locked_ = false;
          mut.unlock();

          version_match = true;
        }
      } else {
        if (version == order_version_ && !order_version_locked_) {
          version_match = true;
        }
      }
    } else {
      version_match = true;
      assert(false);
    }
  }
  return false;
}

bool SerializationGraph::dfsF(uint64_t n, uint64_t ub) const {
  Node* cur;
  bool lookup = node_map_.lookup(n, cur);
  bool ret = true;

  if (lookup) {
    uint64_t n_ord = 0;
    lookup &= order_map_.lookup(n, n_ord);
    visited.insert(n);
    dF.push_back(std::make_pair(n, n_ord));

    auto it = cur->incoming_nodes_->begin();
    while (it != cur->incoming_nodes_->end()) {
      uint64_t out = it.getKey();
      uint64_t ord = 0;
      lookup &= order_map_.lookup(out, ord);
      if (lookup) {
        if (ord == ub) {
          return false;
        }
        if (ord < ub && std::end(visited) == std::find(std::begin(visited), std::end(visited), out)) {
          ret &= dfsF(out, ub);
        }
      }
      it++;
    }
  }

  return ret;
}

bool SerializationGraph::dfsB(uint64_t n, uint64_t lb) const {
  Node* cur;
  bool lookup = node_map_.lookup(n, cur);
  bool ret = true;

  if (lookup) {
    uint64_t n_ord = 0;
    lookup &= order_map_.lookup(n, n_ord);
    visited.insert(n);
    dB.push_back(std::make_pair(n, n_ord));

    auto it = cur->outgoing_nodes_->begin();
    while (it != cur->outgoing_nodes_->end()) {
      uint64_t in_node = it.getKey();
      uint64_t ord = 0;
      lookup &= order_map_.lookup(in_node, ord);
      if (lookup) {
        if (ord > lb && std::end(visited) == std::find(std::begin(visited), std::end(visited), in_node)) {
          ret &= dfsB(in_node, lb);
        }
      }
      it++;
    }
  }

  return ret;
}

void SerializationGraph::reorder() const {
  auto sortLambda = [](const std::pair<uint64_t, uint64_t>& a, const std::pair<uint64_t, uint64_t>& b) -> bool {
    return a.second < b.second;
  };

  std::sort(dF.begin(), dF.end(), sortLambda);
  std::sort(dB.begin(), dB.end(), sortLambda);

  L.clear();
  R.clear();

  for (auto elem : dB) {
    L.push_back(elem.first);
  }

  for (auto elem : dF) {
    L.push_back(elem.first);
  }

  std::merge(dB.begin(), dB.end(), dF.begin(), dF.end(), R.begin(), sortLambda);
}

bool SerializationGraph::cycleCheckNaive(uint64_t ctr) {
  bool check = false;
  visited.clear();
  visit_path.clear();
  for (auto node : node_map_) {
    // std::cout << node_map_.size() << std::endl;
    if (std::end(visited) == std::find(std::begin(visited), std::end(visited), node->transaction_)) {
      check |= cycleCheckNaive(*node, ctr);
    }
  }
  return check;
}

bool SerializationGraph::cycleCheckExternal(uint64_t transaction) {
  StepGuard sg{sm_};
  return cycleCheckNaive(sg.getCtr(), transaction);
}

bool SerializationGraph::cycleCheckNaive(uint64_t ctr, uint64_t transaction) {
  visited.clear();
  visit_path.clear();
  bool check = false;
  Node* node;
  bool lookup = node_map_.lookup(transaction, node);
  if (lookup) {
    // std::cout << node_map_.size() << std::endl;
    if (std::end(visited) == std::find(std::begin(visited), std::end(visited), node->transaction_)) {
      check |= cycleCheckNaive(*node, ctr);
    }
  }
  return check;
}

bool SerializationGraph::cycleCheckNaive(Node& cur, uint64_t ctr) const {
  uint64_t transaction = cur.transaction_;

  visited.insert(transaction);
  visit_path.insert(transaction);

  auto it = cur.incoming_nodes_->begin();
  while (it != cur.incoming_nodes_->end()) {
    if (*it <= ctr) {
      if (std::end(visit_path) != std::find(std::begin(visit_path), std::end(visit_path), it.getKey()))
        return true;
      else {
        Node* that_node;
        bool found = node_map_.lookup(it.getKey(), that_node);
        if (found && cycleCheckNaive(*that_node, ctr))
          return true;
      }
    }
    ++it;
  }

  visit_path.erase(transaction);
  return false;
}

bool SerializationGraph::needsAbort(uint64_t transaction) {
  Node* this_node;
  StepGuard sg{sm_};
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup)
    return (this_node->cascading_abort_ || this_node->abort_);
  return false;
}

bool SerializationGraph::isCommited(uint64_t transaction) {
  Node* this_node;
  StepGuard sg{sm_};
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup)
    return this_node->commited_;
  return false;
}

void SerializationGraph::abort(uint64_t transaction, std::unordered_set<uint64_t>& oset) {
  StepGuard sg{sm_};
  Node* this_node;
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup) {
    sg.waitSaveRead();

    this_node->abort_ = true;
    node_map_.erase(transaction);
    order_map_.erase(transaction);
    // std::cout << "abort-rem: " << transaction << std::endl;

#ifdef SGLOGGER
    bool cascading = this_node->cascading_abort_;
    logger.log(generateString());
    if (cascading)
      logger.log(common::LogInfo{transaction, 0, 0, 0, 'x'});
    else
      logger.log(common::LogInfo{transaction, 0, 0, 0, 'a'});
#endif

    sg.destroy();

    StepGuard sg_clean{sm_};
    this_node->last_alive_ = sg_clean.getCtr();
    sg_clean.waitSaveRead();
    auto it_in = this_node->incoming_nodes_->begin();
    while (it_in != this_node->incoming_nodes_->end()) {
      oset.emplace(reinterpret_cast<uintptr_t>(*it_in));
      it_in++;
    }
    auto it_out = this_node->outgoing_nodes_->begin();
    while (it_out != this_node->outgoing_nodes_->end()) {
      oset.emplace(reinterpret_cast<uintptr_t>(*it_out));
      it_out++;
    }
    cleanup(this_node);
  } else {
    assert(false);
    std::cout << "Unlucky abort" << std::endl;
  }
}

bool SerializationGraph::checkCommited(uint64_t transaction) {
  Node* this_node;
  StepGuard sg{sm_};
  uint64_t ctr = sg.getCtr();
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup) {
    // if (this_node->commit_ctr_ > 0)
    //  ctr = this_node->commit_ctr_;
    // else
    this_node->commit_ctr_ = ctr;
  } else {
    return false;
  }

  if (this_node->abort_ || this_node->cascading_abort_) {
    return false;
  }

  for (uint32_t i = 0; lookup && this_node->incoming_nodes_->size() != 0; i++) {
    if (i >= 1000) {
      return false;
    }
  }

  if (this_node->abort_ || this_node->cascading_abort_) {
    return false;
  }

  sg.waitSaveRead();
  sg.destroy();

  StepGuard sg_check{sm_};

  bool success = erase_graph_constraints(transaction, sg_check.getCtr());
  if (success) {
    sg_check.destroy();
    StepGuard sg_clean{sm_};
    this_node->last_alive_ = sg_clean.getCtr();
    sg_clean.waitSaveRead();
    cleanup(this_node);
  }
  return success;
}

bool SerializationGraph::erase_graph_constraints(uint64_t transaction, uint64_t ctr) {
  Node* this_node;
  bool lookup = node_map_.lookup(transaction, this_node);
  if (lookup) {
    if (cycleCheckNaive(ctr, transaction)) {
      return false;
    }

    node_map_.erase(transaction);
    order_map_.erase(transaction);
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
    auto it = node->incoming_nodes_->begin();
    while (it != node->incoming_nodes_->end()) {
      s << "\t\t{transaction: " << it.getKey() << "}, " << std::endl;
      ++it;
    }
    s << "\t], outgoing_nodes_: [" << std::endl;
    auto itt = node->outgoing_nodes_->begin();
    while (itt != node->outgoing_nodes_->end()) {
      s << "\t\t{transaction: " << itt.getKey() << "}, " << std::endl;
      ++itt;
    }
    s << "\t]}," << std::endl;
  }
  s << "]" << std::endl;
  return s.str();
}

void SerializationGraph::print() {
  StepGuard sg{sm_};
  std::cout << generateString() << std::endl;
}

void SerializationGraph::log(const common::LogInfo log_info) {
  StepGuard sg{sm_};
  logger.log(log_info);
}

void SerializationGraph::log(const std::string log_info) {
  StepGuard sg{sm_};
  logger.log(log_info);
}
};  // namespace serial
};  // namespace step
