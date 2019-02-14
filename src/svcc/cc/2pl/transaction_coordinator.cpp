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

#include "svcc/cc/2pl/transaction_coordinator.hpp"

using namespace twopl;

template <>
thread_local std::atomic<uint64_t> transaction::TransactionCoordinator<common::StdAllocator>::transaction_counter_(0);
template <>
thread_local std::atomic<uint64_t> transaction::TransactionCoordinator<common::ChunkAllocator>::transaction_counter_(0);

template <>
thread_local uint8_t transaction::TransactionCoordinator<common::StdAllocator>::current_core_(0xFF);
template <>
thread_local uint8_t transaction::TransactionCoordinator<common::ChunkAllocator>::current_core_(0xFF);

template <>
thread_local std::unordered_set<uint64_t> transaction::TransactionCoordinator<common::StdAllocator>::not_alive_{};
template <>
thread_local std::unordered_set<uint64_t> transaction::TransactionCoordinator<common::ChunkAllocator>::not_alive_{};

template <>
thread_local atom::AtomicSinglyLinkedList<transaction::TransactionInformationBase<common::StdAllocator>*>*
    transaction::TransactionCoordinator<common::StdAllocator>::atom_info_{};
template <>
thread_local atom::AtomicSinglyLinkedList<transaction::TransactionInformationBase<common::ChunkAllocator>*>*
    transaction::TransactionCoordinator<common::ChunkAllocator>::atom_info_{};

template <>
thread_local atom::EpochGuard<atom::EpochManagerBase<common::StdAllocator>, atom::EpochManager<common::StdAllocator>>*
    transaction::TransactionCoordinator<common::StdAllocator>::eg_ = nullptr;
template <>
thread_local atom::EpochGuard<atom::EpochManagerBase<common::ChunkAllocator>,
                              atom::EpochManager<common::ChunkAllocator>>*
    transaction::TransactionCoordinator<common::ChunkAllocator>::eg_ = nullptr;
