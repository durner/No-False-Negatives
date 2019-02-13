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

#include "mvcc/cc/mvocc/transaction_coordinator.hpp"
#include "ds/extent_vector.hpp"

template <>
thread_local std::unordered_set<uint64_t> mv::mvocc::transaction::
    TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::StdAllocator>::abort_transaction_{};
template <>
thread_local std::unordered_set<uint64_t> mv::mvocc::transaction::
    TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>::abort_transaction_{};

template <>
thread_local std::unordered_set<uint64_t> mv::mvocc::transaction::
    TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::StdAllocator>::not_alive_{};
template <>
thread_local std::unordered_set<uint64_t> mv::mvocc::transaction::
    TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>::not_alive_{};

template <>
thread_local std::list<mv::mvocc::transaction::TransactionInformationBase<atom::ExtentVector,
                                                                          atom::AtomicExtentVector,
                                                                          common::StdAllocator>*>*
    mv::mvocc::transaction::TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::StdAllocator>::
        atom_info_{};
template <>
thread_local std::list<mv::mvocc::transaction::TransactionInformationBase<atom::ExtentVector,
                                                                          atom::AtomicExtentVector,
                                                                          common::ChunkAllocator>*>*
    mv::mvocc::transaction::
        TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>::atom_info_{};

template <>
thread_local atom::EpochGuard<atom::EpochManagerBase<common::StdAllocator>, atom::EpochManager<common::StdAllocator>>*
    mv::mvocc::transaction::TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::StdAllocator>::
        eg_ = nullptr;
template <>
thread_local atom::EpochGuard<atom::EpochManagerBase<common::ChunkAllocator>,
                              atom::EpochManager<common::ChunkAllocator>>* mv::mvocc::transaction::
    TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>::eg_ = nullptr;

template <>
tbb::spin_mutex mv::mvocc::transaction::
    TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::StdAllocator>::mut{};
template <>
tbb::spin_mutex mv::mvocc::transaction::
    TransactionCoordinator<atom::ExtentVector, atom::AtomicExtentVector, common::ChunkAllocator>::mut{};
