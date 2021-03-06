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

#include "common/optimistic_predicate_locking.hpp"

template <>
thread_local std::vector<std::pair<atom::AtomicSinglyLinkedList<common::Predicate>*, uint64_t>>
    common::OptimisticPredicateLocking<common::ChunkAllocator>::predicate_removal_{};
template <>
thread_local std::vector<std::pair<atom::AtomicSinglyLinkedList<common::Predicate>*, uint64_t>>
    common::OptimisticPredicateLocking<common::StdAllocator>::predicate_removal_{};
template <>
thread_local std::vector<std::pair<atom::AtomicSinglyLinkedList<common::Predicate>*, uint64_t>>
    common::OptimisticPredicateLocking<common::NoAllocator>::predicate_removal_{};
