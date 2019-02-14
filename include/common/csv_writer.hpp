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

#include <fstream>
#include <iostream>
#include <string>

namespace common {

class CSVWriter {
 private:
  std::string log_name_;

 public:
  CSVWriter() { log_name_ = "result.csv"; }

  inline void log(const std::string s) {
    std::ofstream log_file(log_name_, std::ios_base::out | std::ios_base::app);
    log_file << s << std::endl;
  }

  inline void setLogName(std::string log_name) { log_name_ = log_name; }
};
};  // namespace common
