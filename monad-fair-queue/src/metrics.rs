// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

pub const COUNTER_FAIR_QUEUE_PUSH_TOTAL: &str = "monad.fair_queue.push.total";
pub const COUNTER_FAIR_QUEUE_PUSH_PRIORITY: &str = "monad.fair_queue.push.priority";
pub const COUNTER_FAIR_QUEUE_PUSH_REGULAR: &str = "monad.fair_queue.push.regular";
pub const COUNTER_FAIR_QUEUE_PUSH_ERROR_FULL: &str = "monad.fair_queue.push.error.full";
pub const COUNTER_FAIR_QUEUE_PUSH_ERROR_PER_ID_LIMIT: &str =
    "monad.fair_queue.push.error.per_id_limit";

pub const COUNTER_FAIR_QUEUE_POP_TOTAL: &str = "monad.fair_queue.pop.total";
pub const COUNTER_FAIR_QUEUE_POP_EMPTY: &str = "monad.fair_queue.pop.empty";
pub const COUNTER_FAIR_QUEUE_POP_FROM_PRIORITY: &str = "monad.fair_queue.pop.from_priority";
pub const COUNTER_FAIR_QUEUE_POP_FROM_REGULAR: &str = "monad.fair_queue.pop.from_regular";

pub const GAUGE_FAIR_QUEUE_PRIORITY_ITEMS: &str = "monad.fair_queue.priority_items";
pub const GAUGE_FAIR_QUEUE_REGULAR_ITEMS: &str = "monad.fair_queue.regular_items";
