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

use monad_executor::ExecutorMetrics;

monad_executor::metric_consts! {
    pub COUNTER_FAIR_QUEUE_PUSH_TOTAL {
        name: "monad.fair_queue.push.total",
        help: "Total successful fair-queue pushes",
    }
    pub COUNTER_FAIR_QUEUE_PUSH_PRIORITY {
        name: "monad.fair_queue.push.priority",
        help: "Total items pushed into the priority pool",
    }
    pub COUNTER_FAIR_QUEUE_PUSH_REGULAR {
        name: "monad.fair_queue.push.regular",
        help: "Total items pushed into the regular pool",
    }
    pub COUNTER_FAIR_QUEUE_PUSH_ERROR_FULL {
        name: "monad.fair_queue.push.error.full",
        help: "Total push attempts rejected because the destination pool was full",
    }
    pub COUNTER_FAIR_QUEUE_PUSH_ERROR_PER_ID_LIMIT {
        name: "monad.fair_queue.push.error.per_id_limit",
        help: "Total push attempts rejected because an identity exceeded its item limit",
    }
    pub COUNTER_FAIR_QUEUE_POP_TOTAL {
        name: "monad.fair_queue.pop.total",
        help: "Total successful fair-queue pops",
    }
    pub COUNTER_FAIR_QUEUE_POP_EMPTY {
        name: "monad.fair_queue.pop.empty",
        help: "Total pop attempts on an empty fair queue",
    }
    pub COUNTER_FAIR_QUEUE_POP_FROM_PRIORITY {
        name: "monad.fair_queue.pop.from_priority",
        help: "Total items popped from the priority pool",
    }
    pub COUNTER_FAIR_QUEUE_POP_FROM_REGULAR {
        name: "monad.fair_queue.pop.from_regular",
        help: "Total items popped from the regular pool",
    }
    pub GAUGE_FAIR_QUEUE_PRIORITY_ITEMS {
        name: "monad.fair_queue.priority_items",
        help: "Current number of items in the priority pool",
    }
    pub GAUGE_FAIR_QUEUE_REGULAR_ITEMS {
        name: "monad.fair_queue.regular_items",
        help: "Current number of items in the regular pool",
    }
}

pub fn init_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs([
        COUNTER_FAIR_QUEUE_PUSH_TOTAL,
        COUNTER_FAIR_QUEUE_PUSH_PRIORITY,
        COUNTER_FAIR_QUEUE_PUSH_REGULAR,
        COUNTER_FAIR_QUEUE_PUSH_ERROR_FULL,
        COUNTER_FAIR_QUEUE_PUSH_ERROR_PER_ID_LIMIT,
        COUNTER_FAIR_QUEUE_POP_TOTAL,
        COUNTER_FAIR_QUEUE_POP_EMPTY,
        COUNTER_FAIR_QUEUE_POP_FROM_PRIORITY,
        COUNTER_FAIR_QUEUE_POP_FROM_REGULAR,
        GAUGE_FAIR_QUEUE_PRIORITY_ITEMS,
        GAUGE_FAIR_QUEUE_REGULAR_ITEMS,
    ])
}
