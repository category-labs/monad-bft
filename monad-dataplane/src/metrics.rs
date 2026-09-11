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

use std::sync::Arc;

use monad_executor::{ExecutorMetrics, Gauge};

monad_executor::counter_labels! {
    pub(crate) struct ReceiveErrors {
        accept => "accept",
        header_io => "header_io",
        header_timeout => "header_timeout",
        invalid_magic => "invalid_magic",
        invalid_version => "invalid_version",
        message_too_large => "message_too_large",
        body_io => "body_io",
        body_timeout => "body_timeout",
    }
}

monad_executor::counter_labels! {
    pub(crate) struct ConnectionRejections {
        banned => "banned",
        connection_limit => "connection_limit",
        per_ip_limit => "per_ip_limit",
    }
}

macro_rules! define_metrics {
    ($($field:ident $(: $ty:ty)? => $constant:ident($name:literal, $help:literal $(, $label:literal)?)),+ $(,)?) => {
        monad_executor::metric_consts! {
            $($constant { name: $name, help: $help })+
        }

        #[derive(Clone)]
        pub(crate) struct DataplaneMetrics {
            executor_metrics: Arc<ExecutorMetrics>,
            $(pub(crate) $field: define_metrics!(@type $($ty)?),)+
        }

        impl DataplaneMetrics {
            pub(crate) fn new() -> Self {
                let gauge_defs: Vec<_> = [$(define_metrics!(@gauge_def $constant $(, $label)?)),+]
                    .into_iter().flatten().collect();
                let mut executor_metrics = ExecutorMetrics::with_metric_defs(&gauge_defs);
                Self {
                    $($field: define_metrics!(@init executor_metrics, $constant $(, $ty)? $(, $label)?),)+
                    executor_metrics: Arc::new(executor_metrics),
                }
            }

            pub(crate) fn executor_metrics(&self) -> &ExecutorMetrics {
                self.executor_metrics.as_ref()
            }
        }
    };
    (@type) => { Gauge };
    (@type $ty:ty) => { $ty };
    (@gauge_def $constant:ident) => { Some($constant) };
    (@gauge_def $constant:ident, $label:literal) => { None };
    (@init $metrics:ident, $constant:ident) => {
        $metrics.gauge($constant).clone()
    };
    (@init $metrics:ident, $constant:ident, $ty:ty, $label:literal) => {
        <$ty>::new(&mut $metrics, $constant, $label)
    };
}

define_metrics! {
    udp_messages_received => UDP_MESSAGES_RECEIVED("monad.dataplane.udp.total_messages_received", "Total UDP datagrams received from the network"),
    udp_bytes_received => UDP_BYTES_RECEIVED("monad.dataplane.udp.total_bytes_received", "Total UDP payload bytes received from the network"),
    udp_messages_sent => UDP_MESSAGES_SENT("monad.dataplane.udp.total_messages_sent", "Total UDP datagrams successfully sent to the network"),
    udp_bytes_sent => UDP_BYTES_SENT("monad.dataplane.udp.total_bytes_sent", "Total UDP payload bytes successfully sent to the network"),
    udp_receive_errors => UDP_RECEIVE_ERRORS("monad.dataplane.udp.total_receive_errors", "Total UDP socket receive errors"),
    udp_send_errors => UDP_SEND_ERRORS("monad.dataplane.udp.total_send_errors", "Total UDP socket send errors"),
    udp_egress_messages_dropped => UDP_EGRESS_MESSAGES_DROPPED("monad.dataplane.udp.total_egress_messages_dropped", "Total UDP egress messages dropped because a dataplane queue was full"),
    tcp_messages_received => TCP_MESSAGES_RECEIVED("monad.dataplane.tcp.total_messages_received", "Total TCP payload messages received from the network"),
    tcp_bytes_received => TCP_BYTES_RECEIVED("monad.dataplane.tcp.total_bytes_received", "Total TCP payload bytes received from the network"),
    tcp_messages_sent => TCP_MESSAGES_SENT("monad.dataplane.tcp.total_messages_sent", "Total TCP payload messages successfully sent to the network"),
    tcp_bytes_sent => TCP_BYTES_SENT("monad.dataplane.tcp.total_bytes_sent", "Total TCP payload bytes successfully sent to the network"),
    tcp_current_inbound_connections => TCP_CURRENT_INBOUND_CONNECTIONS("monad.dataplane.tcp.current_inbound_connections", "Current accepted inbound TCP connections"),
    tcp_current_outbound_connections => TCP_CURRENT_OUTBOUND_CONNECTIONS("monad.dataplane.tcp.current_outbound_connections", "Current established outbound TCP connections"),
    tcp_inbound_connections_accepted => TCP_INBOUND_CONNECTIONS_ACCEPTED("monad.dataplane.tcp.total_inbound_connections_accepted", "Total inbound TCP connections accepted by dataplane limits"),
    tcp_inbound_connections_rejected: ConnectionRejections => TCP_INBOUND_CONNECTIONS_REJECTED("monad.dataplane.tcp.inbound_connections_rejected_total", "Total inbound TCP connections rejected because the peer was banned or a connection limit was reached", "reason"),
    tcp_outbound_connections_established => TCP_OUTBOUND_CONNECTIONS_ESTABLISHED("monad.dataplane.tcp.total_outbound_connections_established", "Total outbound TCP connections successfully established"),
    tcp_outbound_connection_errors => TCP_OUTBOUND_CONNECTION_ERRORS("monad.dataplane.tcp.total_outbound_connection_errors", "Total outbound TCP connection attempts that failed or timed out"),
    tcp_receive_errors: ReceiveErrors => TCP_RECEIVE_ERRORS("monad.dataplane.tcp.receive_errors_total", "Total TCP accept, framing, or payload receive errors", "reason"),
    tcp_send_errors => TCP_SEND_ERRORS("monad.dataplane.tcp.total_send_errors", "Total TCP payload send errors or timeouts"),
    tcp_egress_messages_dropped => TCP_EGRESS_MESSAGES_DROPPED("monad.dataplane.tcp.total_egress_messages_dropped", "Total TCP egress messages dropped by dataplane limits, full queues, or failed connections"),
    tcp_connections_rate_limited => TCP_CONNECTIONS_RATE_LIMITED("monad.dataplane.tcp.total_connections_rate_limited", "Total inbound TCP connections closed after exceeding the per-connection message rate limit"),
}

pub(crate) struct ActiveConnectionGuard(Gauge);

impl ActiveConnectionGuard {
    /// Increments `total` once and `current` for the lifetime of the guard.
    pub(crate) fn new(total: &Gauge, current: &Gauge) -> Self {
        total.inc();
        current.inc();
        Self(current.clone())
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

#[cfg(test)]
mod tests {
    use monad_executor::ExecutorMetricsChain;

    use super::*;

    #[test]
    fn receive_errors_are_exported_by_reason_from_shared_handles() {
        let metrics = DataplaneMetrics::new();
        let worker = metrics.clone();
        worker.tcp_receive_errors.header_timeout.inc();
        worker.tcp_receive_errors.invalid_magic.inc_by(2);
        let families = ExecutorMetricsChain::from(metrics.executor_metrics()).counter_families();
        assert_eq!(families.len(), 2);
        let family = families
            .iter()
            .find(|family| family.definition().name == TCP_RECEIVE_ERRORS.name)
            .unwrap();
        assert_eq!(family.label_name(), "reason");
        assert_eq!(
            family.samples().collect::<Vec<_>>(),
            [
                ("accept", 0),
                ("header_io", 0),
                ("header_timeout", 1),
                ("invalid_magic", 2),
                ("invalid_version", 0),
                ("message_too_large", 0),
                ("body_io", 0),
                ("body_timeout", 0),
            ]
        );
    }
}
