use monad_metrics::metrics_bft;

metrics_bft! {
    Dataplane {
        #[counter]
        dropped_msgs_full_tcp,

        #[counter]
        dropped_msgs_full_udp,

        Tcp {
            Rx {
                #[counter]
                new_connection,

                #[counter]
                bytes,

                #[gauge]
                connections
            },
            Tx {
                #[counter]
                new_connection,

                #[counter]
                bytes,

                #[counter]
                bytes_success,

                #[gauge]
                connections
            }
        },

        Udp {
            Rx {
                #[counter]
                bytes,
            },
            Tx {
                #[counter]
                bytes,

                #[counter]
                bytes_success,
            }
        }
    }
}
