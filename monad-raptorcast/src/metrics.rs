use monad_dataplane::metrics::DataplaneMetrics;
use monad_metrics::metrics_bft;

metrics_bft! {
    #[raptorcast]
    RaptorCast {
        #[counter]
        excess_chunks,

        #[counter]
        invalid_symbols,

        #[gauge]
        peers,

        #[p2p]
        PointToPoint {
            #[counter]
            app_message,

            #[counter]
            app_message_bytes,
        },

        Broadcast {
            #[counter]
            app_message,

            #[counter]
            app_message_bytes,

            #[counter]
            app_message_build_elapsed,
        },

        #[raptorcast]
        RaptorCast {
            #[counter]
            app_message,

            #[counter]
            app_message_bytes,

            #[counter]
            app_message_build_elapsed,
        },

        Rx {
            #[counter]
            message,

            #[counter]
            message_bytes,

            #[counter]
            message_reassembly_elapsed,
        }
    }
}

pub struct RaptorCastDataplaneMetrics {
    pub raptorcast: RaptorCastMetrics,
    pub dataplane: DataplaneMetrics,
}
