#[macro_export]
macro_rules! include_proto {
    ($proto_name:ident) => {
        pub mod $proto_name {
            include!(concat!(
                env!("OUT_DIR"),
                concat!("/monad_proto.", stringify!($proto_name), ".rs")
            ));
            include!(concat!(
                env!("OUT_DIR"),
                concat!("/monad_proto.", stringify!($proto_name), ".serde.rs")
            ));
        }
    };
}

pub mod error;

pub mod proto {
    include_proto!(basic);
    include_proto!(compress);
    include_proto!(signing);
    include_proto!(voting);
    include_proto!(ledger);
    include_proto!(timeout);
    include_proto!(quorum_certificate);
    include_proto!(block);
    include_proto!(message);
    include_proto!(event);
    include_proto!(validator_data);
    include_proto!(state_root_hash);
}
