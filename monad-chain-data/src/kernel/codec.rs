#![allow(dead_code, unused_imports, unused_macros)]

pub trait StorageCodec: Sized {
    fn encode(&self) -> bytes::Bytes;
    fn decode(bytes: &[u8]) -> crate::error::Result<Self>;
}

macro_rules! fixed_codec_field_len {
    (u64) => {
        8usize
    };
    (u32) => {
        4usize
    };
    ([u8; $len:expr]) => {
        $len
    };
}
pub(crate) use fixed_codec_field_len;

macro_rules! fixed_codec_encode_field {
    ($out:expr, $value:expr, u64) => {
        $out.extend_from_slice(&$value.to_be_bytes());
    };
    ($out:expr, $value:expr, u32) => {
        $out.extend_from_slice(&$value.to_be_bytes());
    };
    ($out:expr, $value:expr, [u8; $len:expr]) => {
        $out.extend_from_slice(&$value);
    };
}
pub(crate) use fixed_codec_encode_field;

macro_rules! fixed_codec_decode_field {
    ($bytes:expr, $offset:expr, u64) => {{
        let mut value = [0u8; 8];
        value.copy_from_slice(&$bytes[$offset..$offset + 8]);
        u64::from_be_bytes(value)
    }};
    ($bytes:expr, $offset:expr, u32) => {{
        let mut value = [0u8; 4];
        value.copy_from_slice(&$bytes[$offset..$offset + 4]);
        u32::from_be_bytes(value)
    }};
    ($bytes:expr, $offset:expr, [u8; $len:expr]) => {{
        let mut value = [0u8; $len];
        value.copy_from_slice(&$bytes[$offset..$offset + $len]);
        value
    }};
}
pub(crate) use fixed_codec_decode_field;

macro_rules! fixed_codec {
    (
        impl $ty:ty {
            length_error = $length_error:literal;
            version = $version:expr;
            version_error = $version_error:literal;
            fields {
                $($field:ident : $field_ty:tt),+ $(,)?
            }
        }
    ) => {
        impl $crate::kernel::codec::StorageCodec for $ty {
            fn encode(&self) -> bytes::Bytes {
                const ENCODE_LEN: usize = 1usize $(+ $crate::kernel::codec::fixed_codec_field_len!($field_ty))+;
                let mut out = Vec::with_capacity(ENCODE_LEN);
                out.push($version);
                $($crate::kernel::codec::fixed_codec_encode_field!(out, self.$field, $field_ty);)+
                bytes::Bytes::from(out)
            }

            fn decode(bytes: &[u8]) -> crate::error::Result<Self> {
                const DECODE_LEN: usize = 1usize $(+ $crate::kernel::codec::fixed_codec_field_len!($field_ty))+;
                if bytes.len() != DECODE_LEN {
                    return Err(crate::error::Error::Decode($length_error));
                }
                if bytes[0] != $version {
                    return Err(crate::error::Error::Decode($version_error));
                }
                let mut offset = 1usize;
                let value = Self {
                    $(
                        $field: {
                            let value = $crate::kernel::codec::fixed_codec_decode_field!(bytes, offset, $field_ty);
                            offset += $crate::kernel::codec::fixed_codec_field_len!($field_ty);
                            value
                        }
                    ),+
                };
                debug_assert_eq!(offset, DECODE_LEN);
                Ok(value)
            }
        }
    };
    (
        impl $ty:ty {
            length_error = $length_error:literal;
            fields {
                $($field:ident : $field_ty:tt),+ $(,)?
            }
        }
    ) => {
        impl $crate::kernel::codec::StorageCodec for $ty {
            fn encode(&self) -> bytes::Bytes {
                const ENCODE_LEN: usize = 0usize $(+ $crate::kernel::codec::fixed_codec_field_len!($field_ty))+;
                let mut out = Vec::with_capacity(ENCODE_LEN);
                $($crate::kernel::codec::fixed_codec_encode_field!(out, self.$field, $field_ty);)+
                bytes::Bytes::from(out)
            }

            fn decode(bytes: &[u8]) -> crate::error::Result<Self> {
                const DECODE_LEN: usize = 0usize $(+ $crate::kernel::codec::fixed_codec_field_len!($field_ty))+;
                if bytes.len() != DECODE_LEN {
                    return Err(crate::error::Error::Decode($length_error));
                }
                let mut offset = 0usize;
                let value = Self {
                    $(
                        $field: {
                            let value = $crate::kernel::codec::fixed_codec_decode_field!(bytes, offset, $field_ty);
                            offset += $crate::kernel::codec::fixed_codec_field_len!($field_ty);
                            value
                        }
                    ),+
                };
                debug_assert_eq!(offset, DECODE_LEN);
                Ok(value)
            }
        }
    };
}
use bytes::Bytes;
pub(crate) use fixed_codec;

pub(crate) fn encode_u64(value: u64) -> Bytes {
    Bytes::copy_from_slice(&value.to_be_bytes())
}
