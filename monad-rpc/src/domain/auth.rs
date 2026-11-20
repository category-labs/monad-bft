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

//! Access tokens for the domain RPC.
//!
//! Every HTTP request must prove control of an account. The proof is an EIP-712 signature
//! over the domain chain id, carried in the [`AUTH_HEADER`] HTTP header:
//!
//! ```text
//! domain  = { name: "Monad Domain RPC", version: "1", chainId: <domain chain id> }
//! message = DomainAccessToken { chainId: <domain chain id> }
//! header  = <domain chain id>.<0x-prefixed 65-byte signature>
//! ```
//!
//! The signer is the authenticated account of every JSON-RPC call in the HTTP request. The
//! token carries no expiry: it authenticates its signer on that domain for as long as the
//! key is in use, so it must be handled like a password. It is a bearer credential that is
//! not bound to the request body, so the domain RPC must only be served over TLS. Only
//! externally owned accounts can authenticate: EIP-1271 contract signatures are not
//! supported.

use std::borrow::Cow;

use actix_web::HttpRequest;
use alloy_primitives::{hex, Address, Signature, B256, U256};
use alloy_sol_types::{sol, Eip712Domain, SolStruct};

use crate::types::jsonrpc::JsonRpcError;

/// HTTP header carrying the access token.
pub const AUTH_HEADER: &str = "X-Authorization-Token";
/// JSON-RPC error code of a request without a valid access token.
pub const UNAUTHORIZED_ERROR_CODE: i32 = -32006;

const EIP712_DOMAIN_NAME: &str = "Monad Domain RPC";
const EIP712_DOMAIN_VERSION: &str = "1";

sol! {
    /// EIP-712 message an account signs to authenticate its requests. `chainId` is the
    /// domain chain id the token grants access to.
    struct DomainAccessToken {
        uint64 chainId;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("missing {AUTH_HEADER} header")]
    MissingToken,
    #[error("malformed {AUTH_HEADER} header: expected <chainId>.<signature>")]
    MalformedToken,
    #[error("token chain id {token} does not match domain chain id {domain}")]
    ChainIdMismatch { token: u64, domain: u64 },
    #[error("invalid token signature")]
    InvalidSignature,
}

impl From<AuthError> for JsonRpcError {
    fn from(err: AuthError) -> Self {
        Self {
            code: UNAUTHORIZED_ERROR_CODE,
            message: format!("Unauthorized: {err}"),
            data: None,
        }
    }
}

/// The hash an account signs for `eth_signTypedData_v4` to access `domain_chain_id`. The
/// chain id is also the EIP-712 domain's, so a token signed for one domain does not
/// authenticate its signer on another.
fn signing_hash(domain_chain_id: u64) -> B256 {
    let domain = Eip712Domain::new(
        Some(Cow::Borrowed(EIP712_DOMAIN_NAME)),
        Some(Cow::Borrowed(EIP712_DOMAIN_VERSION)),
        Some(U256::from(domain_chain_id)),
        None,
        None,
    );
    DomainAccessToken {
        chainId: domain_chain_id,
    }
    .eip712_signing_hash(&domain)
}

/// The account that signed `token` for `domain_chain_id`. The signature may be given with
/// or without the `0x` prefix.
fn authenticate_token(token: &str, domain_chain_id: u64) -> Result<Address, AuthError> {
    let (chain_id, signature) = token
        .trim()
        .split_once('.')
        .ok_or(AuthError::MalformedToken)?;
    let token_chain_id = chain_id
        .parse::<u64>()
        .map_err(|_| AuthError::MalformedToken)?;
    let signature = signature
        .strip_prefix("0x")
        .or_else(|| signature.strip_prefix("0X"))
        .unwrap_or(signature);
    let signature = hex::decode(signature)
        .ok()
        .and_then(|bytes| Signature::from_raw(&bytes).ok())
        .ok_or(AuthError::MalformedToken)?;

    if token_chain_id != domain_chain_id {
        return Err(AuthError::ChainIdMismatch {
            token: token_chain_id,
            domain: domain_chain_id,
        });
    }
    signature
        .recover_address_from_prehash(&signing_hash(domain_chain_id))
        .map_err(|_| AuthError::InvalidSignature)
}

/// The account that `req` authenticates as on `domain_chain_id`, from its [`AUTH_HEADER`]
/// header.
pub fn authenticate(req: &HttpRequest, domain_chain_id: u64) -> Result<Address, AuthError> {
    let token = req
        .headers()
        .get(AUTH_HEADER)
        .ok_or(AuthError::MissingToken)?
        .to_str()
        .map_err(|_| AuthError::MalformedToken)?;
    authenticate_token(token, domain_chain_id)
}

/// An access token for `domain_chain_id` signed by `signer`.
#[cfg(test)]
pub fn sign_token(signer: &impl alloy_signer::SignerSync, domain_chain_id: u64) -> String {
    let signature = signer
        .sign_hash_sync(&signing_hash(domain_chain_id))
        .unwrap();
    format!(
        "{domain_chain_id}.{}",
        hex::encode_prefixed(signature.as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{keccak256, B256};
    use alloy_signer_local::PrivateKeySigner;
    use alloy_sol_types::SolStruct;

    use super::{authenticate_token, sign_token, signing_hash, AuthError, DomainAccessToken};

    const CHAIN_ID: u64 = (1 << 16) | 20143;

    fn signer() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap()
    }

    #[test]
    fn recovers_signer() {
        let token = sign_token(&signer(), CHAIN_ID);
        assert_eq!(authenticate_token(&token, CHAIN_ID), Ok(signer().address()));

        let unprefixed = token.replace(".0x", ".");
        assert_ne!(unprefixed, token);
        assert_eq!(
            authenticate_token(&unprefixed, CHAIN_ID),
            Ok(signer().address())
        );
    }

    #[test]
    fn rejects_malformed_tokens() {
        let sixty_four_zero_bytes = "00".repeat(64);
        for token in [
            "",
            "abc",
            "1",
            "1.0xzz",
            "1.0x00.extra",
            "-1.0x00",
            &format!("{CHAIN_ID}.0x{sixty_four_zero_bytes}"),
        ] {
            assert_eq!(
                authenticate_token(token, CHAIN_ID),
                Err(AuthError::MalformedToken),
                "{token:?}"
            );
        }
    }

    #[test]
    fn rejects_unrecoverable_signature() {
        let token = format!("{CHAIN_ID}.0x{}", "00".repeat(65));
        assert_eq!(
            authenticate_token(&token, CHAIN_ID),
            Err(AuthError::InvalidSignature)
        );
    }

    #[test]
    fn rejects_token_of_another_domain() {
        let token = sign_token(&signer(), CHAIN_ID);
        assert_eq!(
            authenticate_token(&token, CHAIN_ID + 1),
            Err(AuthError::ChainIdMismatch {
                token: CHAIN_ID,
                domain: CHAIN_ID + 1,
            })
        );

        // Relabelling the token for the other domain does not authenticate its signer there.
        let relabelled = token.replacen(&CHAIN_ID.to_string(), &(CHAIN_ID + 1).to_string(), 1);
        assert_ne!(
            authenticate_token(&relabelled, CHAIN_ID + 1),
            Ok(signer().address())
        );
    }

    #[test]
    fn signing_hash_is_eip712_typed_data_hash() {
        assert_eq!(
            DomainAccessToken::eip712_encode_type(),
            "DomainAccessToken(uint64 chainId)"
        );

        let mut domain =
            keccak256("EIP712Domain(string name,string version,uint256 chainId)").to_vec();
        domain.extend_from_slice(keccak256("Monad Domain RPC").as_slice());
        domain.extend_from_slice(keccak256("1").as_slice());
        domain.extend_from_slice(B256::left_padding_from(&CHAIN_ID.to_be_bytes()).as_slice());

        let mut message = keccak256("DomainAccessToken(uint64 chainId)").to_vec();
        message.extend_from_slice(B256::left_padding_from(&CHAIN_ID.to_be_bytes()).as_slice());

        let mut preimage = vec![0x19, 0x01];
        preimage.extend_from_slice(keccak256(domain).as_slice());
        preimage.extend_from_slice(keccak256(message).as_slice());
        assert_eq!(signing_hash(CHAIN_ID), keccak256(preimage));
    }
}
