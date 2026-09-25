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

//! Access tokens for the `/domain/{chain_id}` RPC route.
//!
//! Private domain state is only served to the account that owns it, so every request on
//! a domain route must prove control of an account. The proof is an EIP-712 signature
//! over the domain chain id, carried in the [`DOMAIN_AUTH_HEADER`] HTTP header:
//!
//! ```text
//! domain  = { name: "Monad Domain RPC", version: "1", chainId: <domain chain id> }
//! message = DomainAccessToken { chainId: <domain chain id> }
//! header  = <domain chain id>.<0x-prefixed 65-byte signature>
//! ```
//!
//! The server recovers the signer and treats it as the authenticated account for every
//! JSON-RPC call in the HTTP request. The token carries no expiry: it authenticates its
//! signer on that domain for as long as the key is in use, so it must be handled like a
//! password. It is a bearer credential that is not bound to the request body, so domain
//! RPC must only be served over TLS. Only externally owned accounts can authenticate:
//! EIP-1271 contract signatures are not supported.

use std::borrow::Cow;

use actix_web::HttpRequest;
use alloy_primitives::{hex, Address, Signature, B256, U256};
use alloy_signer::SignerSync;
use alloy_sol_types::{sol, Eip712Domain, SolStruct};

use crate::types::jsonrpc::JsonRpcError;

/// HTTP header carrying the access token on domain routes.
pub const DOMAIN_AUTH_HEADER: &str = "X-Authorization-Token";
/// EIP-712 domain name of the access token.
pub const EIP712_DOMAIN_NAME: &str = "Monad Domain RPC";
/// EIP-712 domain version of the access token.
pub const EIP712_DOMAIN_VERSION: &str = "1";

sol! {
    /// EIP-712 message a domain user signs to authenticate domain RPC requests.
    /// `chainId` is the domain chain id the token grants access to.
    #[derive(Debug, PartialEq, Eq)]
    struct DomainAccessToken {
        uint64 chainId;
    }
}

/// EIP-712 domain of the access token for `domain_chain_id`.
///
/// Binding the domain chain id into the domain means a token signed for one domain
/// cannot authenticate against another.
pub fn access_token_eip712_domain(domain_chain_id: u64) -> Eip712Domain {
    Eip712Domain::new(
        Some(Cow::Borrowed(EIP712_DOMAIN_NAME)),
        Some(Cow::Borrowed(EIP712_DOMAIN_VERSION)),
        Some(U256::from(domain_chain_id)),
        None,
        None,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainAuthError {
    #[error("missing {DOMAIN_AUTH_HEADER} header")]
    MissingToken,
    #[error("malformed {DOMAIN_AUTH_HEADER} header: expected <chainId>.<signature>")]
    MalformedToken,
    #[error("token chain id {token} does not match domain chain id {route}")]
    ChainIdMismatch { token: u64, route: u64 },
    #[error("invalid token signature")]
    InvalidSignature,
}

impl From<DomainAuthError> for JsonRpcError {
    fn from(err: DomainAuthError) -> Self {
        JsonRpcError::unauthorized(err.to_string())
    }
}

/// A signed [`DomainAccessToken`] as transported in the [`DOMAIN_AUTH_HEADER`]
/// header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedDomainAccessToken {
    pub domain_chain_id: u64,
    pub signature: Signature,
}

impl SignedDomainAccessToken {
    /// The EIP-712 message that is signed to access `domain_chain_id`.
    pub fn message(domain_chain_id: u64) -> DomainAccessToken {
        DomainAccessToken {
            chainId: domain_chain_id,
        }
    }

    /// The EIP-712 signing hash of the access token for `domain_chain_id`. This is the
    /// hash a wallet signs for `eth_signTypedData_v4` with [`access_token_eip712_domain`].
    pub fn signing_hash(domain_chain_id: u64) -> B256 {
        Self::message(domain_chain_id)
            .eip712_signing_hash(&access_token_eip712_domain(domain_chain_id))
    }

    /// Signs an access token for `domain_chain_id` with any synchronous signer, such as a
    /// `PrivateKeySigner`.
    pub fn sign<S: SignerSync>(
        signer: &S,
        domain_chain_id: u64,
    ) -> Result<Self, alloy_signer::Error> {
        let signature = signer.sign_hash_sync(&Self::signing_hash(domain_chain_id))?;
        Ok(Self {
            domain_chain_id,
            signature,
        })
    }

    /// Encodes the token as a header value: `<chainId>.<0x-prefixed 65-byte signature>`.
    pub fn encode(&self) -> String {
        format!(
            "{}.{}",
            self.domain_chain_id,
            hex::encode_prefixed(self.signature.as_bytes()),
        )
    }

    /// Parses a header value produced by [`Self::encode`]. The signature may be given with
    /// or without the `0x` prefix.
    pub fn decode(value: &str) -> Result<Self, DomainAuthError> {
        let mut parts = value.trim().split('.');
        let (Some(chain_id), Some(signature), None) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(DomainAuthError::MalformedToken);
        };

        let domain_chain_id = chain_id
            .parse::<u64>()
            .map_err(|_| DomainAuthError::MalformedToken)?;

        let signature = signature
            .strip_prefix("0x")
            .or_else(|| signature.strip_prefix("0X"))
            .unwrap_or(signature);
        let signature = hex::decode(signature)
            .ok()
            .and_then(|bytes| Signature::from_raw(&bytes).ok())
            .ok_or(DomainAuthError::MalformedToken)?;

        Ok(Self {
            domain_chain_id,
            signature,
        })
    }

    /// Checks the token against the domain route it was presented on and returns the
    /// account that signed it.
    pub fn verify(&self, route_chain_id: u64) -> Result<Address, DomainAuthError> {
        if self.domain_chain_id != route_chain_id {
            return Err(DomainAuthError::ChainIdMismatch {
                token: self.domain_chain_id,
                route: route_chain_id,
            });
        }

        self.signature
            .recover_address_from_prehash(&Self::signing_hash(route_chain_id))
            .map_err(|_| DomainAuthError::InvalidSignature)
    }
}

/// Authenticates a request on the `/domain/{route_chain_id}` route from its
/// [`DOMAIN_AUTH_HEADER`] header and returns the signer.
pub fn authenticate_domain_request(
    req: &HttpRequest,
    route_chain_id: u64,
) -> Result<Address, DomainAuthError> {
    let header = req
        .headers()
        .get(DOMAIN_AUTH_HEADER)
        .ok_or(DomainAuthError::MissingToken)?;
    let value = header
        .to_str()
        .map_err(|_| DomainAuthError::MalformedToken)?;
    SignedDomainAccessToken::decode(value)?.verify(route_chain_id)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::keccak256;
    use alloy_signer_local::PrivateKeySigner;

    use super::*;

    const CHAIN_ID: u64 = (1 << 16) | 1337;

    fn signer() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap()
    }

    fn signed(chain_id: u64) -> SignedDomainAccessToken {
        SignedDomainAccessToken::sign(&signer(), chain_id).unwrap()
    }

    #[test]
    fn verify_recovers_signer() {
        assert_eq!(signed(CHAIN_ID).verify(CHAIN_ID), Ok(signer().address()));
    }

    #[test]
    fn header_round_trips() {
        let token = signed(CHAIN_ID);
        let header = token.encode();

        let prefix = format!("{CHAIN_ID}.0x");
        assert!(header.starts_with(&prefix));
        assert_eq!(header.len(), prefix.len() + 130);

        let decoded = SignedDomainAccessToken::decode(&header).unwrap();
        assert_eq!(decoded, token);
        assert_eq!(decoded.verify(CHAIN_ID), Ok(signer().address()));

        let unprefixed = header.replace(".0x", ".");
        assert_eq!(
            SignedDomainAccessToken::decode(&unprefixed).unwrap(),
            token
        );
    }

    #[test]
    fn rejects_malformed_headers() {
        let sixty_four_zero_bytes = "00".repeat(64);
        for header in [
            "",
            "abc",
            "1",
            "1.0xzz",
            "1.0x00.extra",
            "-1.0x00",
            &format!("{CHAIN_ID}.0x{sixty_four_zero_bytes}"),
        ] {
            assert_eq!(
                SignedDomainAccessToken::decode(header),
                Err(DomainAuthError::MalformedToken),
                "{header:?}"
            );
        }
    }

    #[test]
    fn rejects_unrecoverable_signature() {
        let header = format!("{CHAIN_ID}.0x{}", "00".repeat(65));
        let token = SignedDomainAccessToken::decode(&header).unwrap();
        assert_eq!(
            token.verify(CHAIN_ID),
            Err(DomainAuthError::InvalidSignature)
        );
    }

    #[test]
    fn rejects_chain_id_mismatch() {
        let token = signed(CHAIN_ID);
        assert_eq!(
            token.verify(CHAIN_ID + 1),
            Err(DomainAuthError::ChainIdMismatch {
                token: CHAIN_ID,
                route: CHAIN_ID + 1,
            })
        );
    }

    #[test]
    fn signature_is_bound_to_domain_chain_id() {
        // Relabelling a token for another domain must not authenticate the signer there.
        let mut token = signed(CHAIN_ID);
        token.domain_chain_id = CHAIN_ID + 1;
        assert_ne!(token.verify(CHAIN_ID + 1), Ok(signer().address()));
    }

    #[test]
    fn signing_hash_is_eip712_typed_data_hash() {
        assert_eq!(
            DomainAccessToken::eip712_encode_type(),
            "DomainAccessToken(uint64 chainId)"
        );

        let message = SignedDomainAccessToken::message(CHAIN_ID);
        assert_eq!(message, DomainAccessToken { chainId: CHAIN_ID });

        let mut preimage = vec![0x19, 0x01];
        preimage.extend_from_slice(access_token_eip712_domain(CHAIN_ID).separator().as_slice());
        preimage.extend_from_slice(message.eip712_hash_struct().as_slice());
        assert_eq!(
            SignedDomainAccessToken::signing_hash(CHAIN_ID),
            keccak256(preimage)
        );
    }
}
