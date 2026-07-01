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

//! AWS SDK plumbing shared by the dynamo and s3 backends: static credentials
//! and the credential/region config prologue.

use std::time::Duration;

use aws_config::{timeout::TimeoutConfig, BehaviorVersion};
// The service crates re-export the same `aws-credential-types`/`aws-types`
// items; pick whichever enabled backend provides them.
#[cfg(feature = "dynamo")]
use aws_sdk_dynamodb::config::{Credentials, Region};
#[cfg(all(feature = "s3", not(feature = "dynamo")))]
use aws_sdk_s3::config::{Credentials, Region};

#[derive(Clone)]
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Caps one HTTP attempt, request sent to response read — the full body for
/// buffered responses (every dynamo op); a streaming S3 GET body is collected
/// outside the attempt and is bounded by the SDK's default stalled-stream
/// protection instead. Without it the
/// SDK only bounds connection *establishment* (~3.1s default), so a request
/// accepted onto a connection that then goes silent (peer freeze, middlebox
/// dropping the flow without RST) would pend forever: the store-layer retry /
/// `ClientRing` failover machinery only engages on `Err`. Must comfortably
/// exceed worst-case legitimate latency for one `BatchWriteItem` / ranged GET
/// on a loaded backend.
const OPERATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// Caps one SDK operation including the SDK's own internal retries; backstop
/// above [`OPERATION_ATTEMPT_TIMEOUT`].
const OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Resolves the AWS credential/region chain once (async: it performs I/O).
/// Per-endpoint overrides belong on the derived service clients, not here.
/// Every derived client (dynamo meta/blob, s3) inherits the attempt/operation
/// timeouts, so a dead connection surfaces as an `Err` the callers' retry and
/// endpoint-failover paths can act on instead of hanging.
pub(crate) async fn load_sdk_config(
    region: Option<String>,
    profile: Option<String>,
    credentials: Option<StaticCredentials>,
    provider_name: &'static str,
) -> aws_config::SdkConfig {
    // The loader merges this with its defaults, so the default connect
    // timeout (3.1s) stays in place alongside the explicit timeouts.
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).timeout_config(
        TimeoutConfig::builder()
            .operation_attempt_timeout(OPERATION_ATTEMPT_TIMEOUT)
            .operation_timeout(OPERATION_TIMEOUT)
            .build(),
    );
    if let Some(region) = region {
        loader = loader.region(Region::new(region));
    }
    if let Some(profile) = profile {
        loader = loader.profile_name(profile);
    }
    if let Some(creds) = credentials {
        loader = loader.credentials_provider(Credentials::new(
            creds.access_key_id,
            creds.secret_access_key,
            creds.session_token,
            None,
            provider_name,
        ));
    }
    loader.load().await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A request stuck on a silently-dead connection only surfaces as an `Err`
    /// (engaging the stores' retry/`ClientRing`-failover machinery) if the SDK
    /// enforces operation timeouts; the bare defaults set a connect timeout
    /// only. Assert the shared config every derived client builds from
    /// carries them.
    #[tokio::test]
    async fn sdk_config_sets_operation_timeouts() {
        // Static credentials short-circuit the ambient credential chain,
        // keeping the test hermetic (no profile/IMDS lookups).
        let credentials = StaticCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
        };
        let config = load_sdk_config(
            Some("us-east-1".to_string()),
            None,
            Some(credentials),
            "test",
        )
        .await;
        let timeouts = config.timeout_config().expect("timeout config present");
        assert_eq!(
            timeouts.operation_attempt_timeout(),
            Some(OPERATION_ATTEMPT_TIMEOUT)
        );
        assert_eq!(timeouts.operation_timeout(), Some(OPERATION_TIMEOUT));
        // The loader merge keeps the SDK's default connect timeout alongside.
        assert!(timeouts.connect_timeout().is_some());
    }
}
