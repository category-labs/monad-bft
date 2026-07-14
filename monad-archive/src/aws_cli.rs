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

//! AWS CLI argument resolution and SDK config construction.
//!
//! Intended precedence:
//! - Region: explicit `--region`, then named `--profile`, then the AWS default
//!   region chain, then static `us-east-2`.
//! - Credentials: explicit `--access-key-id` and `--secret-access-key`, then
//!   named `--profile`, then the AWS default credential chain.
//! - Partial static credentials are rejected instead of silently falling
//!   through to ambient configuration.

use aws_config::{
    meta::{credentials::CredentialsProviderChain, region::RegionProviderChain},
    profile::{ProfileFileCredentialsProvider, ProfileFileRegionProvider},
    retry::RetryConfig,
    timeout::TimeoutConfig,
    BehaviorVersion, Region, SdkConfig,
};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use eyre::bail;

use crate::{cli::AwsCliArgs, prelude::*};

impl AwsCliArgs {
    pub(crate) async fn config(&self) -> Result<SdkConfig> {
        self.validate_credentials_config()?;

        let mut config = aws_config::defaults(BehaviorVersion::latest());

        if let Some(profile) = &self.profile {
            config = config.profile_name(profile);
        }

        // Keep the full precedence order explicit here so source/sink AWS
        // behavior stays auditable as new flags are added.
        config = config.region(self.region_provider());

        if let Some(credentials_provider) = self.credentials_provider().await? {
            config = config.credentials_provider(credentials_provider);
        }

        let mut config = config
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(self.operation_timeout_secs))
                    .operation_attempt_timeout(Duration::from_secs(
                        self.operation_attempt_timeout_secs,
                    ))
                    .read_timeout(Duration::from_secs(self.read_timeout_secs))
                    .build(),
            )
            .retry_config(RetryConfig::standard().with_max_attempts(3));

        if let Some(endpoint) = &self.endpoint {
            config = config.endpoint_url(endpoint);
        }

        let sdk_config = config.load().await;
        let region = sdk_config
            .region()
            .map(|region| region.as_ref())
            .unwrap_or("unknown");
        info!("Bucket {} running in region: {}", self.bucket, region);

        Ok(sdk_config)
    }

    fn validate_credentials_config(&self) -> Result<()> {
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(_), None) => bail!(
                "aws config for bucket '{}' must set --secret-access-key when --access-key-id is provided",
                self.bucket
            ),
            (None, Some(_)) => bail!(
                "aws config for bucket '{}' must set --access-key-id when --secret-access-key is provided",
                self.bucket
            ),
            _ => Ok(()),
        }
    }

    async fn credentials_provider(&self) -> Result<Option<SharedCredentialsProvider>> {
        if let Some(provider) = self.static_credentials_provider()? {
            return Ok(Some(provider));
        }

        if let Some(profile) = &self.profile {
            let provider = CredentialsProviderChain::first_try(
                "Profile",
                ProfileFileCredentialsProvider::builder()
                    .profile_name(profile)
                    .build(),
            )
            .or_default_provider()
            .await;
            return Ok(Some(SharedCredentialsProvider::new(provider)));
        }

        Ok(None)
    }

    fn static_credentials_provider(&self) -> Result<Option<SharedCredentialsProvider>> {
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => {
                Ok(Some(SharedCredentialsProvider::new(Credentials::new(
                    access_key_id,
                    secret_access_key,
                    None,
                    None,
                    "minio",
                ))))
            }
            (None, None) => Ok(None),
            _ => {
                self.validate_credentials_config()?;
                unreachable!("invalid partial credentials should be rejected")
            }
        }
    }

    fn region_provider(&self) -> RegionProviderChain {
        if let Some(region) = &self.region {
            return RegionProviderChain::first_try(Region::new(region.clone()));
        }

        if let Some(profile) = &self.profile {
            return RegionProviderChain::first_try(
                ProfileFileRegionProvider::builder()
                    .profile_name(profile)
                    .build(),
            )
            .or_default_provider()
            .or_else(Region::new("us-east-2"));
        }

        RegionProviderChain::default_provider().or_else(Region::new("us-east-2"))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        sync::{Mutex, MutexGuard, OnceLock},
    };

    use aws_sdk_s3::config::ProvideCredentials;
    use tempfile::tempdir;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct EnvGuard {
        _guard: MutexGuard<'static, ()>,
        originals: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, &str)]) -> Self {
            let guard = ENV_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|err| err.into_inner());

            let originals = vars
                .iter()
                .map(|(key, _)| (*key, env::var(key).ok()))
                .collect::<Vec<_>>();

            for (key, value) in vars {
                // Tests serialize env mutations through ENV_LOCK.
                unsafe { env::set_var(key, value) };
            }

            Self {
                _guard: guard,
                originals,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.originals.drain(..) {
                match value {
                    Some(value) => {
                        // Tests serialize env mutations through ENV_LOCK.
                        unsafe { env::set_var(key, value) };
                    }
                    None => {
                        // Tests serialize env mutations through ENV_LOCK.
                        unsafe { env::remove_var(key) };
                    }
                }
            }
        }
    }

    fn write_profile_files(
        config_contents: &str,
        credentials_contents: &str,
    ) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config");
        let credentials_path = dir.path().join("credentials");
        fs::write(&config_path, config_contents).unwrap();
        fs::write(&credentials_path, credentials_contents).unwrap();
        (dir, config_path, credentials_path)
    }

    #[tokio::test]
    async fn aws_profile_credentials_ignore_environment_credentials() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\nregion = us-west-2\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_ACCESS_KEY_ID", "ENV_KEY"),
            ("AWS_SECRET_ACCESS_KEY", "ENV_SECRET"),
            ("AWS_REGION", "eu-central-1"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        let credentials = config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();
        assert_eq!(credentials.access_key_id(), "PROFILE_KEY");
        assert_eq!(credentials.secret_access_key(), "PROFILE_SECRET");
    }

    #[tokio::test]
    async fn aws_profile_region_ignores_environment_region() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\nregion = us-west-2\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_REGION", "eu-central-1"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        assert_eq!(
            config.region().map(|region| region.as_ref()),
            Some("us-west-2")
        );
    }

    #[tokio::test]
    async fn aws_profile_without_region_falls_back_to_default_chain_region() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_REGION", "eu-central-1"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        assert_eq!(
            config.region().map(|region| region.as_ref()),
            Some("eu-central-1")
        );
    }

    #[tokio::test]
    async fn aws_explicit_region_beats_profile_and_environment_region() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\nregion = us-west-2\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_REGION", "eu-central-1"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            region: Some("ap-southeast-1".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        assert_eq!(
            config.region().map(|region| region.as_ref()),
            Some("ap-southeast-1")
        );
    }

    #[tokio::test]
    async fn aws_explicit_static_credentials_beat_profile_credentials() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\nregion = us-west-2\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_ACCESS_KEY_ID", "ENV_KEY"),
            ("AWS_SECRET_ACCESS_KEY", "ENV_SECRET"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            access_key_id: Some("STATIC_KEY".to_string()),
            secret_access_key: Some("STATIC_SECRET".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        let credentials = config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();
        assert_eq!(credentials.access_key_id(), "STATIC_KEY");
        assert_eq!(credentials.secret_access_key(), "STATIC_SECRET");
    }

    #[tokio::test]
    async fn aws_partial_static_credentials_are_rejected() {
        let err = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            access_key_id: Some("STATIC_KEY".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("--secret-access-key"));
    }
}
