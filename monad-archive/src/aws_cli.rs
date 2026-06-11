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
//! - Credentials: named `--profile`, then the AWS default credential chain
//!   (environment variables, shared config, IAM roles). Static credentials are
//!   deliberately not accepted as configuration so they can never end up in
//!   logs or serialized state.

use aws_config::{
    meta::{credentials::CredentialsProviderChain, region::RegionProviderChain},
    profile::{ProfileFileCredentialsProvider, ProfileFileRegionProvider},
    retry::RetryConfig,
    timeout::TimeoutConfig,
    BehaviorVersion, Region, SdkConfig,
};
use aws_sdk_s3::config::SharedCredentialsProvider;

use crate::{cli::AwsCliArgs, prelude::*};

impl AwsCliArgs {
    pub(crate) async fn config(&self) -> Result<SdkConfig> {
        let mut config = aws_config::defaults(BehaviorVersion::latest());

        if let Some(profile) = &self.profile {
            config = config.profile_name(profile);
        }

        // Keep the full precedence order explicit here so source/sink AWS
        // behavior stays auditable as new flags are added.
        config = config.region(self.region_provider());

        if let Some(credentials_provider) = self.credentials_provider().await {
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

    async fn credentials_provider(&self) -> Option<SharedCredentialsProvider> {
        if let Some(profile) = &self.profile {
            let provider = CredentialsProviderChain::first_try(
                "Profile",
                ProfileFileCredentialsProvider::builder()
                    .profile_name(profile)
                    .build(),
            )
            .or_default_provider()
            .await;
            return Some(SharedCredentialsProvider::new(provider));
        }

        None
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
    async fn aws_environment_credentials_used_without_profile() {
        let (_dir, config_path, credentials_path) = write_profile_files("", "");

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
        assert_eq!(credentials.access_key_id(), "ENV_KEY");
        assert_eq!(credentials.secret_access_key(), "ENV_SECRET");
    }
}
