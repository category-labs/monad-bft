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

use std::time::Duration;

use crate::{
    kvstore::{KVStoreType, Metrics},
    metrics::MetricNames,
};

pub trait MetricsResultExt {
    fn write_put_metrics(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self;
    fn write_get_metrics(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self;
    fn write_get_metrics_on_err(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self;
    fn write_put_metrics_on_err(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self;
}

impl<T, E> MetricsResultExt for std::result::Result<T, E> {
    fn write_put_metrics(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self {
        kvstore_put_metrics(duration, self.is_ok(), kvstore_type, metrics);
        self
    }

    fn write_get_metrics(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self {
        kvstore_get_metrics(duration, self.is_ok(), kvstore_type, metrics);
        self
    }

    fn write_get_metrics_on_err(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self {
        if self.is_err() {
            kvstore_get_metrics(duration, false, kvstore_type, metrics);
        }
        self
    }

    fn write_put_metrics_on_err(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self {
        if self.is_err() {
            kvstore_put_metrics(duration, false, kvstore_type, metrics);
        }
        self
    }
}

pub(super) fn kvstore_put_metrics(
    duration: Duration,
    is_success: bool,
    kvstore_type: KVStoreType,
    metrics: &Metrics,
) {
    let attrs = &[opentelemetry::KeyValue::new(
        "kvstore_type",
        kvstore_type.as_str(),
    )];
    metrics.histogram_with_attrs(
        MetricNames::KV_STORE_PUT_DURATION_MS,
        duration.as_millis() as f64,
        attrs,
    );
    if is_success {
        metrics.counter_with_attrs(MetricNames::KV_STORE_PUT_SUCCESS, 1, attrs);
    } else {
        metrics.counter_with_attrs(MetricNames::KV_STORE_PUT_FAILURE, 1, attrs);
    }

    // Legacy metrics for backwards compatibility
    match kvstore_type {
        KVStoreType::AwsS3 => {
            if is_success {
                metrics.inc_counter(MetricNames::AWS_S3_WRITES);
            } else {
                metrics.inc_counter(MetricNames::AWS_S3_ERRORS);
            }
        }
        KVStoreType::AwsDynamoDB => {
            if is_success {
                metrics.inc_counter(MetricNames::AWS_DYNAMODB_WRITES);
            } else {
                metrics.inc_counter(MetricNames::AWS_DYNAMODB_ERRORS);
            }
        }
        _ => {}
    }
}

pub(super) fn kvstore_get_metrics(
    duration: Duration,
    is_success: bool,
    kvstore_type: KVStoreType,
    metrics: &Metrics,
) {
    let attrs = &[opentelemetry::KeyValue::new(
        "kvstore_type",
        kvstore_type.as_str(),
    )];
    metrics.histogram_with_attrs(
        MetricNames::KV_STORE_GET_DURATION_MS,
        duration.as_millis() as f64,
        attrs,
    );
    if is_success {
        metrics.counter_with_attrs(MetricNames::KV_STORE_GET_SUCCESS, 1, attrs);
    } else {
        metrics.counter_with_attrs(MetricNames::KV_STORE_GET_FAILURE, 1, attrs);
    }

    // Legacy metrics for backwards compatibility
    match kvstore_type {
        KVStoreType::AwsS3 => {
            if is_success {
                metrics.inc_counter(MetricNames::AWS_S3_READS);
            } else {
                metrics.inc_counter(MetricNames::AWS_S3_ERRORS);
            }
        }
        KVStoreType::AwsDynamoDB => {
            if is_success {
                metrics.inc_counter(MetricNames::AWS_DYNAMODB_READS);
            } else {
                metrics.inc_counter(MetricNames::AWS_DYNAMODB_ERRORS);
            }
        }
        _ => {}
    }
}
