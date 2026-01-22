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

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup,
    Criterion, Throughput,
};
use monad_rpc::{
    eth_json_types::MonadNoParams,
    handlers::eth::call::MonadEthCallParams,
    jsonrpc::{JsonRpcError, JsonRpcResultExt, Request, RequestWrapper, ResponseWrapper},
};
use serde::de::DeserializeOwned;
use serde_json::Value;

fn deserialize<T>(body: &Bytes) -> Result<ResponseWrapper<()>, JsonRpcError>
where
    T: DeserializeOwned,
{
    let request = RequestWrapper::from_body_bytes(body).unwrap();

    match request {
        RequestWrapper::Single(json_request) => {
            let request =
                Request::from_raw_value(json_request).map_err(|_| JsonRpcError::parse_error())?;

            let id = request.id.clone();

            let parsed_params: T = serde_json::from_str(request.params.get()).invalid_params()?;

            black_box(parsed_params);
            black_box(id);
            black_box(request);
            black_box(json_request);

            Ok(ResponseWrapper::Single(()))
        }
        RequestWrapper::Batch(json_batch_request) => Ok(ResponseWrapper::Batch(
            json_batch_request
                .into_iter()
                .map(|json_request| {
                    let request = Request::from_raw_value(json_request).unwrap();

                    black_box(request);
                })
                .collect(),
        )),
    }
}

fn bench_deserialize<T, M>(
    g: &mut BenchmarkGroup<'_, M>,
    name: &'static str,
    body: &Bytes,
    expected: Result<ResponseWrapper<()>, JsonRpcError>,
) where
    T: DeserializeOwned,
    M: Measurement,
{
    g.throughput(Throughput::Bytes(body.len() as u64));
    g.bench_function(name, |b| {
        b.iter(|| {
            let result = black_box(deserialize::<T>(black_box(body)));

            assert_eq!(result, expected);
        });
    });
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("deserialize");

    g.sample_size(1_000);
    g.nresamples(1_000_000);

    let eth_call_params: Value = serde_json::json!([
        {
            "data": "0x82ad56cb000000000000000000000000",
            "to": "0xb092ef5Eba57357112E8BbD5be2b6CfE984D6838"
        },
        "latest"
    ]);

    let eth_call_request = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": eth_call_params,
        "id": 0
    });

    bench_deserialize::<MonadEthCallParams, _>(
        &mut g,
        "eth_call",
        &Bytes::from_owner(serde_json::to_string(&eth_call_request).unwrap()),
        Ok(ResponseWrapper::Single(())),
    );

    bench_deserialize::<MonadEthCallParams, _>(
        &mut g,
        "eth_call-batch",
        &Bytes::from_owner(
            serde_json::to_string(&Value::Array(
                (0..8).map(|_| eth_call_request.clone()).collect(),
            ))
            .unwrap(),
        ),
        Ok(ResponseWrapper::Batch((0..8).map(|_| ()).collect())),
    );

    const MAX_REQUEST_SIZE: usize = 2_000_000;

    fn generate_attack_recursive(augment: fn(&Value) -> Value) -> Value {
        let mut attack = Value::Null;

        loop {
            let new_attack = augment(&attack);

            if serde_json::to_string(&new_attack).unwrap().len() > MAX_REQUEST_SIZE {
                return attack;
            }

            attack = new_attack;
        }
    }

    let attack_large_array =
        generate_attack_recursive(|value| Value::Array((0..2).map(|_| value.clone()).collect()));
    assert!(attack_large_array.is_array());

    let attack_large_dict = generate_attack_recursive(|value| {
        Value::Object((0..2).map(|i| (format!("{i:x}"), value.clone())).collect())
    });
    assert!(attack_large_dict.is_object());

    bench_deserialize::<MonadNoParams, _>(
        &mut g,
        "attack_large_id_array",
        &Bytes::from_owner(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "some_method",
                "params": [],
                "id": attack_large_array
            }))
            .unwrap(),
        ),
        Err(JsonRpcError::parse_error()),
    );

    bench_deserialize::<MonadNoParams, _>(
        &mut g,
        "attack_large_id_dict",
        &Bytes::from_owner(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "some_method",
                "params": [],
                "id": attack_large_dict
            }))
            .unwrap(),
        ),
        Err(JsonRpcError::parse_error()),
    );

    bench_deserialize::<MonadNoParams, _>(
        &mut g,
        "attack_large_payload_array_without_params",
        &Bytes::from_owner(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "some_method_that_doesnt_parse_params",
                "params": attack_large_array,
                "id": 0
            }))
            .unwrap(),
        ),
        Err(JsonRpcError::invalid_params()),
    );

    bench_deserialize::<MonadNoParams, _>(
        &mut g,
        "attack_large_payload_dict_without_params",
        &Bytes::from_owner(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "some_method_that_doesnt_parse_params",
                "params": attack_large_dict,
                "id": 0
            }))
            .unwrap(),
        ),
        Err(JsonRpcError::invalid_params()),
    );

    bench_deserialize::<MonadNoParams, _>(
        &mut g,
        "attack_large_payload_array_with_params",
        &Bytes::from_owner(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "some_method_that_parses_params",
                "params": attack_large_array,
                "id": 0
            }))
            .unwrap(),
        ),
        Err(JsonRpcError::invalid_params()),
    );
    bench_deserialize::<MonadNoParams, _>(
        &mut g,
        "attack_large_payload_dict_with_params",
        &Bytes::from_owner(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "some_method_that_parses_params",
                "params": attack_large_dict,
                "id": 0
            }))
            .unwrap(),
        ),
        Err(JsonRpcError::invalid_params()),
    );
}

criterion_group!(benches, bench);
criterion_main!(benches);
