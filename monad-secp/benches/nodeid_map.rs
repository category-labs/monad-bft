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

use std::{
    collections::{BTreeMap, HashMap},
    hint::black_box,
};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use monad_secp::{PubKey, SecpSignature};
use monad_testutil::signing::create_keys;
use monad_types::NodeId;
use rand::{seq::SliceRandom, thread_rng};

type SignatureType = SecpSignature;
type Key = NodeId<PubKey>;

const SIZES: &[usize] = &[200, 250, 300];

fn make_keys(n: usize) -> Vec<Key> {
    create_keys::<SignatureType>(n as u32)
        .iter()
        .map(|kp| NodeId::new(kp.pubkey()))
        .collect()
}

fn make_miss_keys(n: usize, offset: u32) -> Vec<Key> {
    (0..n)
        .map(|i| {
            let kp = monad_testutil::signing::get_key::<SignatureType>(offset as u64 + i as u64);
            NodeId::new(kp.pubkey())
        })
        .collect()
}

fn make_sorted_vec(keys: &[Key]) -> Vec<(Key, u64)> {
    let mut v: Vec<(Key, u64)> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (*k, i as u64))
        .collect();
    v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    v
}

fn bench_lookup_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_hit");
    for &n in SIZES {
        let keys = make_keys(n);
        let hashmap: HashMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();
        let btreemap: BTreeMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();
        let sortedvec = make_sorted_vec(&keys);

        // Shuffle a lookup sequence so we don't bias toward insertion order.
        let mut shuffled = keys.clone();
        shuffled.shuffle(&mut thread_rng());

        group.bench_function(format!("hashmap/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &shuffled[idx % shuffled.len()];
                idx = idx.wrapping_add(1);
                black_box(hashmap.get(k))
            })
        });

        group.bench_function(format!("btreemap/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &shuffled[idx % shuffled.len()];
                idx = idx.wrapping_add(1);
                black_box(btreemap.get(k))
            })
        });

        group.bench_function(format!("sortedvec_binsearch/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &shuffled[idx % shuffled.len()];
                idx = idx.wrapping_add(1);
                let r = sortedvec
                    .binary_search_by(|(kk, _)| kk.cmp(k))
                    .map(|i| &sortedvec[i].1)
                    .ok();
                black_box(r)
            })
        });

        // sortedvec_linear: same sorted Vec storage as sortedvec_binsearch,
        // but lookup is a linear scan with Eq. This shines because PubKey's
        // Eq runs eq_fast_unstable on the internal 64-byte buffer, whereas
        // Ord (used by binary_search) goes through the secp256k1_ec_pubkey_cmp
        // FFI call.
        group.bench_function(format!("sortedvec_linear/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &shuffled[idx % shuffled.len()];
                idx = idx.wrapping_add(1);
                let r = sortedvec.iter().find(|(kk, _)| kk == k).map(|(_, v)| v);
                black_box(r)
            })
        });
    }
    group.finish();
}

fn bench_lookup_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_miss");
    for &n in SIZES {
        let keys = make_keys(n);
        // Disjoint key set (offset by 1_000_000 from validator seeds).
        let misses = make_miss_keys(n, 1_000_000);

        let hashmap: HashMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();
        let btreemap: BTreeMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();
        let sortedvec = make_sorted_vec(&keys);

        group.bench_function(format!("hashmap/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &misses[idx % misses.len()];
                idx = idx.wrapping_add(1);
                black_box(hashmap.get(k))
            })
        });

        group.bench_function(format!("btreemap/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &misses[idx % misses.len()];
                idx = idx.wrapping_add(1);
                black_box(btreemap.get(k))
            })
        });

        group.bench_function(format!("sortedvec_binsearch/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &misses[idx % misses.len()];
                idx = idx.wrapping_add(1);
                let r = sortedvec
                    .binary_search_by(|(kk, _)| kk.cmp(k))
                    .map(|i| &sortedvec[i].1)
                    .ok();
                black_box(r)
            })
        });

        group.bench_function(format!("sortedvec_linear/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &misses[idx % misses.len()];
                idx = idx.wrapping_add(1);
                let r = sortedvec.iter().find(|(kk, _)| kk == k).map(|(_, v)| v);
                black_box(r)
            })
        });
    }
    group.finish();
}

fn bench_contains_random(c: &mut Criterion) {
    // 50/50 hit/miss mix to approximate a "is this peer in my validator set" probe
    // where some lookups land outside the set.
    let mut group = c.benchmark_group("contains_50_50");
    for &n in SIZES {
        let keys = make_keys(n);
        let misses = make_miss_keys(n, 2_000_000);
        let hashmap: HashMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();
        let btreemap: BTreeMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();
        let sortedvec = make_sorted_vec(&keys);

        let mut probe: Vec<Key> = keys.iter().chain(misses.iter()).copied().collect();
        probe.shuffle(&mut thread_rng());

        group.bench_function(format!("hashmap/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &probe[idx % probe.len()];
                idx = idx.wrapping_add(1);
                black_box(hashmap.contains_key(k))
            })
        });
        group.bench_function(format!("btreemap/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &probe[idx % probe.len()];
                idx = idx.wrapping_add(1);
                black_box(btreemap.contains_key(k))
            })
        });

        group.bench_function(format!("sortedvec_binsearch/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &probe[idx % probe.len()];
                idx = idx.wrapping_add(1);
                black_box(sortedvec.binary_search_by(|(kk, _)| kk.cmp(k)).is_ok())
            })
        });

        group.bench_function(format!("sortedvec_linear/{}", n), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let k = &probe[idx % probe.len()];
                idx = idx.wrapping_add(1);
                black_box(sortedvec.iter().any(|(kk, _)| kk == k))
            })
        });
    }
    group.finish();
}

fn bench_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_sum");
    for &n in SIZES {
        let keys = make_keys(n);
        let hashmap: HashMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();
        let btreemap: BTreeMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();

        group.bench_function(format!("hashmap/{}", n), |b| {
            b.iter(|| {
                let mut s = 0u64;
                for (_, v) in hashmap.iter() {
                    s = s.wrapping_add(*v);
                }
                black_box(s)
            })
        });

        group.bench_function(format!("btreemap/{}", n), |b| {
            b.iter(|| {
                let mut s = 0u64;
                for (_, v) in btreemap.iter() {
                    s = s.wrapping_add(*v);
                }
                black_box(s)
            })
        });

        let sortedvec = make_sorted_vec(&keys);
        group.bench_function(format!("sortedvec_binsearch/{}", n), |b| {
            b.iter(|| {
                let mut s = 0u64;
                for (_, v) in sortedvec.iter() {
                    s = s.wrapping_add(*v);
                }
                black_box(s)
            })
        });
    }
    group.finish();
}

fn bench_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("clone");
    for &n in SIZES {
        let keys = make_keys(n);
        let hashmap: HashMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();
        let btreemap: BTreeMap<Key, u64> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();

        group.bench_function(format!("hashmap/{}", n), |b| {
            b.iter(|| black_box(hashmap.clone()))
        });
        group.bench_function(format!("btreemap/{}", n), |b| {
            b.iter(|| black_box(btreemap.clone()))
        });

        let sortedvec = make_sorted_vec(&keys);
        group.bench_function(format!("sortedvec_binsearch/{}", n), |b| {
            b.iter(|| black_box(sortedvec.clone()))
        });
    }
    group.finish();
}

fn bench_build_from_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_from_iter");
    for &n in SIZES {
        let keys = make_keys(n);
        let pairs: Vec<(Key, u64)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, i as u64))
            .collect();

        group.bench_function(format!("hashmap/{}", n), |b| {
            b.iter_batched(
                || pairs.clone(),
                |p| {
                    let m: HashMap<Key, u64> = p.into_iter().collect();
                    black_box(m)
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function(format!("btreemap/{}", n), |b| {
            b.iter_batched(
                || pairs.clone(),
                |p| {
                    let m: BTreeMap<Key, u64> = p.into_iter().collect();
                    black_box(m)
                },
                BatchSize::SmallInput,
            )
        });

        // Sorted Vec build: collect + sort. Both sortedvec_binsearch and
        // sortedvec_linear share this storage, so it has the same build
        // cost; no separate arms.
        group.bench_function(format!("sortedvec_binsearch/{}", n), |b| {
            b.iter_batched(
                || pairs.clone(),
                |mut p| {
                    p.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                    black_box(p)
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_build_from_iter,
    bench_lookup_hit,
    bench_lookup_miss,
    bench_contains_random,
    bench_iter,
    bench_clone,
);
criterion_main!(benches);
