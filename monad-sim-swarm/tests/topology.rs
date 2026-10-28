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

//! Predefined region tables and the seeded node placer.

use std::time::Duration;

use monad_sim::time::millis;
use monad_sim_swarm::{Placement, Placer, Provenance, RegionTable, Stake};
use rand::SeedableRng;
use rand_chacha::ChaChaRng;

fn presets() -> Vec<RegionTable> {
    vec![
        RegionTable::public_internet(),
        RegionTable::single_region(millis(1)),
        RegionTable::two_clusters(millis(2), millis(80)),
        RegionTable::ring(6, millis(1), millis(10)),
        RegionTable::star(4, millis(1), millis(25)),
    ]
}

fn place(placer: &Placer, n: u32, seed: u64) -> Placement<u32> {
    placer.place(0..n, &mut ChaChaRng::seed_from_u64(seed))
}

/// Every latency of `placement`'s matrix, row by row.
fn latencies(placement: &Placement<u32>) -> Vec<Duration> {
    let matrix = placement.matrix();
    let nodes = placement.nodes();
    nodes
        .iter()
        .flat_map(|a| nodes.iter().map(|b| matrix.latency(&a.addr, &b.addr)))
        .collect()
}

#[test]
fn presets_are_square_positive_and_sourced() {
    for table in presets() {
        let name = &table.provenance().name;
        assert!(!table.provenance().source.is_empty(), "{name}: no source");
        assert_eq!(table.provenance().date.len(), 10, "{name}: date");
        assert_eq!(table.weights().len(), table.len(), "{name}");
        assert!(table.min_latency() > Duration::ZERO, "{name}");
        for i in 0..table.len() {
            assert!(table.one_way(i, i) > Duration::ZERO, "{name}: diagonal");
        }
    }
}

#[test]
fn public_internet_is_symmetric_and_halves_rtt() {
    let table = RegionTable::public_internet();
    assert_eq!(table.len(), 10);
    for i in 0..table.len() {
        for j in 0..table.len() {
            assert_eq!(table.one_way(i, j), table.one_way(j, i));
        }
    }
    let at = |a, b| table.one_way(table.region(a).unwrap(), table.region(b).unwrap());
    assert_eq!(at("FRA", "AMS"), millis(7));
    assert_eq!(at("NYC", "LON"), Duration::from_micros(35_500));
    assert_eq!(at("SAO", "SGP"), Duration::from_micros(187_500));
    assert_eq!(at("TYO", "TYO"), millis(1));
    assert_eq!(table.min_latency(), millis(1));
}

#[test]
fn synthetic_shapes() {
    let clusters = RegionTable::two_clusters(millis(2), millis(80));
    assert_eq!(clusters.one_way(0, 0), millis(2));
    assert_eq!(clusters.one_way(1, 0), millis(80));

    let ring = RegionTable::ring(6, millis(1), millis(10));
    assert_eq!(ring.one_way(0, 1), millis(10));
    assert_eq!(ring.one_way(0, 3), millis(30));
    assert_eq!(ring.one_way(0, 5), millis(10), "the shorter way round");
    assert_eq!(ring.one_way(4, 4), millis(1));

    let star = RegionTable::star(4, millis(1), millis(25));
    assert_eq!(star.len(), 5);
    assert_eq!(star.region("hub"), Some(0));
    assert_eq!(star.one_way(0, 3), millis(25));
    assert_eq!(star.one_way(2, 3), millis(50));
    assert_eq!(star.one_way(3, 3), millis(1));
}

#[test]
#[should_panic(expected = "must be positive")]
fn zero_diagonal_is_rejected() {
    RegionTable::from_rtt(
        Provenance::new("measured", "test", "2026-10-28"),
        ["x", "y"],
        vec![
            vec![Duration::ZERO, millis(10)],
            vec![millis(10), Duration::ZERO],
        ],
    );
}

/// Region counts follow the weights exactly when they divide evenly, and by
/// largest remainder otherwise.
#[test]
fn placement_follows_region_weights() {
    let table = RegionTable::public_internet();
    let placer = Placer::new(&table);
    assert_eq!(
        place(&placer, 100, 1).region_counts(),
        vec![20, 12, 10, 15, 8, 8, 10, 9, 4, 4]
    );
    // quotas 2.0, 1.2, 1.0, 1.5, 0.8, 0.8, 1.0, 0.9, 0.4, 0.4
    assert_eq!(
        place(&placer, 10, 1).region_counts(),
        vec![2, 1, 1, 2, 1, 1, 1, 1, 0, 0]
    );

    let weighted = RegionTable::two_clusters(millis(1), millis(50)).with_weights([3.0, 1.0]);
    assert_eq!(
        place(&Placer::new(&weighted), 8, 7).region_counts(),
        vec![6, 2]
    );
}

#[test]
fn placement_is_deterministic_per_seed() {
    let table = RegionTable::public_internet();
    let placer = Placer::new(&table)
        .last_mile(Duration::from_micros(300)..=millis(4))
        .stake(Stake::LogNormal { sigma: 1.0 });
    let a = place(&placer, 50, 42);
    let b = place(&placer, 50, 42);
    assert_eq!(a.nodes(), b.nodes());
    assert_eq!(latencies(&a), latencies(&b));

    let c = place(&placer, 50, 43);
    assert_ne!(a.nodes(), c.nodes());
}

#[test]
fn region_level_matrix_without_last_mile() {
    let table = RegionTable::public_internet();
    let placement = place(&Placer::new(&table), 30, 3);
    let matrix = placement.matrix();
    for a in placement.nodes() {
        assert_eq!(a.last_mile, Duration::ZERO);
        assert!((a.stake - 1.0 / 30.0).abs() < 1e-12);
        for b in placement.nodes() {
            assert_eq!(
                matrix.latency(&a.addr, &b.addr),
                table.one_way(a.region, b.region)
            );
        }
    }
}

#[test]
fn last_mile_is_added_at_both_ends() {
    let table = RegionTable::two_clusters(millis(2), millis(80));
    let range = millis(1)..=millis(3);
    let placement = place(&Placer::new(&table).last_mile(range.clone()), 10, 9);
    let matrix = placement.matrix();
    for a in placement.nodes() {
        assert!(range.contains(&a.last_mile));
        for b in placement.nodes() {
            assert_eq!(
                matrix.latency(&a.addr, &b.addr),
                table.one_way(a.region, b.region) + a.last_mile + b.last_mile
            );
        }
    }
    assert!(latencies(&placement).iter().all(|d| *d > Duration::ZERO));
}

#[test]
fn stakes_are_normalized() {
    let table = RegionTable::single_region(millis(1));
    let placement = place(
        &Placer::new(&table).stake(Stake::LogNormal { sigma: 1.0 }),
        40,
        5,
    );
    let total: f64 = placement.nodes().iter().map(|n| n.stake).sum();
    assert!((total - 1.0).abs() < 1e-9);
    let first = placement.nodes()[0].stake;
    assert!(placement.nodes().iter().any(|n| n.stake != first));
}

#[test]
fn every_preset_places_with_a_positive_diagonal() {
    for table in presets() {
        let placement = place(
            &Placer::new(&table).last_mile(Duration::ZERO..=millis(1)),
            25,
            11,
        );
        assert_eq!(placement.region_counts().iter().sum::<usize>(), 25);
        let matrix = placement.matrix();
        for node in placement.nodes() {
            assert!(matrix.latency(&node.addr, &node.addr) > Duration::ZERO);
        }
    }
}

#[test]
#[should_panic(expected = "addresses must be unique")]
fn duplicate_addresses_are_rejected() {
    let table = RegionTable::single_region(millis(1));
    Placer::new(&table).place([1u32, 2, 1], &mut ChaChaRng::seed_from_u64(0));
}
