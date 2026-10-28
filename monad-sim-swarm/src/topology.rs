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

//! Predefined latency topologies: [`RegionTable`]s of one-way latencies
//! between named regions, and a seeded [`Placer`] that spreads nodes over a
//! table's regions and yields a [`LatencyMatrix`].
//!
//! Every entry of a table, including the diagonal, is positive: it is the
//! base latency of a link, and [`Network::with_matrix`](crate::Network::with_matrix)
//! only adds non-negative jitter to it, so it is also the link's lower bound.

use std::{collections::BTreeSet, ops::RangeInclusive, time::Duration};

use rand::{distributions::WeightedIndex, seq::SliceRandom, Rng};
use rand_distr::{Distribution, LogNormal};

use crate::LatencyMatrix;

/// Where a table's numbers come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provenance {
    /// A short identifier for the table.
    pub name: String,
    /// What the numbers are and where they come from (a measurement, a
    /// dataset with its license, or a hand-entered approximation).
    pub source: String,
    /// When the numbers were measured or entered, as an ISO 8601 date.
    pub date: String,
}

impl Provenance {
    pub fn new(
        name: impl Into<String>,
        source: impl Into<String>,
        date: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            source: source.into(),
            date: date.into(),
        }
    }
}

/// One-way base latencies between named regions, with a weight per region
/// for spreading nodes (see [`Placer`]) and the [`Provenance`] of the
/// numbers. Rows are senders and columns receivers.
#[derive(Clone, Debug)]
pub struct RegionTable {
    provenance: Provenance,
    regions: Vec<String>,
    one_way: Vec<Vec<Duration>>,
    weights: Vec<f64>,
}

impl RegionTable {
    /// A table of one-way latencies with equal region weights.
    ///
    /// Panics unless `one_way` is square with one row per region, every
    /// entry is positive (nodes in the same region still have a positive
    /// latency), and region names are unique.
    pub fn new(
        provenance: Provenance,
        regions: impl IntoIterator<Item = impl Into<String>>,
        one_way: Vec<Vec<Duration>>,
    ) -> Self {
        let regions: Vec<String> = regions.into_iter().map(Into::into).collect();
        let n = regions.len();
        assert!(n > 0, "a region table needs at least one region");
        assert_eq!(
            regions.iter().collect::<BTreeSet<_>>().len(),
            n,
            "region names must be unique"
        );
        assert!(
            one_way.len() == n && one_way.iter().all(|row| row.len() == n),
            "latency table must be square with one row per region"
        );
        assert!(
            one_way.iter().flatten().all(|d| !d.is_zero()),
            "every latency, including the diagonal, must be positive"
        );
        Self {
            provenance,
            regions,
            one_way,
            weights: vec![1.0; n],
        }
    }

    /// A table from round-trip times; each one-way latency is half the RTT.
    /// The same requirements as [`new`](RegionTable::new) apply, so a
    /// measured RTT matrix needs its zero diagonal replaced first.
    pub fn from_rtt(
        provenance: Provenance,
        regions: impl IntoIterator<Item = impl Into<String>>,
        rtt: Vec<Vec<Duration>>,
    ) -> Self {
        let one_way = rtt
            .into_iter()
            .map(|row| row.into_iter().map(|d| d / 2).collect())
            .collect();
        Self::new(provenance, regions, one_way)
    }

    /// Replace the region weights. Panics unless there is one finite,
    /// non-negative weight per region and they are not all zero.
    pub fn with_weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        let weights: Vec<f64> = weights.into_iter().collect();
        assert_eq!(weights.len(), self.regions.len(), "one weight per region");
        assert!(
            weights.iter().all(|w| w.is_finite() && *w >= 0.0),
            "weights must be finite and non-negative"
        );
        assert!(
            weights.iter().any(|w| *w > 0.0),
            "weights must not all be zero"
        );
        self.weights = weights;
        self
    }

    pub fn provenance(&self) -> &Provenance {
        &self.provenance
    }

    pub fn regions(&self) -> &[String] {
        &self.regions
    }

    pub fn len(&self) -> usize {
        self.regions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    pub fn weights(&self) -> &[f64] {
        &self.weights
    }

    /// The index of the region called `name`.
    pub fn region(&self, name: &str) -> Option<usize> {
        self.regions.iter().position(|r| r == name)
    }

    /// The one-way latency from region `from` to region `to` (indices).
    pub fn one_way(&self, from: usize, to: usize) -> Duration {
        self.one_way[from][to]
    }

    /// The smallest entry: a lower bound on every link of the table.
    pub fn min_latency(&self) -> Duration {
        self.one_way
            .iter()
            .flatten()
            .copied()
            .min()
            .expect("non-empty table")
    }

    /// The table as a region-level [`LatencyMatrix`] with no addresses
    /// placed yet; place them with [`LatencyMatrix::place`], or use a
    /// [`Placer`].
    pub fn matrix<Addr: Ord>(&self) -> LatencyMatrix<Addr> {
        LatencyMatrix::new(self.one_way.clone())
    }

    /// Ten public-internet regions in Europe, North America, Asia-Pacific
    /// and South America: FRA, AMS, LON, NYC, CHI, LAX, SGP, TYO, SYD, SAO.
    ///
    /// Derived from the public one-day dataset of the [WonderNetwork Global
    /// Ping Statistics](https://wondernetwork.com/pings) (2020-07-19/20,
    /// published on the [WonderProxy blog](https://wonderproxy.com/blog/a-day-in-the-life-of-the-internet/)):
    /// per city pair, the median of the 30-ping average RTTs (48 samples per
    /// direction), averaged over both directions and rounded to whole
    /// milliseconds, then halved. Nodes in the same region get 1 ms (2 ms
    /// RTT; the dataset's one same-metro pair, in New York, has 1.5 ms). The
    /// weights are illustrative, skewed towards Europe and North America.
    pub fn public_internet() -> Self {
        const REGIONS: [&str; 10] = [
            "FRA", "AMS", "LON", "NYC", "CHI", "LAX", "SGP", "TYO", "SYD", "SAO",
        ];
        const SAME_REGION_RTT_MS: u64 = 2;
        // median RTT in ms, lower triangle: row i lists regions 0..i
        const RTT_MS: [&[u64]; 10] = [
            &[],
            &[14],
            &[13, 7],
            &[83, 76, 71],
            &[97, 98, 86, 23],
            &[148, 139, 130, 68, 58],
            &[150, 163, 170, 243, 228, 186],
            &[222, 240, 217, 176, 169, 115, 76],
            &[274, 290, 279, 201, 251, 138, 94, 114],
            &[192, 188, 179, 127, 150, 165, 375, 281, 330],
        ];
        const WEIGHTS: [f64; 10] = [20.0, 12.0, 10.0, 15.0, 8.0, 8.0, 10.0, 9.0, 4.0, 4.0];

        let rtt_ms = |i: usize, j: usize| match i.cmp(&j) {
            std::cmp::Ordering::Equal => SAME_REGION_RTT_MS,
            std::cmp::Ordering::Greater => RTT_MS[i][j],
            std::cmp::Ordering::Less => RTT_MS[j][i],
        };
        let rtt = (0..REGIONS.len())
            .map(|i| {
                (0..REGIONS.len())
                    .map(|j| Duration::from_millis(rtt_ms(i, j)))
                    .collect()
            })
            .collect();
        Self::from_rtt(
            Provenance::new(
                "public-internet-10",
                "WonderNetwork Global Ping Statistics \
                 (https://wondernetwork.com/pings), public dataset of \
                 2020-07-19/20: per city pair the median of 30-ping average \
                 RTTs, mean of both directions, rounded to ms; one-way = \
                 RTT / 2; same region = 1 ms (assumed); derived 2026-10-28",
                "2020-07-20",
            ),
            REGIONS,
            rtt,
        )
        .with_weights(WEIGHTS)
    }

    /// One region: every link has latency `local`.
    pub fn single_region(local: Duration) -> Self {
        Self::new(synthetic("single-region"), ["local"], vec![vec![local]])
    }

    /// Two regions, `near` within each and `far` between them.
    pub fn two_clusters(near: Duration, far: Duration) -> Self {
        Self::new(
            synthetic("two-clusters"),
            ["a", "b"],
            vec![vec![near, far], vec![far, near]],
        )
    }

    /// `n` regions on a ring: `local` within a region, and `hop` per step
    /// along the shorter way round between regions.
    pub fn ring(n: usize, local: Duration, hop: Duration) -> Self {
        let one_way = (0..n)
            .map(|i| {
                (0..n)
                    .map(|j| {
                        let steps = i.abs_diff(j).min(n - i.abs_diff(j));
                        if steps == 0 {
                            local
                        } else {
                            hop * u32::try_from(steps).expect("ring size")
                        }
                    })
                    .collect()
            })
            .collect();
        Self::new(synthetic("ring"), (0..n).map(|i| format!("r{i}")), one_way)
    }

    /// A hub region (index 0) and `leaves` leaf regions: `local` within a
    /// region, `spoke` between the hub and a leaf, and two spokes between
    /// leaves.
    pub fn star(leaves: usize, local: Duration, spoke: Duration) -> Self {
        let n = leaves + 1;
        let one_way = (0..n)
            .map(|i| {
                (0..n)
                    .map(|j| match (i, j) {
                        _ if i == j => local,
                        (0, _) | (_, 0) => spoke,
                        _ => spoke * 2,
                    })
                    .collect()
            })
            .collect();
        let regions = std::iter::once("hub".to_string()).chain((1..n).map(|i| format!("leaf{i}")));
        Self::new(synthetic("star"), regions, one_way)
    }
}

fn synthetic(name: &str) -> Provenance {
    Provenance::new(name, "synthetic", "2026-10-28")
}

/// How a [`Placer`] assigns stake weights to nodes.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Stake {
    /// Every node gets the same stake.
    #[default]
    Equal,
    /// Independent log-normal stakes, `exp(N(0, sigma²))` before
    /// normalization.
    LogNormal { sigma: f64 },
}

/// How a [`Placer`] decides how many nodes go to each region.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Regions {
    /// Counts apportioned to the weights by largest remainder (ties to the
    /// lower region index), so they match the weights as closely as whole
    /// nodes allow. The seed only decides which address goes where.
    #[default]
    Apportioned,
    /// Each node draws its region independently, with probability
    /// proportional to the weights, so the seed varies the counts too.
    Sampled,
}

/// Spreads nodes over the regions of a [`RegionTable`] according to the
/// region weights, optionally with a per-node last-mile latency and stake
/// weights.
///
/// Randomness comes only from the RNG passed to [`place`](Placer::place), so a
/// seeded RNG gives the same placement every time.
#[derive(Clone, Debug)]
pub struct Placer<'a> {
    table: &'a RegionTable,
    regions: Regions,
    last_mile: Option<RangeInclusive<Duration>>,
    stake: Stake,
}

impl<'a> Placer<'a> {
    pub fn new(table: &'a RegionTable) -> Self {
        Self {
            table,
            regions: Regions::Apportioned,
            last_mile: None,
            stake: Stake::Equal,
        }
    }

    pub fn regions(mut self, regions: Regions) -> Self {
        self.regions = regions;
        self
    }

    /// Give each node a last-mile latency drawn uniformly from `range`. It is
    /// added at both ends of every link, so the matrix becomes node-level.
    pub fn last_mile(mut self, range: RangeInclusive<Duration>) -> Self {
        assert!(range.start() <= range.end(), "empty last-mile range");
        self.last_mile = Some(range);
        self
    }

    pub fn stake(mut self, stake: Stake) -> Self {
        if let Stake::LogNormal { sigma } = stake {
            assert!(sigma.is_finite() && sigma >= 0.0, "invalid sigma {sigma}");
        }
        self.stake = stake;
        self
    }

    /// Place `addrs` over the table's regions.
    ///
    /// The draws from `rng` come in a fixed order: the regions (a shuffle of
    /// the apportioned counts, or one draw per node in address order), then
    /// the last miles and the stakes, each in address order. See [`Regions`].
    ///
    /// Panics if an address repeats.
    pub fn place<Addr: Ord + Clone>(
        &self,
        addrs: impl IntoIterator<Item = Addr>,
        rng: &mut impl Rng,
    ) -> Placement<Addr> {
        let addrs: Vec<Addr> = addrs.into_iter().collect();
        assert_eq!(
            addrs.iter().collect::<BTreeSet<_>>().len(),
            addrs.len(),
            "addresses must be unique"
        );
        let n = addrs.len();

        let regions: Vec<usize> = match self.regions {
            Regions::Apportioned => {
                let mut regions: Vec<usize> = apportion(self.table.weights(), n)
                    .into_iter()
                    .enumerate()
                    .flat_map(|(region, count)| std::iter::repeat_n(region, count))
                    .collect();
                regions.shuffle(rng);
                regions
            }
            Regions::Sampled => {
                let distribution = WeightedIndex::new(self.table.weights()).expect("valid weights");
                (0..n).map(|_| distribution.sample(rng)).collect()
            }
        };

        let last_mile: Vec<Duration> = match &self.last_mile {
            Some(range) => (0..n).map(|_| rng.gen_range(range.clone())).collect(),
            None => vec![Duration::ZERO; n],
        };

        let stake: Vec<f64> = match self.stake {
            Stake::Equal => vec![1.0; n],
            Stake::LogNormal { sigma } => {
                let distribution = LogNormal::new(0.0, sigma).expect("valid sigma");
                (0..n).map(|_| distribution.sample(rng)).collect()
            }
        };
        let total: f64 = stake.iter().sum();

        let nodes = addrs
            .into_iter()
            .zip(regions)
            .zip(last_mile)
            .zip(stake)
            .map(|(((addr, region), last_mile), stake)| PlacedNode {
                addr,
                region,
                last_mile,
                stake: stake / total,
            })
            .collect();
        Placement {
            table: self.table.one_way.clone(),
            node_level: self.last_mile.is_some(),
            nodes,
        }
    }
}

/// Split `n` into counts proportional to `weights` by largest remainder.
fn apportion(weights: &[f64], n: usize) -> Vec<usize> {
    let total: f64 = weights.iter().sum();
    let quotas: Vec<f64> = weights.iter().map(|w| w / total * n as f64).collect();
    let mut counts: Vec<usize> = quotas.iter().map(|q| q.floor() as usize).collect();
    let mut by_remainder: Vec<usize> = (0..weights.len()).collect();
    // stable sort keeps lower indices first among equal remainders
    by_remainder.sort_by(|&a, &b| {
        let ra = quotas[a] - quotas[a].floor();
        let rb = quotas[b] - quotas[b].floor();
        rb.total_cmp(&ra)
    });
    let missing = n - counts.iter().sum::<usize>();
    for &region in by_remainder.iter().take(missing) {
        counts[region] += 1;
    }
    counts
}

/// One node of a [`Placement`].
#[derive(Clone, Debug, PartialEq)]
pub struct PlacedNode<Addr> {
    pub addr: Addr,
    /// The region index in the table.
    pub region: usize,
    /// Added at both ends of each of the node's links; zero without
    /// [`Placer::last_mile`].
    pub last_mile: Duration,
    /// The node's share of the total stake; the shares sum to 1.
    pub stake: f64,
}

/// The output of a [`Placer`]: where each node sits, and the resulting
/// latencies.
#[derive(Clone, Debug)]
pub struct Placement<Addr> {
    table: Vec<Vec<Duration>>,
    node_level: bool,
    nodes: Vec<PlacedNode<Addr>>,
}

impl<Addr: Ord + Clone> Placement<Addr> {
    /// The nodes in the order their addresses were given.
    pub fn nodes(&self) -> &[PlacedNode<Addr>] {
        &self.nodes
    }

    /// The number of nodes per region index.
    pub fn region_counts(&self) -> Vec<usize> {
        let mut counts = vec![0; self.table.len()];
        for node in &self.nodes {
            counts[node.region] += 1;
        }
        counts
    }

    /// The placement as a [`LatencyMatrix`] with every node placed: the
    /// region table itself without last miles, or one row per node with
    /// them, where a link's latency is the region latency plus both nodes'
    /// last miles. A node's link to itself counts its last mile twice.
    pub fn matrix(&self) -> LatencyMatrix<Addr> {
        if !self.node_level {
            return self
                .nodes
                .iter()
                .fold(LatencyMatrix::new(self.table.clone()), |matrix, node| {
                    matrix.place(node.addr.clone(), node.region)
                });
        }
        let one_way = self
            .nodes
            .iter()
            .map(|from| {
                self.nodes
                    .iter()
                    .map(|to| self.table[from.region][to.region] + from.last_mile + to.last_mile)
                    .collect()
            })
            .collect();
        self.nodes
            .iter()
            .enumerate()
            .fold(LatencyMatrix::new(one_way), |matrix, (row, node)| {
                matrix.place(node.addr.clone(), row)
            })
    }
}
