use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    fs::File,
    io::Write,
    vec,
};

use clap::Parser;
use rand::prelude::SliceRandom;
use serde::{Deserialize, Serialize};

struct PartitionGenerator {
    num_nodes: usize,
    num_partitions: usize,
    num_twins: usize,
    partitions: Vec<Vec<usize>>,
    twins_config: Vec<Vec<usize>>,
    twins_partitions: Vec<Vec<Vec<usize>>>,
}

impl PartitionGenerator {
    pub fn new(num_nodes: usize, num_partitions: usize, num_twins: usize) -> Self {
        let mut pg = PartitionGenerator {
            num_nodes,
            num_partitions,
            num_twins,
            partitions: Vec::new(),
            twins_config: Vec::new(),
            twins_partitions: Vec::new(),
        };
        pg.generate_partition(0, num_nodes, vec![0; num_partitions]);
        pg.generate_twins_config(0, &mut vec![0; num_twins]);
        for p in &pg.partitions {
            for t in &pg.twins_config {
                if let Some(partition) = pg.generate_twin_partition(p, t) {
                    pg.twins_partitions.push(partition);
                }
            }
        }
        pg
    }

    fn generate_partition(&mut self, i: usize, n: usize, curr_path: Vec<usize>) {
        let mut curr_path = curr_path;
        curr_path[i] = n;
        if i == 0 || curr_path[i - 1] >= n {
            // only keeps monotonic decreasing partitions to avoid repetition
            self.partitions.push(curr_path.clone());
        }
        let mut m = n - 1;
        if i > 0 {
            m = min(m, curr_path[i - 1]);
        }
        if i < self.num_partitions - 1 {
            while m > 0 {
                curr_path[i] = m;
                self.generate_partition(i + 1, n - m, curr_path.clone());
                m -= 1;
            }
        }
    }

    fn generate_twins_config(&mut self, i: usize, curr_path: &mut Vec<usize>) {
        if i == self.num_twins {
            self.twins_config.push(curr_path.clone());
            return;
        }
        for index in 0..self.num_partitions {
            if i == 0 || curr_path[i - 1] <= index {
                curr_path[i] = index;
                self.generate_twins_config(i + 1, curr_path);
            }
        }
    }

    fn generate_twin_partition(
        &self,
        partition: &[usize],
        twin_config: &[usize],
    ) -> Option<Vec<Vec<usize>>> {
        let mut result = partition.iter().map(|n| vec![0; *n]).collect::<Vec<_>>();
        let mut twins_hashmap: HashMap<usize, usize> = HashMap::new();
        for loc in twin_config {
            let count = twins_hashmap.entry(*loc).or_insert(0);
            *count += 1;
        }
        let mut curr_node = self.num_twins;
        let mut curr_twin = 0;
        for (i, part) in result.iter_mut().enumerate() {
            let start_idx = *twins_hashmap.get(&i).unwrap_or(&0);
            if part.len() < start_idx {
                return None;
            } else {
                for p in part.iter_mut().take(start_idx) {
                    *p = curr_twin;
                    curr_twin += 1;
                }
                for p in part.iter_mut().skip(start_idx) {
                    *p = curr_node;
                    curr_node += 1;
                }
            }
        }
        Some(result)
    }
}

#[derive(Parser, Default)]
struct Arg {
    num_rounds: usize,
    num_nodes: usize,
    num_partitions: usize,
    num_twins: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct Scenario {
    num_of_nodes: usize,
    num_of_twins: usize,
    scenario: Vec<SingleScenario>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SingleScenario {
    round_leaders: BTreeMap<usize, Vec<usize>>,
    round_partitions: BTreeMap<usize, Vec<Vec<usize>>>,
}

fn generate_single_scenario(num_of_rounds: usize, pg: &PartitionGenerator) -> SingleScenario {
    let mut round_leaders: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut round_partitions: BTreeMap<usize, Vec<Vec<usize>>> = BTreeMap::new();
    for round in 0..num_of_rounds {
        let mut rng = rand::thread_rng();
        round_leaders.insert(round + 1, (0..pg.num_twins).collect());
        round_partitions.insert(
            round + 1,
            pg.twins_partitions.choose(&mut rng).unwrap().clone(),
        );
    }
    SingleScenario {
        round_leaders,
        round_partitions,
    }
}

fn main() {
    let cli = Arg::parse();
    let pg = PartitionGenerator::new(cli.num_nodes, cli.num_partitions, cli.num_twins);
    let mut sce = Scenario {
        num_of_nodes: cli.num_nodes,
        num_of_twins: cli.num_twins,
        scenario: Vec::new(),
    };
    let generate_length = 10;
    for _ in 0..generate_length {
        sce.scenario
            .push(generate_single_scenario(cli.num_rounds, &pg));
    }
    println!("Scenario Generated!");
    let ser_scenario = serde_json::to_string(&sce).unwrap();
    let mut file = File::create("scenario.json").unwrap();
    file.write_all(ser_scenario.as_bytes()).unwrap();
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_partition_generator() {
        let pg = PartitionGenerator::new(7, 3, 2);
        for p in &pg.twins_partitions {
            assert_eq!(p.len(), pg.num_partitions);
            let mut collapsed = p.iter().flatten().map(|n| *n).collect::<Vec<usize>>();
            assert_eq!(collapsed.len(), pg.num_nodes);
            collapsed.sort();
            assert_eq!(collapsed, (0..pg.num_nodes).collect::<Vec<_>>());
        }
    }
}
