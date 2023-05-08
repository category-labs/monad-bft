use criterion::{criterion_group, criterion_main, Criterion};
use std::error::Error;
use std::fmt::Debug;
use std::fs::create_dir_all;
use tempfile::{tempdir, TempDir};

use monad_types::{Deserializable, Serializable};
use monad_wal::wal::*;
use monad_wal::PersistenceLogger;

const VOTE_SIZE: usize = 400;
const BLOCK_SIZE: usize = 32 * 10000;
const N_VALIDATORS: usize = 400;

// benchmark file io append only, without serde overhead
#[derive(Debug, Clone)]
struct Datablob {
    data: Vec<u8>,
}

impl Datablob {
    fn new(byte_len: usize) -> Self {
        Datablob {
            data: vec![0xbf; byte_len],
        }
    }
}

#[derive(Debug)]
struct ReadError {}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Debug>::fmt(self, f)
    }
}

impl Error for ReadError {}

impl Serializable for Datablob {
    fn serialize(&self) -> Vec<u8> {
        self.data.clone()
    }
}

impl Deserializable for Datablob {
    type ReadError = ReadError;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
        Ok(Datablob {
            data: message.to_vec(),
        })
    }
}
struct Bencher {
    data: Datablob,
    logger: WALogger<Datablob>,
    _tmpdir: TempDir,
}

impl Bencher {
    fn new(byte_len: usize) -> Bencher {
        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let file_path = tmpdir.path().join("wal");
        Bencher {
            data: Datablob::new(byte_len),
            logger: WALogger::<Datablob>::new(file_path).unwrap().0,
            _tmpdir: tmpdir,
        }
    }

    fn append(&mut self) {
        self.logger.push(&self.data).unwrap()
    }
    fn append_two_write(&mut self) {
        self.logger.push_two_write(&self.data).unwrap()
    }
}

fn bench_block(c: &mut Criterion) {
    let mut bencher = Bencher::new(BLOCK_SIZE);

    c.bench_function("block", |b| b.iter(|| bencher.append()));
}

fn bench_block_two_write(c: &mut Criterion) {
    let mut bencher = Bencher::new(BLOCK_SIZE);

    c.bench_function("block_two_write", |b| b.iter(|| bencher.append_two_write()));
}

fn bench_vote(c: &mut Criterion) {
    let mut bencher = Bencher::new(VOTE_SIZE);

    c.bench_function("vote", |b| {
        b.iter(|| {
            for _ in 0..N_VALIDATORS {
                bencher.append()
            }
        })
    });
}

fn bench_vote_two_write(c: &mut Criterion) {
    let mut bencher = Bencher::new(VOTE_SIZE);

    c.bench_function("vote_two_write", |b| {
        b.iter(|| {
            for _ in 0..N_VALIDATORS {
                bencher.append_two_write()
            }
        })
    });
}

criterion_group!(
    bench,
    bench_block,
    bench_block_two_write,
    bench_vote,
    bench_vote_two_write,
);

#[cfg(target_os = "linux")]
criterion_main!(bench);

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("Linux only benchmark");
}
