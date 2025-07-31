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
    collections::HashMap,
    env,
    fs::File,
    panic,
    time::{Duration, Instant},
};

use simple_xml_builder::XMLElement;

pub mod testcases;

#[derive(Debug)]
pub struct RandomizedTest {
    pub name: &'static str,
    pub func: fn(u64),
}

#[derive(Debug)]
pub struct TestResults {
    pub pass: bool,
    pub time: Duration,
}

#[derive(Debug)]
struct TestsuiteError();

impl std::fmt::Display for TestsuiteError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "tests failed")
    }
}

impl std::error::Error for TestsuiteError {}

fn setup() {
    println!("Running randomized testcases");
}

fn summarize(
    seed: u64,
    results: HashMap<String, TestResults>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_tests = results.len();
    let passed = results.values().filter(|x| x.pass).count();
    let failed = num_tests - passed;
    println!(
        "tests run: {}, passed: {}, failed: {}",
        num_tests, passed, failed,
    );
    println!("{:#?}", results);

    let file = File::create("tests_results.xml")?;
    let mut testsuite = XMLElement::new("testsuite");
    testsuite.add_attribute("name", "Randomized tests");
    testsuite.add_attribute("tests", num_tests);
    testsuite.add_attribute("failures", failed);
    testsuite.add_attribute("errors", 0);
    testsuite.add_attribute("skipped", 0);
    testsuite.add_attribute("assertions", 0);
    testsuite.add_attribute("time", 0);
    testsuite.add_attribute("timestamp", 0);
    testsuite.add_attribute("file", format!("monad-randomized-tests;seed={}", seed));

    for tc in results {
        let mut testcase = XMLElement::new("testcase");
        testcase.add_attribute("name", tc.0);
        testcase.add_attribute("time", tc.1.time.as_millis());

        if !tc.1.pass {
            let failure = XMLElement::new("failure");
            testcase.add_child(failure);
        }

        testsuite.add_child(testcase);
    }

    testsuite.write(file)?;

    if failed > 0 {
        Err(Box::new(TestsuiteError()))
    } else {
        Ok(())
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);

    let arg = match args.get(1) {
        Some(seed) => seed,
        None => {
            println!("need a seed arg");
            return;
        }
    };

    let seed = match arg.parse::<u64>() {
        Ok(x) => x,
        Err(e) => {
            println!("cannot parse seed arg, {}", e);
            return;
        }
    };

    let mut results = HashMap::new();

    setup();

    for t in inventory::iter::<RandomizedTest> {
        let start = Instant::now();
        let result = panic::catch_unwind(|| (t.func)(seed));
        let elapsed = start.elapsed();

        results.insert(
            String::from(t.name),
            TestResults {
                pass: result.is_ok(),
                time: elapsed,
            },
        );
    }

    let r = summarize(seed, results);
    match r {
        Ok(()) => {
            std::process::exit(0);
        }
        Err(e) => {
            println!("testsuite failed, {}", e);
            std::process::exit(-1);
        }
    }
}

inventory::collect!(RandomizedTest);
