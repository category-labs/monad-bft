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
    error::Error,
    fs::File,
    io::Read,
    path::PathBuf,
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Parser)]
#[command(about = "Tool for benchmarking eth_getLogs")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Generate request specifications
    Generate(GenerateArgs),

    /// Run benchmark using request specifications
    Benchmark(BenchmarkArgs),

    /// Compare benchmark results from two different runs
    Compare(CompareArgs),
}

#[derive(Debug, Parser)]
struct GenerateArgs {
    #[arg(short, long, default_value = "requests.json")]
    output_file: PathBuf,

    #[arg(short, long, default_value = "5000000")]
    min_block: u64,

    #[arg(short, long, default_value = "8000000")]
    max_block: u64,

    #[arg(short, long, default_value = "100")]
    min_range: u64,

    #[arg(short, long, default_value = "101")]
    max_range: u64,

    #[arg(short, long, default_value = "10")]
    num_requests: usize,

    #[arg(short, long)]
    frequency_file: Option<PathBuf>,

    #[arg(short = 'T', long, default_value = "3")]
    top_topics: usize,

    #[arg(short = 'A', long, default_value = "3")]
    top_addresses: usize,

    #[arg(long, default_value = "25")]
    address_filter_percent: u8,

    #[arg(long, default_value = "30")]
    topic_filter_percent: u8,
}

#[derive(Debug, Parser)]
struct BenchmarkArgs {
    #[arg(short, long)]
    request_file: PathBuf,

    #[arg(short, long, default_value = "results.json")]
    output_file: PathBuf,

    #[arg(short = 'f', long, default_value = "frequency.json")]
    frequency_file: PathBuf,

    #[arg(short, long, default_value = "http://localhost:8080")]
    url: String,

    #[arg(short, long, default_value = "4")]
    timeout_seconds: u64,
}

#[derive(Debug, Parser)]
struct CompareArgs {
    #[arg(short = '1', long, help = "First benchmark result file")]
    first_file: PathBuf,

    #[arg(short = '2', long, help = "Second benchmark result file")]
    second_file: PathBuf,

    #[arg(
        short,
        long,
        default_value = "comparison.json",
        help = "Output file for comparison results"
    )]
    output_file: PathBuf,
}

/// Represents a request specification
#[derive(Debug, Serialize, Deserialize)]
struct RequestSpec {
    from_block: u64,
    to_block: u64,
    address: Option<String>,
    topics: Vec<Option<String>>,
}

/// Represents benchmark results for a single request
#[derive(Debug, Serialize, Deserialize, Clone)]
struct BenchmarkResult {
    from_block: u64,
    to_block: u64,
    block_range: u64,
    time_taken_ms: u64,
    logs_count: usize,
    error: Option<String>,
}

/// Represents a frequency entry with key and count
#[derive(Debug, Serialize, Deserialize)]
struct FrequencyEntry {
    key: String,
    count: usize,
}

/// Represents a topic frequency entry with position, topic value and count
#[derive(Debug, Serialize, Deserialize)]
struct TopicFrequencyEntry {
    position: usize,
    topic: String,
    count: usize,
}

/// Represents frequency data loaded from file
#[derive(Debug, Serialize, Deserialize)]
struct FrequencyData {
    topics: Vec<TopicFrequencyEntry>,
    addresses: Vec<FrequencyEntry>,
}

/// Represents a comparison of two benchmark result sets
#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkComparison {
    // General statistics
    total_requests: usize,
    matched_requests: usize,

    // First result set stats
    first_name: String,
    first_successful_count: usize,
    first_avg_time_ms: f64,
    first_avg_logs: f64,
    first_avg_range: f64,
    first_range_time_correlation: f64,
    first_logs_time_correlation: f64,

    // Second result set stats
    second_name: String,
    second_successful_count: usize,
    second_avg_time_ms: f64,
    second_avg_logs: f64,
    second_avg_range: f64,
    second_range_time_correlation: f64,
    second_logs_time_correlation: f64,

    // Comparison metrics
    time_diff_percentage: f64,
    time_diff_absolute_ms: f64,

    // Request by request comparisons
    request_comparisons: Vec<RequestComparison>,
}

/// Represents a comparison of two individual benchmark requests
#[derive(Debug, Serialize, Deserialize)]
struct RequestComparison {
    from_block: u64,
    to_block: u64,
    block_range: u64,

    first_time_ms: u64,
    first_logs_count: usize,
    first_had_error: bool,

    second_time_ms: u64,
    second_logs_count: usize,
    second_had_error: bool,

    time_diff_ms: i64,
    time_diff_percentage: f64,
    logs_count_diff: i64,
}

/// Generate a random block range between min_range and max_range blocks in size
/// within the overall range of min_block to max_block
fn generate_random_block_range(
    min_block: u64,
    max_block: u64,
    min_range: u64,
    max_range: u64,
) -> (u64, u64) {
    let mut rng = rand::thread_rng();

    // Ensure range size is within bounds
    let range_size = rng.gen_range(min_range..=max_range);

    // Ensure the starting block allows for the full range
    let max_start = max_block.saturating_sub(range_size);
    let from_block = rng.gen_range(min_block..=max_start);
    let to_block = from_block + range_size;

    (from_block, to_block)
}

/// Create a request specification with optional topic and address filters
fn create_request_spec(
    from_block: u64,
    to_block: u64,
    popular_topics: &[TopicFrequencyEntry],
    popular_addresses: &[FrequencyEntry],
    address_filter_percent: u8,
    topic_filter_percent: u8,
) -> RequestSpec {
    let mut rng = rand::thread_rng();

    // Randomly decide if we should filter by address based on specified percentage
    let address =
        if rng.gen_ratio(address_filter_percent as u32, 100) && !popular_addresses.is_empty() {
            // Select a random popular address
            let idx = rng.gen_range(0..popular_addresses.len());
            Some(popular_addresses[idx].key.clone())
        } else {
            None
        };

    // Randomly decide if we should filter by topics based on specified percentage
    let topics = if rng.gen_ratio(topic_filter_percent as u32, 100) && !popular_topics.is_empty() {
        // Track max topic position to create a properly sized array
        let max_position = popular_topics
            .iter()
            .map(|topic| topic.position)
            .max()
            .unwrap_or(0);

        // Initialize with None values
        let mut topic_filters = vec![None; max_position + 1];

        // Select a random popular topic
        let idx = rng.gen_range(0..popular_topics.len());
        let selected_topic = &popular_topics[idx];

        // Set the topic at the correct position
        topic_filters[selected_topic.position] = Some(selected_topic.topic.clone());

        topic_filters
    } else {
        vec![]
    };

    RequestSpec {
        from_block,
        to_block,
        address,
        topics,
    }
}

/// Load frequency data from a file
fn load_frequency_data(path: &PathBuf) -> Result<FrequencyData, Box<dyn Error>> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let data: FrequencyData = serde_json::from_str(&contents)?;
    Ok(data)
}

/// Generate request specifications
async fn generate_requests(args: &GenerateArgs) -> Result<(), Box<dyn Error>> {
    println!("Generating {} request specifications...", args.num_requests);
    println!(
        "Using {}% address filters and {}% topic filters",
        args.address_filter_percent, args.topic_filter_percent
    );

    // Load frequency data if available
    let (popular_topics, popular_addresses) = if let Some(freq_path) = &args.frequency_file {
        match load_frequency_data(freq_path) {
            Ok(data) => {
                println!("Loaded frequency data from {}", freq_path.display());

                // Get top N topics
                let top_topics: Vec<_> = data.topics.into_iter().take(args.top_topics).collect();

                // Get top N addresses
                let top_addresses: Vec<_> = data
                    .addresses
                    .into_iter()
                    .take(args.top_addresses)
                    .collect();

                println!(
                    "Using {} popular topics and {} popular addresses",
                    top_topics.len(),
                    top_addresses.len()
                );

                (top_topics, top_addresses)
            }
            Err(e) => {
                println!("Failed to load frequency data: {}", e);
                (Vec::new(), Vec::new())
            }
        }
    } else {
        println!("No frequency file specified, generating requests without filters");
        (Vec::new(), Vec::new())
    };

    // Generate request specifications
    let mut specs = Vec::with_capacity(args.num_requests);

    for i in 0..args.num_requests {
        let (from_block, to_block) = generate_random_block_range(
            args.min_block,
            args.max_block,
            args.min_range,
            args.max_range,
        );

        let spec = create_request_spec(
            from_block,
            to_block,
            &popular_topics,
            &popular_addresses,
            args.address_filter_percent,
            args.topic_filter_percent,
        );

        println!(
            "Generated request {}/{}: Blocks {} to {} (range: {})",
            i + 1,
            args.num_requests,
            spec.from_block,
            spec.to_block,
            spec.to_block - spec.from_block
        );

        if let Some(addr) = &spec.address {
            println!("  With address filter: {}", addr);
        }

        if !spec.topics.is_empty() {
            println!("  With topic filters: {:?}", spec.topics);
        }

        specs.push(spec);
    }

    // Save to file
    let file = File::create(&args.output_file)?;
    serde_json::to_writer_pretty(file, &specs)?;

    println!(
        "Saved {} request specifications to {}",
        specs.len(),
        args.output_file.display()
    );

    Ok(())
}

/// Run benchmarks using request specifications
async fn run_benchmarks(args: &BenchmarkArgs) -> Result<(), Box<dyn Error>> {
    // Load request specifications
    let file = File::open(&args.request_file)?;
    let specs: Vec<RequestSpec> = serde_json::from_reader(file)?;

    println!(
        "Loaded {} request specifications from {}",
        specs.len(),
        args.request_file.display()
    );

    // Create HTTP client
    let client = Client::builder()
        .timeout(Duration::from_secs(args.timeout_seconds))
        .build()?;

    // Frequency counters
    let mut topic_frequencies: HashMap<String, usize> = HashMap::new();
    let mut address_frequencies: HashMap<String, usize> = HashMap::new();

    // Store benchmark results
    let mut results = Vec::with_capacity(specs.len());
    // Store only successful results for statistics
    let mut successful_results = Vec::with_capacity(specs.len());

    // Run benchmarks
    println!("Running {} benchmarks...", specs.len());
    println!(
        "{:<10} {:<12} {:<12} {:<12} {:<15} {:<10}",
        "Request", "From Block", "To Block", "Range", "Time (ms)", "Logs"
    );
    println!("{}", "-".repeat(65));

    for (i, spec) in specs.iter().enumerate() {
        let from_block = spec.from_block;
        let to_block = spec.to_block;
        let block_range = to_block - from_block;

        // Convert to hex strings
        let from_block_hex = format!("0x{:x}", from_block);
        let to_block_hex = format!("0x{:x}", to_block);

        // Prepare request parameters
        let mut params = json!({
            "fromBlock": from_block_hex,
            "toBlock": to_block_hex,
        });

        // Add address filter if specified
        if let Some(address) = &spec.address {
            params["address"] = json!(address);
        }

        // Add topics filter if specified
        if !spec.topics.is_empty() {
            params["topics"] = json!(spec.topics);
        }

        let json_request = json!({
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [params],
            "id": i + 1
        });

        // Time the request
        let start = Instant::now();
        let response_result = client.post(&args.url).json(&json_request).send().await;

        let result = match response_result {
            Ok(response) => {
                let text = response.text().await?;
                let elapsed = start.elapsed();

                // Parse the response to get log count and extract topics and addresses
                match serde_json::from_str::<Value>(&text) {
                    Ok(json) => {
                        if let Some(result) = json.get("result").and_then(|r| r.as_array()) {
                            // Process each log entry to extract topics and addresses
                            for log in result {
                                // Extract address - all lowercase to avoid case-sensitivity issues
                                if let Some(address) = log.get("address").and_then(|a| a.as_str()) {
                                    let address_lower = address.to_lowercase();
                                    *address_frequencies.entry(address_lower).or_insert(0) += 1;
                                }

                                // Extract topics
                                if let Some(topics) = log.get("topics").and_then(|t| t.as_array()) {
                                    for (idx, topic) in topics.iter().enumerate() {
                                        if let Some(topic_str) = topic.as_str() {
                                            let topic_key =
                                                format!("{}:{}", idx, topic_str.to_lowercase());
                                            *topic_frequencies.entry(topic_key).or_insert(0) += 1;
                                        }
                                    }
                                }
                            }

                            let successful_result = BenchmarkResult {
                                from_block,
                                to_block,
                                block_range,
                                time_taken_ms: elapsed.as_millis() as u64,
                                logs_count: result.len(),
                                error: None,
                            };

                            // Add to successful results for statistics
                            successful_results.push(successful_result.clone());

                            successful_result
                        } else {
                            println!("Warning: Could not parse result array from response");
                            if let Some(error) = json.get("error") {
                                println!("Error reported: {:?}", error);

                                BenchmarkResult {
                                    from_block,
                                    to_block,
                                    block_range,
                                    time_taken_ms: elapsed.as_millis() as u64,
                                    logs_count: 0,
                                    error: Some(error.to_string()),
                                }
                            } else {
                                BenchmarkResult {
                                    from_block,
                                    to_block,
                                    block_range,
                                    time_taken_ms: elapsed.as_millis() as u64,
                                    logs_count: 0,
                                    error: Some("Unknown error parsing response".to_string()),
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Warning: Failed to parse JSON response: {}", e);

                        BenchmarkResult {
                            from_block,
                            to_block,
                            block_range,
                            time_taken_ms: elapsed.as_millis() as u64,
                            logs_count: 0,
                            error: Some(format!("Failed to parse JSON: {}", e)),
                        }
                    }
                }
            }
            Err(e) => {
                println!("Error: {}", e);

                BenchmarkResult {
                    from_block,
                    to_block,
                    block_range,
                    time_taken_ms: 0,
                    logs_count: 0,
                    error: Some(e.to_string()),
                }
            }
        };

        // Print result
        if let Some(error) = &result.error {
            println!(
                "{:<10} {:<12} {:<12} {:<12} ERROR: {}",
                i + 1,
                result.from_block,
                result.to_block,
                result.block_range,
                error
            );
        } else {
            println!(
                "{:<10} {:<12} {:<12} {:<12} {:<15} {:<10}",
                i + 1,
                result.from_block,
                result.to_block,
                result.block_range,
                result.time_taken_ms,
                result.logs_count
            );
        }

        // Add to all results (both successful and failed)
        results.push(result);

        // Add a small delay between requests to avoid overwhelming the server
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Calculate and display statistics (only for successful results)
    display_benchmark_statistics(&successful_results);

    // Process frequency data
    let frequency_data = process_frequencies(topic_frequencies, address_frequencies);

    // Save frequency data to a separate file
    let frequency_file = File::create(&args.frequency_file)?;
    serde_json::to_writer_pretty(frequency_file, &frequency_data)?;
    println!(
        "\nFrequency data written to {}",
        args.frequency_file.display()
    );

    // Display top frequencies
    display_top_frequencies(&frequency_data);

    // Save results
    let json_results = json!({
        "results": results,  // Save all results
        "successful_count": successful_results.len(),
        "total_count": results.len(),
        "frequencies": frequency_data
    });

    let file = File::create(&args.output_file)?;
    serde_json::to_writer_pretty(file, &json_results)?;

    println!(
        "\nSaved benchmark results to {} ({} successful, {} total)",
        args.output_file.display(),
        successful_results.len(),
        results.len()
    );

    Ok(())
}

/// Process frequencies and return as structured data
fn process_frequencies(
    topic_frequencies: HashMap<String, usize>,
    address_frequencies: HashMap<String, usize>,
) -> FrequencyData {
    // Process topic frequencies
    let mut topic_entries: Vec<FrequencyEntry> = topic_frequencies
        .into_iter()
        .map(|(key, count)| FrequencyEntry { key, count })
        .collect();
    topic_entries.sort_by(|a, b| b.count.cmp(&a.count)); // Sort by count in descending order

    // Process topic entries to split position and topic
    let topic_entries_formatted: Vec<TopicFrequencyEntry> = topic_entries
        .iter()
        .filter_map(|entry| {
            let parts: Vec<&str> = entry.key.splitn(2, ':').collect();
            if parts.len() == 2 {
                if let Ok(position) = parts[0].parse::<usize>() {
                    Some(TopicFrequencyEntry {
                        position,
                        topic: parts[1].to_string(),
                        count: entry.count,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Process address frequencies
    let mut address_entries: Vec<FrequencyEntry> = address_frequencies
        .into_iter()
        .map(|(key, count)| FrequencyEntry { key, count })
        .collect();
    address_entries.sort_by(|a, b| b.count.cmp(&a.count)); // Sort by count in descending order

    FrequencyData {
        topics: topic_entries_formatted,
        addresses: address_entries,
    }
}

/// Display benchmark statistics
fn display_benchmark_statistics(results: &[BenchmarkResult]) {
    // Check if there are any results to analyze
    if results.is_empty() {
        println!("\n===== SUMMARY =====");
        println!("No successful benchmark results to analyze");
        return;
    }

    let avg_time =
        results.iter().map(|r| r.time_taken_ms as f64).sum::<f64>() / results.len() as f64;

    let avg_logs =
        results.iter().map(|r| r.logs_count).sum::<usize>() as f64 / results.len() as f64;

    let avg_range =
        results.iter().map(|r| r.block_range).sum::<u64>() as f64 / results.len() as f64;

    println!("\n===== SUMMARY =====");
    println!("Average time: {:.2} ms", avg_time);
    println!("Average logs: {:.2}", avg_logs);
    println!("Average range: {:.2} blocks", avg_range);

    // Compute some simple correlation metrics
    if results.len() > 1 {
        println!("\n===== SCALING RELATIONSHIPS =====");

        // Analyze relationship between block range and time
        let range_time_correlation = compute_correlation(
            results.iter().map(|r| r.block_range as f64).collect(),
            results.iter().map(|r| r.time_taken_ms as f64).collect(),
        );
        println!(
            "Block Range vs Time correlation: {:.4}",
            range_time_correlation
        );

        // Analyze relationship between logs count and time
        let logs_time_correlation = compute_correlation(
            results.iter().map(|r| r.logs_count as f64).collect(),
            results.iter().map(|r| r.time_taken_ms as f64).collect(),
        );
        println!(
            "Logs Count vs Time correlation: {:.4}",
            logs_time_correlation
        );
    }
}

/// Compute Pearson correlation coefficient between two vectors
fn compute_correlation(x: Vec<f64>, y: Vec<f64>) -> f64 {
    if x.len() != y.len() || x.is_empty() {
        return 0.0;
    }

    let n = x.len() as f64;
    let sum_x: f64 = x.iter().sum();
    let sum_y: f64 = y.iter().sum();
    let sum_xy: f64 = x.iter().zip(y.iter()).map(|(xi, yi)| xi * yi).sum();
    let sum_xx: f64 = x.iter().map(|xi| xi * xi).sum();
    let sum_yy: f64 = y.iter().map(|yi| yi * yi).sum();

    let numerator = n * sum_xy - sum_x * sum_y;
    let denominator = ((n * sum_xx - sum_x * sum_x) * (n * sum_yy - sum_y * sum_y)).sqrt();

    if denominator.abs() < f64::EPSILON {
        return 0.0;
    }

    numerator / denominator
}

/// Display top frequencies
fn display_top_frequencies(frequency_data: &FrequencyData) {
    // Check if we have any frequency data to display
    if frequency_data.topics.is_empty() && frequency_data.addresses.is_empty() {
        println!("\n===== TOP FREQUENCIES =====");
        println!("No frequency data collected");
        return;
    }

    println!("\n===== TOP FREQUENCIES =====");

    // Display top topics
    println!("Top topics:");
    if frequency_data.topics.is_empty() {
        println!("  No topics found");
    } else {
        for (i, topic) in frequency_data.topics.iter().take(5).enumerate() {
            println!(
                "  {}: Position {} - {} (count: {})",
                i + 1,
                topic.position,
                topic.topic,
                topic.count
            );
        }
    }

    // Display top addresses
    println!("\nTop addresses:");
    if frequency_data.addresses.is_empty() {
        println!("  No addresses found");
    } else {
        for (i, address) in frequency_data.addresses.iter().take(5).enumerate() {
            println!("  {}: {} (count: {})", i + 1, address.key, address.count);
        }
    }
}

/// Compare results from two separate benchmark runs
async fn compare_results(args: &CompareArgs) -> Result<(), Box<dyn Error>> {
    println!("Comparing benchmark results...");

    // Load first result file
    println!("Loading first result file: {}", args.first_file.display());
    let mut file = File::open(&args.first_file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let first_data: Value = serde_json::from_str(&contents)?;

    // Load second result file
    println!("Loading second result file: {}", args.second_file.display());
    let mut file = File::open(&args.second_file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let second_data: Value = serde_json::from_str(&contents)?;

    // Extract results from both files
    let first_results = extract_results(&first_data)?;
    let second_results = extract_results(&second_data)?;

    // Generate comparison
    let comparison = generate_comparison(
        args.first_file.to_string_lossy().to_string(),
        first_results,
        args.second_file.to_string_lossy().to_string(),
        second_results,
    )?;

    // Display comparison
    display_comparison(&comparison);

    // Save comparison to file
    let file = File::create(&args.output_file)?;
    serde_json::to_writer_pretty(file, &comparison)?;
    println!("\nComparison saved to {}", args.output_file.display());

    Ok(())
}

/// Extract result data from the loaded JSON
fn extract_results(data: &Value) -> Result<Vec<BenchmarkResult>, Box<dyn Error>> {
    let results = data
        .get("results")
        .and_then(|r| r.as_array())
        .ok_or("Could not find results array in data")?;

    let mut benchmark_results = Vec::with_capacity(results.len());

    for result in results {
        let benchmark_result = BenchmarkResult {
            from_block: result["from_block"].as_u64().unwrap_or(0),
            to_block: result["to_block"].as_u64().unwrap_or(0),
            block_range: result["block_range"].as_u64().unwrap_or(0),
            time_taken_ms: result["time_taken_ms"].as_u64().unwrap_or(0),
            logs_count: result["logs_count"].as_u64().unwrap_or(0) as usize,
            error: result["error"].as_str().map(|s| s.to_string()),
        };

        benchmark_results.push(benchmark_result);
    }

    Ok(benchmark_results)
}

/// Generate comprehensive comparison between two result sets
fn generate_comparison(
    first_name: String,
    first_results: Vec<BenchmarkResult>,
    second_name: String,
    second_results: Vec<BenchmarkResult>,
) -> Result<BenchmarkComparison, Box<dyn Error>> {
    // Filter successful results
    let first_successful: Vec<&BenchmarkResult> =
        first_results.iter().filter(|r| r.error.is_none()).collect();

    let second_successful: Vec<&BenchmarkResult> = second_results
        .iter()
        .filter(|r| r.error.is_none())
        .collect();

    // Calculate basic statistics for first result set
    let first_avg_time = if !first_successful.is_empty() {
        first_successful
            .iter()
            .map(|r| r.time_taken_ms as f64)
            .sum::<f64>()
            / first_successful.len() as f64
    } else {
        0.0
    };

    let first_avg_logs = if !first_successful.is_empty() {
        first_successful
            .iter()
            .map(|r| r.logs_count as f64)
            .sum::<f64>()
            / first_successful.len() as f64
    } else {
        0.0
    };

    let first_avg_range = if !first_successful.is_empty() {
        first_successful
            .iter()
            .map(|r| r.block_range as f64)
            .sum::<f64>()
            / first_successful.len() as f64
    } else {
        0.0
    };

    // Calculate correlation metrics for first result set
    let first_range_time_correlation = if first_successful.len() > 1 {
        compute_correlation(
            first_successful
                .iter()
                .map(|r| r.block_range as f64)
                .collect(),
            first_successful
                .iter()
                .map(|r| r.time_taken_ms as f64)
                .collect(),
        )
    } else {
        0.0
    };

    let first_logs_time_correlation = if first_successful.len() > 1 {
        compute_correlation(
            first_successful
                .iter()
                .map(|r| r.logs_count as f64)
                .collect(),
            first_successful
                .iter()
                .map(|r| r.time_taken_ms as f64)
                .collect(),
        )
    } else {
        0.0
    };

    // Calculate basic statistics for second result set
    let second_avg_time = if !second_successful.is_empty() {
        second_successful
            .iter()
            .map(|r| r.time_taken_ms as f64)
            .sum::<f64>()
            / second_successful.len() as f64
    } else {
        0.0
    };

    let second_avg_logs = if !second_successful.is_empty() {
        second_successful
            .iter()
            .map(|r| r.logs_count as f64)
            .sum::<f64>()
            / second_successful.len() as f64
    } else {
        0.0
    };

    let second_avg_range = if !second_successful.is_empty() {
        second_successful
            .iter()
            .map(|r| r.block_range as f64)
            .sum::<f64>()
            / second_successful.len() as f64
    } else {
        0.0
    };

    // Calculate correlation metrics for second result set
    let second_range_time_correlation = if second_successful.len() > 1 {
        compute_correlation(
            second_successful
                .iter()
                .map(|r| r.block_range as f64)
                .collect(),
            second_successful
                .iter()
                .map(|r| r.time_taken_ms as f64)
                .collect(),
        )
    } else {
        0.0
    };

    let second_logs_time_correlation = if second_successful.len() > 1 {
        compute_correlation(
            second_successful
                .iter()
                .map(|r| r.logs_count as f64)
                .collect(),
            second_successful
                .iter()
                .map(|r| r.time_taken_ms as f64)
                .collect(),
        )
    } else {
        0.0
    };

    // Calculate overall comparison metrics
    let time_diff_absolute_ms = second_avg_time - first_avg_time;
    let time_diff_percentage = if first_avg_time > 0.0 {
        (second_avg_time - first_avg_time) / first_avg_time * 100.0
    } else {
        0.0
    };

    // Generate request by request comparisons
    let mut request_comparisons = Vec::new();
    let mut matched_requests = 0;

    // Create a map for faster lookup by block range
    let mut first_map: HashMap<(u64, u64), &BenchmarkResult> = HashMap::new();
    for result in &first_results {
        first_map.insert((result.from_block, result.to_block), result);
    }

    // Compare each request from the second set with its counterpart in the first set
    for second_result in &second_results {
        if let Some(first_result) =
            first_map.get(&(second_result.from_block, second_result.to_block))
        {
            matched_requests += 1;

            let time_diff_ms =
                second_result.time_taken_ms as i64 - first_result.time_taken_ms as i64;
            let time_diff_percentage = if first_result.time_taken_ms > 0 {
                (second_result.time_taken_ms as f64 - first_result.time_taken_ms as f64)
                    / first_result.time_taken_ms as f64
                    * 100.0
            } else {
                0.0
            };

            let logs_count_diff = second_result.logs_count as i64 - first_result.logs_count as i64;

            request_comparisons.push(RequestComparison {
                from_block: second_result.from_block,
                to_block: second_result.to_block,
                block_range: second_result.block_range,

                first_time_ms: first_result.time_taken_ms,
                first_logs_count: first_result.logs_count,
                first_had_error: first_result.error.is_some(),

                second_time_ms: second_result.time_taken_ms,
                second_logs_count: second_result.logs_count,
                second_had_error: second_result.error.is_some(),

                time_diff_ms,
                time_diff_percentage,
                logs_count_diff,
            });
        }
    }

    // Sort comparisons by block range for consistent output
    request_comparisons.sort_by(|a, b| a.from_block.cmp(&b.from_block));

    Ok(BenchmarkComparison {
        total_requests: first_results.len().max(second_results.len()),
        matched_requests,

        first_name,
        first_successful_count: first_successful.len(),
        first_avg_time_ms: first_avg_time,
        first_avg_logs,
        first_avg_range,
        first_range_time_correlation,
        first_logs_time_correlation,

        second_name,
        second_successful_count: second_successful.len(),
        second_avg_time_ms: second_avg_time,
        second_avg_logs,
        second_avg_range,
        second_range_time_correlation,
        second_logs_time_correlation,

        time_diff_percentage,
        time_diff_absolute_ms,

        request_comparisons,
    })
}

/// Display comparison results
fn display_comparison(comparison: &BenchmarkComparison) {
    println!("\n===== BENCHMARK COMPARISON =====");
    println!(
        "Comparing: {} vs {}",
        comparison.first_name, comparison.second_name
    );
    println!(
        "Matched requests: {}/{}",
        comparison.matched_requests, comparison.total_requests
    );

    println!("\n===== SUMMARY STATISTICS =====");

    println!(
        "{:<30} {:<15} {:<15} {:<15}",
        "Metric", "First Run", "Second Run", "Difference"
    );
    println!("{}", "-".repeat(75));

    println!(
        "{:<30} {:<15} {:<15} {:<15}",
        "Successful Requests",
        comparison.first_successful_count,
        comparison.second_successful_count,
        format!(
            "{:+}",
            comparison.second_successful_count as i64 - comparison.first_successful_count as i64
        )
    );

    println!(
        "{:<30} {:<15.2} {:<15.2} {:<15.2}",
        "Average Time (ms)",
        comparison.first_avg_time_ms,
        comparison.second_avg_time_ms,
        comparison.time_diff_absolute_ms
    );

    println!(
        "{:<30} {:<15.2} {:<15.2} {:<15.2}%",
        "Time Difference (%)", "-", "-", comparison.time_diff_percentage
    );

    println!(
        "{:<30} {:<15.2} {:<15.2} {:<15.2}",
        "Average Logs Count",
        comparison.first_avg_logs,
        comparison.second_avg_logs,
        comparison.second_avg_logs - comparison.first_avg_logs
    );

    println!(
        "{:<30} {:<15.2} {:<15.2} {:<15}",
        "Range-Time Correlation",
        comparison.first_range_time_correlation,
        comparison.second_range_time_correlation,
        "-"
    );

    println!(
        "{:<30} {:<15.2} {:<15.2} {:<15}",
        "Logs-Time Correlation",
        comparison.first_logs_time_correlation,
        comparison.second_logs_time_correlation,
        "-"
    );

    // Display performance interpretation
    println!("\n===== PERFORMANCE COMPARISON =====");

    if comparison.time_diff_percentage < 0.0 {
        println!(
            "Second run was FASTER by {:.2}% ({:.2} ms)",
            -comparison.time_diff_percentage, -comparison.time_diff_absolute_ms
        );
    } else if comparison.time_diff_percentage > 0.0 {
        println!(
            "Second run was SLOWER by {:.2}% ({:.2} ms)",
            comparison.time_diff_percentage, comparison.time_diff_absolute_ms
        );
    } else {
        println!("No significant performance difference between runs");
    }

    // Display request-by-request comparison if not too many
    if comparison.request_comparisons.len() < 1000 {
        println!("\n===== REQUEST BY REQUEST COMPARISON =====");
        println!(
            "{:<10} {:<10} {:<15} {:<15} {:<15}",
            "From", "To", "First (ms)", "Second (ms)", "Diff (%)"
        );
        println!("{}", "-".repeat(65));

        for req in &comparison.request_comparisons {
            println!(
                "{:<10} {:<10} {:<15} {:<15} {:<15.2}%",
                req.from_block,
                req.to_block,
                req.first_time_ms,
                req.second_time_ms,
                req.time_diff_percentage
            );
        }
    } else {
        println!(
            "\n{} requests compared (too many to display individually)",
            comparison.request_comparisons.len()
        );

        // Display summary of improvements/degradations
        let improvements = comparison
            .request_comparisons
            .iter()
            .filter(|r| r.time_diff_percentage < -5.0)
            .count();

        let degradations = comparison
            .request_comparisons
            .iter()
            .filter(|r| r.time_diff_percentage > 5.0)
            .count();

        let neutral = comparison.request_comparisons.len() - improvements - degradations;

        println!("Improvements (>5% faster): {}", improvements);
        println!("Degradations (>5% slower): {}", degradations);
        println!("Neutral (within ±5%): {}", neutral);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match &args.command {
        Commands::Generate(generate_args) => {
            generate_requests(generate_args).await?;
        }
        Commands::Benchmark(benchmark_args) => {
            run_benchmarks(benchmark_args).await?;
        }
        Commands::Compare(compare_args) => {
            compare_results(compare_args).await?;
        }
    }

    Ok(())
}
