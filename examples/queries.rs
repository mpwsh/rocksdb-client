use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rocksdb_client::{KVStore, KvStoreError, Options, RocksDB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
enum MatchStyle {
    Team,
    Dm,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Room {
    id: u64,
    name: String,
    owner: u64,
    style: MatchStyle,
    capacity: u32,
    player_count: u32,
    is_private: bool,
    created_at: i64,
    tags: Vec<String>,
}

#[derive(Debug)]
struct QueryBenchmarkResults {
    query_type: String,
    total_matches: usize,
    duration: Duration,
    matches_per_second: f64,
}

async fn run_query_benchmark(db: Arc<RocksDB>, num_rooms: usize) -> Result<(), KvStoreError> {
    const QUERY_TIMEOUT_SECS: u64 = 10;
    println!("Generating {} test rooms...", num_rooms);
    let rooms = generate_test_rooms(num_rooms);

    println!("Inserting rooms into database...");
    let insert_start = Instant::now();
    for room in &rooms {
        db.insert_cf("rooms", &room.id.to_string(), room)?;
    }
    println!("Insertion took: {:?}", insert_start.elapsed());

    let queries = vec![
        ("Get all rooms", "$[*]"),
        ("Get room at index 0", "$[0]"),
        ("Get last room (negative index)", "$[-1]"),
        ("Get rooms at specific indices (0 and 2)", "$[0,2]"),
        // Array Slice Selectors
        ("Get rooms from index 0 to 2 (exclusive)", "$[0:2]"),
        ("Get rooms from index 1 to end", "$[1:]"),
        ("Get last two rooms", "$[-2:]"),
        // Filter Expressions
        ("Find rooms with owner property", "$[?@.owner]"),
        ("Find rooms with id equal to 2", "$[?@.id==2]"),
        ("Find rooms with id not equal to 2", "$[?@.id!=2]"),
        ("Find rooms with id less than 3", "$[?@.id<3]"),
        ("Find rooms with id greater than 1", "$[?@.id>1]"),
        ("Find rooms with id less than or equal to 2", "$[?@.id<=2]"),
        (
            "Find rooms with id greater than or equal to 2",
            "$[?@.id>=2]",
        ),
        // Logical Operators
        (
            "Find rooms with id > 1 AND style == Team",
            "$[?@.id>1&&@.style=='Team']",
        ),
        (
            "Find rooms with id == 1 OR style == Dm",
            "$[?@.id==1||@.style=='Dm']",
        ),
        (
            "Find rooms that don't have style == Dm",
            "$[?!(@.style=='Dm')]",
        ),
        ("Find rooms with id equal to 1 or 3", "$[?@.id==1||@.id==3]"),
        // Property Selectors
        (
            "Get specific room property (name of first room)",
            "$[0].name",
        ),
        ("Find settings with color = 'blue'", "$[?@.color=='blue']"),
        ("Find rooms with id = 5", "$[?@.id==5]"),
        ("Find Team rooms", "$[?@.style=='Team']"),
        (
            "Find available Team rooms",
            "$[?@.style=='Team'&&@.player_count<@.capacity]",
        ),
        (
            "Find popular rooms",
            "$[?@.player_count>5&&@.is_private==false]",
        ),
        (
            "Find empty public rooms",
            "$[?@.player_count==0&&@.is_private==false]",
        ),
        ("Find rooms with specific tag", "$[?@.tags[0]=='abc']"),
        ("Find rooms with any tags", "$[?@.tags[0]]"),
        (
            "Find recent team rooms",
            "$[?@.style=='Team'&&@.created_at>1234567890]",
        ),
    ];

    // Store results for values-only and key-value queries
    let mut value_only_results = Vec::new();
    let mut key_value_results = Vec::new();

    // Run both types of queries for each test case
    for (description, query) in queries {
        println!("\n{:-<50}", "");
        println!("Query: {} - {}", description, query);
        println!("{:-<50}", "");

        // Run values-only query
        println!("\nRunning without keys (query_cf)...");
        let start = Instant::now();
        let query_result = tokio::time::timeout(Duration::from_secs(QUERY_TIMEOUT_SECS), async {
            db.query_cf::<Room>("rooms", query)
        })
        .await;

        match query_result {
            Ok(Ok(matching_rooms)) => {
                let duration = start.elapsed();
                let result = QueryBenchmarkResults {
                    query_type: description.to_string(),
                    total_matches: matching_rooms.len(),
                    duration,
                    matches_per_second: matching_rooms.len() as f64 / duration.as_secs_f64(),
                };
                println!(
                    "Values-only results: {} matches in {:?}",
                    matching_rooms.len(),
                    duration
                );
                value_only_results.push(result);
            }
            Ok(Err(e)) => {
                println!("Query error: {:?}", e);
                value_only_results.push(QueryBenchmarkResults {
                    query_type: format!("{} (ERROR)", description),
                    total_matches: 0,
                    duration: start.elapsed(),
                    matches_per_second: 0.0,
                });
            }
            Err(_) => {
                println!("Query timed out after {} seconds!", QUERY_TIMEOUT_SECS);
                value_only_results.push(QueryBenchmarkResults {
                    query_type: format!("{} (TIMEOUT)", description),
                    total_matches: 0,
                    duration: Duration::from_secs(QUERY_TIMEOUT_SECS),
                    matches_per_second: 0.0,
                });
            }
        }

        // Run key-value query
        println!("\nRunning with keys (query_cf_with_keys)...");
        let start = Instant::now();
        let query_result = tokio::time::timeout(Duration::from_secs(QUERY_TIMEOUT_SECS), async {
            db.query_cf_with_keys::<Room>("rooms", query)
        })
        .await;

        match query_result {
            Ok(Ok(matching_rooms)) => {
                let duration = start.elapsed();
                let result = QueryBenchmarkResults {
                    query_type: description.to_string(),
                    total_matches: matching_rooms.len(),
                    duration,
                    matches_per_second: matching_rooms.len() as f64 / duration.as_secs_f64(),
                };
                println!(
                    "Key-value results: {} matches in {:?}",
                    matching_rooms.len(),
                    duration
                );
                key_value_results.push(result);
            }
            Ok(Err(e)) => {
                println!("Query error: {:?}", e);
                key_value_results.push(QueryBenchmarkResults {
                    query_type: format!("{} (ERROR)", description),
                    total_matches: 0,
                    duration: start.elapsed(),
                    matches_per_second: 0.0,
                });
            }
            Err(_) => {
                println!("Query timed out after {} seconds!", QUERY_TIMEOUT_SECS);
                key_value_results.push(QueryBenchmarkResults {
                    query_type: format!("{} (TIMEOUT)", description),
                    total_matches: 0,
                    duration: Duration::from_secs(QUERY_TIMEOUT_SECS),
                    matches_per_second: 0.0,
                });
            }
        }
    }

    // Print comparison summary
    println!("\n{:=<80}", "");
    println!("QUERY PERFORMANCE COMPARISON SUMMARY");
    println!("{:=<80}", "");
    println!(
        "{:<30} | {:<20} | {:<20} | {:<10}",
        "Query", "Values-Only Time", "With Keys Time", "% Diff"
    );
    println!("{:-<80}", "");

    for (i, value_result) in value_only_results.iter().enumerate() {
        if i < key_value_results.len() {
            let key_result = &key_value_results[i];

            // Calculate performance difference as percentage
            let value_time = value_result.duration.as_secs_f64() * 1000.0; // ms
            let key_time = key_result.duration.as_secs_f64() * 1000.0; // ms

            let percent_diff = if value_time > 0.0 {
                ((key_time - value_time) / value_time * 100.0).round()
            } else {
                0.0
            };

            println!(
                "{:<30} | {:<20.2?} | {:<20.2?} | {:<+10.1}%",
                value_result.query_type, value_result.duration, key_result.duration, percent_diff
            );
        }
    }

    println!("\nDetailed Results:");
    println!("\nValues-Only Queries:");
    println!("{:-<50}", "");
    for result in value_only_results {
        println!(
            "{}\n\tMatches: {}\n\tDuration: {:?}\n\tMatches/sec: {:.2}",
            result.query_type, result.total_matches, result.duration, result.matches_per_second
        );
    }

    println!("\nKey-Value Queries:");
    println!("{:-<50}", "");
    for result in key_value_results {
        println!(
            "{}\n\tMatches: {}\n\tDuration: {:?}\n\tMatches/sec: {:.2}",
            result.query_type, result.total_matches, result.duration, result.matches_per_second
        );
    }
    Ok(())
}

fn generate_test_rooms(count: usize) -> Vec<Room> {
    let mut rng = thread_rng();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    (0..count)
        .map(|i| {
            let capacity = rng.gen_range(2..=20);
            Room {
                id: i as u64,
                name: generate_random_string(15),
                owner: rng.gen_range(1..1000),
                style: if rng.gen_bool(0.5) {
                    MatchStyle::Team
                } else {
                    MatchStyle::Dm
                },
                capacity,
                player_count: rng.gen_range(0..=capacity),
                is_private: rng.gen_bool(0.2),
                created_at: now - rng.gen_range(0..30 * 24 * 60 * 60),
                tags: (0..rng.gen_range(0..5))
                    .map(|_| generate_random_string(5))
                    .collect(),
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./query_bench_data";
    let column_families = vec!["rooms"];
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.increase_parallelism(num_cpus::get() as i32);

    println!("Initializing database at {}", path);
    let db = Arc::new(RocksDB::open_cf(&opts, path, column_families)?);

    for size in [1000, 20_000].iter() {
        println!("\nTesting with {} rooms", size);
        println!("{:-<50}", "");

        let benchmark = async {
            if let Err(e) = run_query_benchmark(db.clone(), *size).await {
                println!("Benchmark failed: {:?}", e);
            }
        };

        match tokio::time::timeout(Duration::from_secs(120), benchmark).await {
            Ok(_) => println!("Benchmark completed successfully"),
            Err(_) => println!("Benchmark timed out after 120 seconds!"),
        }
    }

    println!("\nRemoving test database...");
    std::fs::remove_dir_all(path)?;

    Ok(())
}

fn generate_random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
