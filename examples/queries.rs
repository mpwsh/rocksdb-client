use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rocksdb_client::{KvStoreError, Options, RocksDB};
use serde::{Deserialize, Serialize};

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
        ("Find rooms with any tags", "$[?@.tags[0]]"),
    ];

    let mut value_only_results = Vec::new();
    let mut key_value_results = Vec::new();

    for (description, query) in queries {
        println!("\n{:-<50}", "");
        println!("Query: {} - {}", description, query);

        // Values only
        let start = Instant::now();
        // Values only — use query_cf_as for typed results (no include_keys param)
        let query_result = tokio::time::timeout(Duration::from_secs(QUERY_TIMEOUT_SECS), async {
            db.query_cf_as::<Room>("rooms", query)
        })
        .await;

        match query_result {
            Ok(Ok(matches)) => {
                let duration = start.elapsed();
                println!("Values-only: {} matches in {:?}", matches.len(), duration);
                value_only_results.push(QueryBenchmarkResults {
                    query_type: description.to_string(),
                    total_matches: matches.len(),
                    duration,
                    matches_per_second: matches.len() as f64 / duration.as_secs_f64(),
                });
            }
            Ok(Err(e)) => println!("Query error: {:?}", e),
            Err(_) => println!("Timed out!"),
        }

        // With keys
        let start = Instant::now();
        let query_result = tokio::time::timeout(Duration::from_secs(QUERY_TIMEOUT_SECS), async {
            db.query_cf("rooms", query, true)
        })
        .await;

        match query_result {
            Ok(Ok(matches)) => {
                let duration = start.elapsed();
                println!("With keys: {} matches in {:?}", matches.len(), duration);
                key_value_results.push(QueryBenchmarkResults {
                    query_type: description.to_string(),
                    total_matches: matches.len(),
                    duration,
                    matches_per_second: matches.len() as f64 / duration.as_secs_f64(),
                });
            }
            Ok(Err(e)) => println!("Query error: {:?}", e),
            Err(_) => println!("Timed out!"),
        }
    }

    println!("\n{:=<80}", "");
    println!("PERFORMANCE COMPARISON");
    println!("{:=<80}", "");
    println!(
        "{:<35} | {:<15} | {:<15} | {:<10}",
        "Query", "Values-Only", "With Keys", "% Diff"
    );
    println!("{:-<80}", "");

    for (i, vr) in value_only_results.iter().enumerate() {
        if let Some(kr) = key_value_results.get(i) {
            let vt = vr.duration.as_secs_f64() * 1000.0;
            let kt = kr.duration.as_secs_f64() * 1000.0;
            let diff = if vt > 0.0 {
                ((kt - vt) / vt * 100.0).round()
            } else {
                0.0
            };
            println!(
                "{:<35} | {:<15.2?} | {:<15.2?} | {:<+10.1}%",
                vr.query_type, vr.duration, kr.duration, diff
            );
        }
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

fn generate_random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./query_bench_data";
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.increase_parallelism(
        std::thread::available_parallelism()
            .map(|p| p.get() as i32)
            .unwrap_or(4),
    );

    let db = Arc::new(RocksDB::open_cf(&opts, path, vec!["rooms"])?);

    for size in [1000, 20_000] {
        println!("\nTesting with {} rooms", size);
        let benchmark = async {
            if let Err(e) = run_query_benchmark(db.clone(), size).await {
                println!("Benchmark failed: {:?}", e);
            }
        };
        match tokio::time::timeout(Duration::from_secs(120), benchmark).await {
            Ok(_) => println!("Completed"),
            Err(_) => println!("Timed out!"),
        }
    }

    std::fs::remove_dir_all(path)?;
    Ok(())
}
