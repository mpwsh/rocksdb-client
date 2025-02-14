use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rocksdb_client::{KVStore, KvStoreError, Options, RocksDB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum MatchStyle {
    Team,
    Dm,
}
// Add some more fields to make queries interesting
#[derive(Serialize, Deserialize, Debug)]
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
    // First, generate and insert test data
    println!("Generating {} test rooms...", num_rooms);
    let rooms = generate_test_rooms(num_rooms);

    println!("Inserting rooms into database...");
    let insert_start = Instant::now();
    for room in &rooms {
        db.insert_cf("rooms", &room.id.to_string(), room)?;
    }
    println!("Insertion took: {:?}", insert_start.elapsed());

    // Define our test queries
    let queries = vec![
        ("Find rooms with id = 5", r#"{"==": [{"var": "id"}, 5]}"#),
        ("Find Team rooms", r#"{"==": [{"var": "style"}, "Team"]}"#),
        (
            "Find available Team rooms",
            r#"{
                "and": [
                    {"==": [{"var": "style"}, "Team"]},
                    {"<": [{"var": "player_count"}, {"var": "capacity"}]}
                ]
            }"#,
        ),
        (
            "Find popular rooms",
            r#"{
                "and": [
                    {">": [{"var": "player_count"}, 5]},
                    {"==": [{"var": "is_private"}, false]}
                ]
            }"#,
        ),
        (
            "Find empty public rooms",
            r#"{
                "and": [
                    {"==": [{"var": "player_count"}, 0]},
                    {"==": [{"var": "is_private"}, false]}
                ]
            }"#,
        ),
    ];

    let mut results = Vec::new();

    // Run each query and measure performance
    for (description, query) in queries {
        println!("\nRunning query: {}", description);
        let start = Instant::now();

        let matching_rooms: Vec<Room> = db.query_cf("rooms", query)?;
        let duration = start.elapsed();

        let result = QueryBenchmarkResults {
            query_type: description.to_string(),
            total_matches: matching_rooms.len(),
            duration,
            matches_per_second: matching_rooms.len() as f64 / duration.as_secs_f64(),
        };

        println!("Results: {:#?}", result);
        results.push(result);
    }

    // Print summary
    println!("\nQuery Benchmark Summary:");
    println!("{:-<50}", "");
    for result in results {
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
                created_at: now - rng.gen_range(0..30 * 24 * 60 * 60), // Random time in last 30 days
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

    // Performance options
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.increase_parallelism(num_cpus::get() as i32);

    println!("Initializing database at {}", path);
    let db = Arc::new(RocksDB::open_cf(&opts, path, column_families)?);

    // Test with different dataset sizes
    for size in [1000, 10_000, 100_000].iter() {
        println!("\nTesting with {} rooms", size);
        println!("{:-<50}", "");

        if let Err(e) = run_query_benchmark(db.clone(), *size).await {
            println!("Benchmark failed: {:?}", e);
        }
    }

    // Cleanup
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
