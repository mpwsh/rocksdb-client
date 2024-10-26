use futures::future::join_all;
use rand::seq::IteratorRandom;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rocksdb_client::{KVStore, KvStoreError, Options, RocksDB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    id: u64,
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Room {
    id: u64,
    name: String,
    owner: u64,
    style: MatchStyle,
}

#[derive(Serialize, Deserialize, Debug)]
struct Settings {
    id: u64,
    color: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum MatchStyle {
    Team,
    Dm,
}

#[derive(Debug)]
struct BenchmarkResults {
    total_operations: usize,
    successful_operations: usize,
    failed_operations: usize,
    total_duration: Duration,
    operations_per_second: f64,
    avg_operation_time: Duration,
    total_bytes_written: usize,
    write_throughput_mb_sec: f64,
}

#[derive(Debug)]
struct OperationMetrics {
    total: usize,
    successful: usize,
    failed: usize,
}

struct TestData {
    user: User,
    room: Room,
    settings: Settings,
}

async fn calculate_data_size(data: &TestData) -> usize {
    // Calculate serialized size of the data
    let user_size = serde_json::to_vec(&data.user).map(|v| v.len()).unwrap_or(0);
    let room_size = serde_json::to_vec(&data.room).map(|v| v.len()).unwrap_or(0);
    let settings_size = serde_json::to_vec(&data.settings)
        .map(|v| v.len())
        .unwrap_or(0);

    // Include key sizes
    let key_sizes = data.user.id.to_string().len()
        + data.room.id.to_string().len()
        + data.settings.id.to_string().len();

    user_size + room_size + settings_size + key_sizes
}
async fn run_benchmark(
    db: Arc<RocksDB>,
    num_users: usize,
) -> Result<BenchmarkResults, KvStoreError> {
    let start = Instant::now();
    let mut handles = Vec::new();

    // Generate random data once and store in memory
    let test_data = generate_test_data(num_users);

    // Calculate total data size before starting
    let mut total_bytes = 0;
    for data in &test_data {
        total_bytes += calculate_data_size(data).await;
    }

    // Simulate concurrent users
    for user_batch in test_data.into_iter().enumerate() {
        let db = db.clone();
        let handle = tokio::spawn(async move {
            let op_start = Instant::now();
            let results = perform_user_operations(db, user_batch.1).await?;
            let op_duration = op_start.elapsed();
            Ok::<(OperationMetrics, Duration), KvStoreError>((results, op_duration))
        });
        handles.push(handle);
    }

    let results: Vec<Result<(OperationMetrics, Duration), _>> = join_all(handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let total_duration = start.elapsed();

    let mut total_metrics = BenchmarkResults {
        total_operations: 0,
        successful_operations: 0,
        failed_operations: 0,
        total_duration,
        operations_per_second: 0.0,
        avg_operation_time: Duration::new(0, 0),
        total_bytes_written: total_bytes,
        write_throughput_mb_sec: 0.0,
    };

    let mut total_operation_time = Duration::new(0, 0);

    for result in results {
        match result {
            Ok((metrics, duration)) => {
                total_metrics.total_operations += metrics.total;
                total_metrics.successful_operations += metrics.successful;
                total_metrics.failed_operations += metrics.failed;
                total_operation_time += duration;
            }
            Err(_) => total_metrics.failed_operations += 1,
        }
    }

    total_metrics.operations_per_second =
        total_metrics.successful_operations as f64 / total_duration.as_secs_f64();

    if total_metrics.total_operations > 0 {
        total_metrics.avg_operation_time =
            total_operation_time / total_metrics.total_operations as u32;
    }

    // Calculate write throughput in MB/s
    let mb_written = total_metrics.total_bytes_written as f64 / (1024.0 * 1024.0);
    total_metrics.write_throughput_mb_sec = mb_written / total_duration.as_secs_f64();

    Ok(total_metrics)
}

fn generate_random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn generate_test_data(count: usize) -> Vec<TestData> {
    (0..count)
        .map(|i| TestData {
            user: User {
                id: i as u64,
                name: generate_random_string(10),
            },
            room: Room {
                id: i as u64,
                name: generate_random_string(15),
                owner: i as u64,
                style: if i % 2 == 0 {
                    MatchStyle::Team
                } else {
                    MatchStyle::Dm
                },
            },
            settings: Settings {
                id: i as u64,
                color: generate_random_string(6),
            },
        })
        .collect()
}

async fn perform_user_operations(
    db: Arc<RocksDB>,
    data: TestData,
) -> Result<OperationMetrics, KvStoreError> {
    let mut metrics = OperationMetrics {
        total: 0,
        successful: 0,
        failed: 0,
    };

    // Create operations
    metrics.total += 1;
    if db
        .insert_cf("users", &data.user.id.to_string(), &data.user)
        .is_ok()
    {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    metrics.total += 1;
    if db
        .insert_cf("rooms", &data.room.id.to_string(), &data.room)
        .is_ok()
    {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    metrics.total += 1;
    if db
        .insert_cf("settings", &data.settings.id.to_string(), &data.settings)
        .is_ok()
    {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    // Read operations
    metrics.total += 1;
    if db
        .get_cf::<User>("users", &data.user.id.to_string())
        .is_ok()
    {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    metrics.total += 1;
    if db
        .get_cf::<Room>("rooms", &data.room.id.to_string())
        .is_ok()
    {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    metrics.total += 1;
    if db
        .get_cf::<Settings>("settings", &data.settings.id.to_string())
        .is_ok()
    {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    // Update operations
    metrics.total += 1;
    let updated_user = User {
        id: data.user.id,
        name: generate_random_string(10),
    };
    if db
        .insert_cf("users", &data.user.id.to_string(), &updated_user)
        .is_ok()
    {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    metrics.total += 1;
    let updated_settings = Settings {
        id: data.settings.id,
        color: format!("#{}", generate_random_string(6)),
    };
    if db
        .insert_cf("settings", &data.settings.id.to_string(), &updated_settings)
        .is_ok()
    {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    // Delete operations
    metrics.total += 1;
    if db.delete(&data.user.id.to_string()).is_ok() {
        metrics.successful += 1;
    } else {
        metrics.failed += 1;
    }

    metrics.total += 1;
    if db.cf_exists("rooms") {
        if db.delete(&data.room.id.to_string()).is_ok() {
            metrics.successful += 1;
        } else {
            metrics.failed += 1;
        }
    } else {
        metrics.successful += 1;
    }

    Ok(metrics)
}

async fn validate_data(db: &RocksDB, num_users: usize) -> ValidationResults {
    let mut results = ValidationResults {
        users_found: 0,
        rooms_found: 0,
        settings_found: 0,
        users_missing: 0,
        rooms_missing: 0,
        settings_missing: 0,
    };

    let expected_keys: Vec<u64> = (0..num_users as u64).collect();

    // Check random sample for larger datasets
    let keys_to_check = if num_users > 1000 {
        let mut rng = thread_rng();
        expected_keys
            .iter()
            .choose_multiple(&mut rng, 1000)
            .into_iter()
            .copied()
            .collect::<Vec<_>>()
    } else {
        expected_keys
    };

    println!("\nValidating {} random keys...", keys_to_check.len());

    for key in &keys_to_check {
        // Check users
        match db.get_cf::<User>("users", &key.to_string()) {
            Ok(_) => results.users_found += 1,
            Err(_) => results.users_missing += 1,
        }

        // Check rooms
        match db.get_cf::<Room>("rooms", &key.to_string()) {
            Ok(_) => results.rooms_found += 1,
            Err(_) => results.rooms_missing += 1,
        }

        // Check settings
        match db.get_cf::<Settings>("settings", &key.to_string()) {
            Ok(_) => results.settings_found += 1,
            Err(_) => results.settings_missing += 1,
        }
    }

    results
}

#[derive(Debug)]
struct ValidationResults {
    users_found: usize,
    rooms_found: usize,
    settings_found: usize,
    users_missing: usize,
    rooms_missing: usize,
    settings_missing: usize,
}

impl ValidationResults {
    fn total_found(&self) -> usize {
        self.users_found + self.rooms_found + self.settings_found
    }

    fn total_missing(&self) -> usize {
        self.users_missing + self.rooms_missing + self.settings_missing
    }

    fn success_rate(&self) -> f64 {
        self.total_found() as f64 / (self.total_found() + self.total_missing()) as f64 * 100.0
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./bench_data";
    let column_families = vec!["users", "settings", "rooms"];
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Performance options
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(3);
    opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.set_level_zero_file_num_compaction_trigger(4);
    opts.increase_parallelism(num_cpus::get() as i32);

    // Since we can't use set_fsync, we'll rely on RocksDB's default durability

    println!("Initializing database at {}", path);
    let db = RocksDB::open_cf(&opts, path, column_families)?;
    let db = Arc::new(db);

    let iterations = vec![100, 500, 2000, 50000];
    for num_users in iterations {
        println!(
            "\nStarting benchmark with {} concurrent users...",
            num_users
        );
        let start = Instant::now();

        match run_benchmark(db.clone(), num_users).await {
            Ok(results) => {
                let total_time = start.elapsed();
                println!("\nBenchmark Results for {} users:", num_users);
                println!("Total operations: {}", results.total_operations);
                println!("Successful operations: {}", results.successful_operations);
                println!("Failed operations: {}", results.failed_operations);
                println!("Total duration: {:?}", results.total_duration);
                println!("Overall time (including setup): {:?}", total_time);
                println!(
                    "Operations per second: {:.2}",
                    results.operations_per_second
                );
                println!("Average operation time: {:?}", results.avg_operation_time);
                println!(
                    "Total data written: {:.2} MB",
                    results.total_bytes_written as f64 / (1024.0 * 1024.0)
                );
                println!(
                    "Write throughput: {:.2} MB/s",
                    results.write_throughput_mb_sec
                );

                // Show current disk usage
                if let Ok(entries) = std::fs::read_dir(path) {
                    let total_size: u64 = entries
                        .filter_map(Result::ok)
                        .filter_map(|entry| entry.metadata().ok())
                        .map(|metadata| metadata.len())
                        .sum();
                    println!(
                        "\nCurrent database size on disk: {:.2} MB",
                        total_size as f64 / (1024.0 * 1024.0)
                    );

                    // Calculate disk write speed
                    let disk_write_speed =
                        total_size as f64 / (1024.0 * 1024.0) / total_time.as_secs_f64();
                    println!("Actual disk write speed: {:.2} MB/s", disk_write_speed);
                }

                // Validate the data with more detailed results
                println!("\nValidating written data...");
                let validation = validate_data(&db, num_users).await;
                println!("\nDetailed Validation Results:");
                println!(
                    "Users  - Found: {}, Missing: {}",
                    validation.users_found, validation.users_missing
                );
                println!(
                    "Rooms  - Found: {}, Missing: {}",
                    validation.rooms_found, validation.rooms_missing
                );
                println!(
                    "Settings - Found: {}, Missing: {}",
                    validation.settings_found, validation.settings_missing
                );
                println!("Overall success rate: {:.1}%", validation.success_rate());

                println!("\n{}", "-".repeat(50));
            }
            Err(e) => println!("Benchmark failed: {:?}", e),
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Print final database stats
    println!("\nFinal Database Statistics:");
    println!("Database location: {}", path);
    if let Ok(entries) = std::fs::read_dir(path) {
        let total_size: u64 = entries
            .filter_map(Result::ok)
            .filter_map(|entry| entry.metadata().ok())
            .map(|metadata| metadata.len())
            .sum();
        println!(
            "Final database size: {:.2} MB",
            total_size as f64 / (1024.0 * 1024.0)
        );

        println!("\nDatabase files:");
        for entry in std::fs::read_dir(path)?.filter_map(Result::ok) {
            if let Ok(metadata) = entry.metadata() {
                println!(
                    "{}: {:.2} MB",
                    entry.file_name().to_string_lossy(),
                    metadata.len() as f64 / (1024.0 * 1024.0)
                );
            }
        }
    }

    println!("\nDo you want to remove the database? (press Enter to continue)");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;

    std::fs::remove_dir_all(path).ok();
    println!("Database removed.");

    Ok(())
}
