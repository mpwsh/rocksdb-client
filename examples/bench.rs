use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::join_all;
use rand::{thread_rng, Rng};
use rocksdb_client::{Options, RocksDB};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ComplexTestData {
    id: String,
    metadata: HashMap<String, String>,
    nested: Vec<NestedData>,
    timestamps: Vec<i64>,
    flags: HashSet<String>,
    payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct NestedData {
    field1: String,
    field2: Option<Vec<i32>>,
    field3: HashMap<String, f64>,
}

fn generate_complex_data(id: &str) -> ComplexTestData {
    let mut rng = thread_rng();
    let mut metadata = HashMap::new();
    for i in 0..5 {
        metadata.insert(
            format!("meta_key_{}", i),
            format!("meta_value_{}", rng.gen::<u32>()),
        );
    }
    let nested = (0..3)
        .map(|_| {
            let mut field3 = HashMap::new();
            for i in 0..3 {
                field3.insert(format!("f_{}", i), rng.gen::<f64>());
            }
            NestedData {
                field1: format!("nested_{}", rng.gen::<u32>()),
                field2: if rng.gen_bool(0.5) {
                    Some(vec![rng.gen(), rng.gen(), rng.gen()])
                } else {
                    None
                },
                field3,
            }
        })
        .collect();
    let timestamps = (0..5).map(|_| rng.gen::<i64>()).collect();
    let mut flags = HashSet::new();
    for i in 0..3 {
        flags.insert(format!("flag_{}", i));
    }
    ComplexTestData {
        id: id.to_string(),
        metadata,
        nested,
        timestamps,
        flags,
        payload: vec![0u8; rng.gen_range(100..2000)],
    }
}

#[derive(Debug)]
struct BenchResults {
    total_duration: Duration,
    throughput: f64,
    read_latencies: Vec<Duration>,
    write_latencies: Vec<Duration>,
    total_bytes: usize,
}

async fn run_mixed_workload(db: Arc<RocksDB>, total_ops: usize, read_ratio: f64) -> BenchResults {
    let keys: Arc<Vec<String>> = Arc::new((0..1000).map(|i| format!("key:{}", i)).collect());
    let start = Instant::now();
    let mut read_latencies = Vec::new();
    let mut write_latencies = Vec::new();
    let mut total_bytes = 0;

    let handles: Vec<_> = (0..total_ops)
        .map(|op_num| {
            let db = db.clone();
            let keys = keys.clone();
            tokio::spawn(async move {
                let op_start = Instant::now();
                let is_read = (op_num % 100) < (read_ratio * 100.0) as usize;
                let key = &keys[op_num % keys.len()];
                let (latency, bytes) = if is_read {
                    let res: Option<ComplexTestData> = db.get_cf("test", key).ok();
                    (
                        op_start.elapsed(),
                        res.map(|d| d.payload.len()).unwrap_or(0),
                    )
                } else {
                    let data = generate_complex_data(key);
                    let bytes = data.payload.len();
                    let _ = db.insert_cf("test", key, &data);
                    (op_start.elapsed(), bytes)
                };
                (is_read, latency, bytes)
            })
        })
        .collect();

    for (is_read, latency, bytes) in join_all(handles).await.into_iter().flatten() {
        if is_read {
            read_latencies.push(latency);
        } else {
            write_latencies.push(latency);
        }
        total_bytes += bytes;
    }

    BenchResults {
        total_duration: start.elapsed(),
        throughput: total_ops as f64 / start.elapsed().as_secs_f64(),
        read_latencies,
        write_latencies,
        total_bytes,
    }
}

async fn run_benchmark_suite(db: Arc<RocksDB>) -> Result<(), Box<dyn std::error::Error>> {
    let ops = 100_000;
    let read_ratio = 0.7;
    let iterations = 5;

    println!("Running warmup...");
    let _ = run_mixed_workload(db.clone(), ops / 10, read_ratio).await;

    println!("\nStarting benchmark ({} iterations)...", iterations);
    let mut all_results = Vec::new();

    for i in 1..=iterations {
        println!("\nIteration {}/{}:", i, iterations);
        let results = run_mixed_workload(db.clone(), ops, read_ratio).await;
        println!("Throughput: {:.2} ops/sec", results.throughput);
        println!(
            "Data throughput: {:.2} MB/s",
            (results.total_bytes as f64 / 1024.0 / 1024.0) / results.total_duration.as_secs_f64()
        );
        all_results.push(results);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let avg_throughput: f64 =
        all_results.iter().map(|r| r.throughput).sum::<f64>() / iterations as f64;
    println!("\nAverage throughput: {:.2} ops/sec", avg_throughput);

    let mut all_read: Vec<Duration> = all_results
        .iter()
        .flat_map(|r| r.read_latencies.iter().copied())
        .collect();
    let mut all_write: Vec<Duration> = all_results
        .iter()
        .flat_map(|r| r.write_latencies.iter().copied())
        .collect();

    let percentiles = |latencies: &mut Vec<Duration>| {
        let ms: Vec<f64> = {
            latencies.sort();
            latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect()
        };
        (
            ms[ms.len() * 50 / 100],
            ms[ms.len() * 95 / 100],
            ms[ms.len() * 99 / 100],
        )
    };

    let (r50, r95, r99) = percentiles(&mut all_read);
    let (w50, w95, w99) = percentiles(&mut all_write);
    println!(
        "\nRead  p50: {:.2}ms  p95: {:.2}ms  p99: {:.2}ms",
        r50, r95, r99
    );
    println!(
        "Write p50: {:.2}ms  p95: {:.2}ms  p99: {:.2}ms",
        w50, w95, w99
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.increase_parallelism(
        std::thread::available_parallelism()
            .map(|p| p.get() as i32)
            .unwrap_or(4),
    );

    let db = Arc::new(RocksDB::open_cf(&opts, "./bench_db", vec!["test"])?);
    run_benchmark_suite(db).await?;
    std::fs::remove_dir_all("./bench_db").ok();
    Ok(())
}
