use futures::future::join_all;
use rand::{thread_rng, Rng};
use rocksdb_client::{KVStore, Options, RocksDB};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

    let mut nested = Vec::new();
    for _ in 0..3 {
        let mut field3 = HashMap::new();
        for i in 0..3 {
            field3.insert(format!("f_{}", i), rng.gen::<f64>());
        }

        nested.push(NestedData {
            field1: format!("nested_{}", rng.gen::<u32>()),
            field2: if rng.gen_bool(0.5) {
                Some(vec![rng.gen(), rng.gen(), rng.gen()])
            } else {
                None
            },
            field3,
        });
    }

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
    let keys: Vec<String> = (0..1000).map(|i| format!("key:{}", i)).collect();
    let keys = Arc::new(keys);
    let start = Instant::now();
    let mut read_latencies = Vec::new();
    let mut write_latencies = Vec::new();
    let mut total_bytes = 0;

    let mut handles = Vec::new();

    for op_num in 0..total_ops {
        let db = db.clone();
        let keys = keys.clone();

        let handle = tokio::spawn(async move {
            let op_start = Instant::now();

            let is_read = (op_num % 100) < (read_ratio * 100.0) as usize;
            let key_idx = op_num % keys.len();
            let key = &keys[key_idx];

            let (latency, bytes) = if is_read {
                let res: Option<ComplexTestData> = db.get_cf("test", key).ok();
                let bytes = res.map(|d| d.payload.len()).unwrap_or(0);
                (op_start.elapsed(), bytes)
            } else {
                let data = generate_complex_data(key);
                let bytes = data.payload.len();
                let _ = db.insert_cf("test", key, &data);
                (op_start.elapsed(), bytes)
            };

            (is_read, latency, bytes)
        });

        handles.push(handle);
    }

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
    let avg_duration = all_results
        .iter()
        .map(|r| r.total_duration.as_secs_f64())
        .sum::<f64>()
        / iterations as f64;

    let mut all_read_latencies = Vec::new();
    let mut all_write_latencies = Vec::new();
    for r in &all_results {
        all_read_latencies.extend(r.read_latencies.iter());
        all_write_latencies.extend(r.write_latencies.iter());
    }

    let process_latencies = |latencies: &[Duration]| {
        let mut ms: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
        ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        (
            ms[ms.len() * 50 / 100],
            ms[ms.len() * 95 / 100],
            ms[ms.len() * 99 / 100],
        )
    };

    let (r_p50, r_p95, r_p99) = process_latencies(&all_read_latencies);
    let (w_p50, w_p95, w_p99) = process_latencies(&all_write_latencies);

    println!("\nAggregated Results ({} iterations):", iterations);
    println!("Average throughput: {:.2} ops/sec", avg_throughput);
    println!("Average duration per iteration: {:.2}s", avg_duration);

    println!("\nRead Latencies (across all iterations):");
    println!("  p50: {:.2}ms", r_p50);
    println!("  p95: {:.2}ms", r_p95);
    println!("  p99: {:.2}ms", r_p99);

    println!("\nWrite Latencies (across all iterations):");
    println!("  p50: {:.2}ms", w_p50);
    println!("  p95: {:.2}ms", w_p95);
    println!("  p99: {:.2}ms", w_p99);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.increase_parallelism(num_cpus::get() as i32);

    let db = Arc::new(RocksDB::open_cf(&opts, "./bench_db", vec!["test"])?);

    run_benchmark_suite(db.clone()).await?;

    std::fs::remove_dir_all("./bench_db").ok();
    Ok(())
}
