use rand::{thread_rng, Rng};
use rocksdb_client::{Direction, KVStore, KvStoreError, Options, RocksDB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Position {
    map_id: u32,
    x: f32,
    y: f32,
    z: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PlayerUpdate {
    position: Position,
    health: f32,
    mana: f32,
    timestamp: i64,
}

async fn run_mmo_benchmark(
    db: Arc<RocksDB>,
    num_players: usize,
    max_latency: Duration,
) -> Result<(), KvStoreError> {
    let mut rng = thread_rng();
    println!("Starting benchmark with {} players", num_players);

    // Clean up previous data
    println!("\nCleaning up previous data...");
    db.drop_cf("player_updates")?;
    db.drop_cf("player_positions")?;
    db.drop_cf("player_history")?;
    db.create_cf("player_updates")?;
    db.create_cf("player_positions")?;
    db.create_cf("player_history")?;

    // Test 1: Insert player updates
    println!("\nInserting player updates...");
    let start = Instant::now();

    for player_id in 0..num_players {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let pos = Position {
            map_id: rng.gen_range(1..=5),
            x: rng.gen_range(-1000.0..1000.0),
            y: rng.gen_range(-1000.0..1000.0),
            z: rng.gen_range(-100.0..100.0),
        };

        let update = PlayerUpdate {
            position: pos.clone(),
            health: rng.gen_range(0.0..100.0),
            mana: rng.gen_range(0.0..100.0),
            timestamp: now,
        };

        // Store update in player_updates
        let update_key = format!("player:{}:update:{}", player_id, now);
        db.insert_cf("player_updates", &update_key, &update)?;

        // Store position for quick map lookups
        let position_key = format!("map:{}:player:{}", pos.map_id, player_id);
        db.insert_cf("player_positions", &position_key, &pos)?;

        // Move older updates to history if necessary
        let prefix = format!("player:{}:update:", player_id);
        let updates: Vec<PlayerUpdate> = db.get_range_cf(
            "player_updates",
            &prefix,
            &format!("{}∞", prefix),
            11, // Fetch 11 updates to check if we need to trim
            Direction::Reverse,
            false,
        )?;

        if updates.len() > 10 {
            let oldest_update = updates.last().unwrap();
            let oldest_key = format!("player:{}:update:{}", player_id, oldest_update.timestamp);
            db.delete_cf("player_updates", &oldest_key)?;
            db.insert_cf("player_history", &oldest_key, oldest_update)?;
        }
    }

    let insert_duration = start.elapsed();
    println!("Inserts took: {:?}", insert_duration);

    // Test 2: Get latest updates for a player
    println!("\nFetching latest updates for random players...");
    let start = Instant::now();

    for _ in 0..num_players {
        let player_id = rng.gen_range(0..num_players);
        let prefix = format!("player:{}:update:", player_id);

        let updates: Vec<PlayerUpdate> = db.get_range_cf(
            "player_updates",
            &prefix,
            &format!("{}∞", prefix),
            3,
            Direction::Reverse,
            false,
        )?;
    }

    let query_duration = start.elapsed();
    if start.elapsed() > max_latency {
        println!(
            "❌ Update fetching exceeded latency threshold. {:?}",
            query_duration
        );
        return Ok(());
    }

    // Test 3: Find all players in a map
    println!("\nFinding players in maps...");
    let start = Instant::now();

    let map_id = rng.gen_range(1..=5);
    let prefix = format!("map:{}", map_id);

    let players_in_map: Vec<Position> = db.get_range_cf(
        "player_positions",
        &prefix,
        &format!("{}∞", prefix),
        1000,
        Direction::Forward,
        false,
    )?;

    let query_duration = start.elapsed();
    println!(
        "Found {} players in map {} in {:?}",
        players_in_map.len(),
        map_id,
        query_duration
    );

    if query_duration > max_latency {
        println!("❌ Map query exceeded latency threshold");
        return Ok(());
    }

    println!("✅ All operations within latency threshold");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./mmo_bench_data";
    let column_families = vec![
        "player_updates",   // Stores full updates with timestamp
        "player_positions", // Index for quick map queries
        "player_history",   // Stores older updates
    ];

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.increase_parallelism(num_cpus::get() as i32);

    println!("Initializing database at {}", path);
    let db = Arc::new(RocksDB::open_cf(&opts, path, column_families)?);

    let max_latency = Duration::from_millis(50);

    // Test with increasing player counts
    for size in [100, 200, 300, 400, 500].iter() {
        println!("\nTesting with {} players", size);
        println!("{:-<50}", "");

        if let Err(e) = run_mmo_benchmark(db.clone(), *size, max_latency).await {
            println!("Benchmark failed: {:?}", e);
            break;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
