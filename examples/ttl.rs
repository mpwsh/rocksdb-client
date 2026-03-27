use std::{thread, time::Duration};

use rocksdb_client::{KvStoreError, Options, RocksDB};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Session {
    user: String,
    token: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./ttl_example_data";

    // Clean up from previous runs
    let _ = std::fs::remove_dir_all(path);

    let opts = RocksDB::default_opts();

    // Open a single database
    let db =
        RocksDB::open_cf_with_ttl(&opts, path, Vec::<String>::new(), std::time::Duration::ZERO)?;

    // Create three CFs with different TTLs in the same DB
    db.create_cf_with_ttl("fast", 3)?; // 3 second TTL
    db.create_cf_with_ttl("medium", 8)?; // 8 second TTL
    db.create_cf("permanent")?; // No TTL

    // Insert the same data into all three CFs
    let sessions = vec![
        (
            "session:alice",
            Session {
                user: "alice".to_string(),
                token: "abc".to_string(),
            },
        ),
        (
            "session:bob",
            Session {
                user: "bob".to_string(),
                token: "xyz".to_string(),
            },
        ),
        (
            "session:charlie",
            Session {
                user: "charlie".to_string(),
                token: "123".to_string(),
            },
        ),
    ];

    for (key, session) in &sessions {
        db.insert_cf("fast", key, session)?;
        db.insert_cf("medium", key, session)?;
        db.insert_cf("permanent", key, session)?;
    }

    println!("Inserted 3 sessions into each CF:");
    println!("  fast      = 3s TTL");
    println!("  medium    = 8s TTL");
    println!("  permanent = no TTL\n");

    // t=0 — all entries present
    println!("--- t=0s ---");
    print_counts(&db);

    // Wait for fast TTL to expire, then compact to trigger cleanup
    println!("\nWaiting 4 seconds...");
    thread::sleep(Duration::from_secs(4));
    compact_all(&db);
    println!("\n--- t=4s (after compaction) ---");
    print_counts(&db);

    // Wait for medium TTL to expire
    println!("\nWaiting 5 more seconds...");
    thread::sleep(Duration::from_secs(5));
    compact_all(&db);
    println!("\n--- t=9s (after compaction) ---");
    print_counts(&db);

    // Verify permanent entries are still readable
    println!("\n--- Permanent entries still intact ---");
    for (key, _) in &sessions {
        match db.get_cf::<Session>("permanent", key) {
            Ok(s) => println!("  {} => {:?}", key, s),
            Err(KvStoreError::KeyNotFound(_)) => println!("  {} => gone!", key),
            Err(e) => println!("  {} => error: {}", key, e),
        }
    }

    // Inspect with ldb
    println!("\n--- Verify on disk with ldb ---");
    println!("Fast (should be empty after compaction):");
    println!("  ldb --db={} scan --column_family=fast", path);
    println!("Medium (should be empty after compaction):");
    println!("  ldb --db={} scan --column_family=medium", path);
    println!("Permanent (should have all 3):");
    println!("  ldb --db={} scan --column_family=permanent", path);
    println!("\nCleanup: rm -rf {}", path);

    Ok(())
}

fn count_keys(db: &RocksDB, cf: &str) -> usize {
    db.get_range_cf(
        cf,
        "",
        "\u{ffff}",
        100_000,
        rocksdb_client::Direction::Forward,
        false,
    )
    .map(|v| v.len())
    .unwrap_or(0)
}

fn print_counts(db: &RocksDB) {
    println!("  fast (3s TTL):    {} sessions", count_keys(db, "fast"));
    println!("  medium (8s TTL):  {} sessions", count_keys(db, "medium"));
    println!(
        "  permanent:        {} sessions",
        count_keys(db, "permanent")
    );
}

fn compact_all(db: &RocksDB) {
    let _ = db.compact_cf("fast");
    let _ = db.compact_cf("medium");
    let _ = db.compact_cf("permanent");
    println!("  (compacted all)");
}
