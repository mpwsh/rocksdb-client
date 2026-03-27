use std::time::{SystemTime, UNIX_EPOCH};

use rocksdb_client::{DBCompressionType, Direction, KvStoreError, Options, RocksDB};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: u64,
    name: String,
    created_at: i64,
}

impl User {
    fn new(id: u64, name: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Room {
    id: u64,
    name: String,
    owner: u64,
    style: MatchStyle,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Settings {
    id: u64,
    color: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
enum MatchStyle {
    Team,
    Dm,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./data";
    let column_families = vec!["users", "settings", "rooms"];
    let mut opts = RocksDB::default_opts();
    opts.set_compression_type(DBCompressionType::None);

    let db = match RocksDB::open_cf(&opts, path, column_families) {
        Ok(db) => db,
        Err(_) => RocksDB::open_with_existing_cfs(&opts, path)?,
    };

    println!("Current column families:");
    let cfs = RocksDB::list_cf(path)?;
    for cf in &cfs {
        println!("- {}", cf);
    }

    let users = vec![
        User::new(1, "Alice"),
        User::new(2, "Charlie"),
        User::new(3, "Bob"),
    ];

    let rooms = vec![
        Room {
            id: 1,
            name: "Team Alpha".to_string(),
            owner: 1,
            style: MatchStyle::Team,
        },
        Room {
            id: 2,
            name: "DM Room".to_string(),
            owner: 2,
            style: MatchStyle::Dm,
        },
        Room {
            id: 3,
            name: "Team Beta".to_string(),
            owner: 3,
            style: MatchStyle::Team,
        },
    ];

    let settings = vec![
        Settings {
            id: 1,
            color: "blue".to_string(),
        },
        Settings {
            id: 2,
            color: "red".to_string(),
        },
    ];

    for user in &users {
        db.insert(&user.id.to_string(), user)?;
    }
    for room in &rooms {
        db.insert_cf("rooms", &room.id.to_string(), room)?;
    }
    for setting in &settings {
        db.insert_cf("settings", &setting.id.to_string(), setting)?;
    }

    println!("\nCreating new column family 'archived_rooms'");
    db.create_cf("archived_rooms")?;

    let archived_room = Room {
        id: 999,
        name: "Old Team".to_string(),
        owner: 1,
        style: MatchStyle::Team,
    };
    db.insert_cf(
        "archived_rooms",
        &archived_room.id.to_string(),
        &archived_room,
    )?;

    println!("\nFiltering rooms by style:");
    let filtered_rooms = filter_rooms_by_style(&db, MatchStyle::Team)?;
    for room in filtered_rooms {
        println!("Team room: {}", room);
    }

    println!("\nRooms within ID range 1-2:");
    let range_rooms = db.get_range_cf("rooms", "1", "2", 1000, Direction::Forward, false)?;
    for room in range_rooms {
        println!("Room in range: {}", room);
    }

    print_cf_contents(&db)?;

    println!("\n=== Creating backup of rooms collection ===");
    db.create_backup("rooms", "rooms_backup.sst")?;
    println!("Backup created successfully");

    println!("\n=== Dropping rooms collection ===");
    db.drop_cf("rooms")?;
    println!("Collection dropped");

    db.create_cf("rooms")?;

    println!("\n=== State after dropping rooms ===");
    print_cf_contents(&db)?;

    println!("\n=== Restoring rooms from backup ===");
    db.restore_backup("rooms", "rooms_backup.sst")?;
    println!("Restore completed");

    println!("\n=== Final State after restore ===");
    print_cf_contents(&db)?;

    Ok(())
}

fn filter_rooms_by_style(
    db: &RocksDB,
    style: MatchStyle,
) -> Result<Vec<serde_json::Value>, KvStoreError> {
    let rooms = db.get_range_cf("rooms", "0", "999", 1000, Direction::Forward, false)?;
    Ok(rooms
        .into_iter()
        .filter(|room| {
            room.get("style").and_then(|s| s.as_str())
                == Some(if style == MatchStyle::Team {
                    "Team"
                } else {
                    "Dm"
                })
        })
        .collect())
}

fn print_cf_contents(db: &RocksDB) -> Result<(), KvStoreError> {
    println!("\nContents and sizes of all column families:");

    let print_cf_size = |cf: &str| -> Result<(), KvStoreError> {
        let size = db.get_cf_size(cf)?;
        println!("\n{} Size:", cf);
        println!("  Total: {:.2} MB", size.total_mb());
        println!("  SST Files: {} bytes", size.sst_bytes);
        println!("  Memtable: {} bytes", size.mem_table_bytes);
        println!("  Blob Files: {} bytes", size.blob_bytes);
        Ok(())
    };

    println!("\nUsers:");
    print_cf_size("default")?;
    for id in 1..=3 {
        match db.get::<User>(&id.to_string()) {
            Ok(user) => println!("- {:?}", user),
            Err(KvStoreError::KeyNotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    println!("\nRooms:");
    print_cf_size("rooms")?;
    let rooms = db.get_range_cf("rooms", "0", "999", 1000, Direction::Forward, false)?;
    for room in rooms {
        println!("- {}", room);
    }

    println!("\nSettings:");
    print_cf_size("settings")?;
    let settings = db.get_range_cf("settings", "0", "999", 1000, Direction::Forward, false)?;
    for setting in settings {
        println!("- {}", setting);
    }

    println!("\nArchived Rooms:");
    print_cf_size("archived_rooms")?;
    let archived = db.get_range_cf(
        "archived_rooms",
        "0",
        "999",
        1000,
        Direction::Forward,
        false,
    )?;
    for room in archived {
        println!("- {}", room);
    }

    Ok(())
}
