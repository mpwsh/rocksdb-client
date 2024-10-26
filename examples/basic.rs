use std::time::{SystemTime, UNIX_EPOCH};

use rocksdb_client::{KVStore, KvStoreError, Options, RocksDB};
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database with column families
    let path = "./data";
    let column_families = vec!["users", "settings", "rooms"];
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Try to open with default CFs first, if it fails use existing CFs
    let db = match RocksDB::open_cf(&opts, path, column_families) {
        Ok(db) => db,
        Err(_) => RocksDB::open_with_existing_cfs(&opts, path)?,
    };

    // List existing column families
    println!("Current column families:");
    let cfs = RocksDB::list_cf(path)?;
    for cf in &cfs {
        println!("- {}", cf);
    }

    // Insert sample data
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

    // Insert users individually
    for user in &users {
        db.insert(&user.id.to_string(), user)?;
    }

    // Insert rooms and settings using column families
    for room in &rooms {
        db.insert_cf("rooms", &room.id.to_string(), room)?;
    }

    for setting in &settings {
        db.insert_cf("settings", &setting.id.to_string(), setting)?;
    }

    // Create a new column family during runtime
    println!("\nCreating new column family 'archived_rooms'");
    db.create_cf("archived_rooms")?;

    // Add data to the new column family
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

    // Implement room filtering by style
    println!("\nFiltering rooms by style:");
    let filtered_rooms = filter_rooms_by_style(&db, MatchStyle::Team)?;
    for room in filtered_rooms {
        println!("Team room: {:?}", room);
    }

    // Demonstrate range queries
    println!("\nRooms within ID range 1-2:");
    let range_rooms: Vec<Room> = db.get_range_cf("rooms", "1", "2")?;
    for room in range_rooms {
        println!("Room in range: {:?}", room);
    }

    // Print all entries in each column family
    print_cf_contents(&db)?;

    Ok(())
}

// Helper function to filter rooms by style
fn filter_rooms_by_style(db: &RocksDB, style: MatchStyle) -> Result<Vec<Room>, KvStoreError> {
    let rooms: Vec<Room> = db.get_range_cf("rooms", "0", "999")?;
    Ok(rooms
        .into_iter()
        .filter(|room| room.style == style)
        .collect())
}

// Helper function to print contents of all column families
fn print_cf_contents(db: &RocksDB) -> Result<(), KvStoreError> {
    println!("\nContents of all column families:");

    // Print users (from default CF)
    println!("\nUsers:");
    for id in 1..=3 {
        match db.get::<User>(&id.to_string()) {
            Ok(user) => println!("- {:?}", user),
            Err(KvStoreError::KeyNotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    // Print rooms
    println!("\nRooms:");
    let rooms: Vec<Room> = db.get_range_cf("rooms", "0", "999")?;
    for room in rooms {
        println!("- {:?}", room);
    }

    // Print settings
    println!("\nSettings:");
    let settings: Vec<Settings> = db.get_range_cf("settings", "0", "999")?;
    for setting in settings {
        println!("- {:?}", setting);
    }

    // Print archived rooms
    println!("\nArchived Rooms:");
    let archived_rooms: Vec<Room> = db.get_range_cf("archived_rooms", "0", "999")?;
    for room in archived_rooms {
        println!("- {:?}", room);
    }

    Ok(())
}
