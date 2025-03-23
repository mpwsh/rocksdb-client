use rocksdb_client::{Direction, KVStore, KvStoreError, Options, RocksDB};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

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
    let range_rooms: Vec<Room> =
        db.get_range_cf("rooms", "1", "2", 1000, Direction::Forward, false)?;
    for room in range_rooms {
        println!("Room in range: {:?}", room);
    }

    // Print all entries in each column family
    print_cf_contents(&db)?;
    // Create backup of rooms
    println!("\n=== Creating backup of rooms collection ===");
    db.create_backup("rooms", "rooms_backup.sst")?;
    println!("Backup created successfully");

    // Drop the rooms collection
    println!("\n=== Dropping rooms collection ===");
    db.drop_cf("rooms")?;
    println!("Collection dropped");

    // Create new empty rooms collection
    db.create_cf("rooms")?;

    // Print state after drop
    println!("\n=== State after dropping rooms ===");
    print_cf_contents(&db)?;

    // Restore from backup
    println!("\n=== Restoring rooms from backup ===");
    db.restore_backup("rooms", "rooms_backup.sst")?;
    println!("Restore completed");

    // Print final state
    println!("\n=== Final State after restore ===");
    print_cf_contents(&db)?;
    print_queries(&db)?;

    Ok(())
}

// Helper function to filter rooms by style
fn filter_rooms_by_style(db: &RocksDB, style: MatchStyle) -> Result<Vec<Room>, KvStoreError> {
    let rooms: Vec<Room> = db.get_range_cf("rooms", "0", "999", 1000, Direction::Forward, false)?;
    Ok(rooms
        .into_iter()
        .filter(|room| room.style == style)
        .collect())
}

// Helper function to print contents and sizes of all column families
fn print_cf_contents(db: &RocksDB) -> Result<(), KvStoreError> {
    println!("\nContents and sizes of all column families:");

    // Helper function to print CF size
    let print_cf_size = |cf: &str| -> Result<(), KvStoreError> {
        let size = db.get_cf_size(cf)?;
        println!("\n{} Size:", cf);
        println!("  Total: {:.2} MB", size.total_mb());
        println!("  SST Files: {} bytes", size.sst_bytes);
        println!("  Memtable: {} bytes", size.mem_table_bytes);
        println!("  Blob Files: {} bytes", size.blob_bytes);
        Ok(())
    };

    // Print users (from default CF)
    println!("\nUsers:");
    print_cf_size("default")?;
    for id in 1..=3 {
        match db.get::<User>(&id.to_string()) {
            Ok(user) => println!("- {:?}", user),
            Err(KvStoreError::KeyNotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    // Print rooms
    println!("\nRooms:");
    print_cf_size("rooms")?;
    let rooms: Vec<Room> = db.get_range_cf("rooms", "0", "999", 1000, Direction::Forward, false)?;
    for room in rooms {
        println!("- {:?}", room);
    }

    // Print settings
    println!("\nSettings:");
    print_cf_size("settings")?;
    let settings: Vec<Settings> =
        db.get_range_cf("settings", "0", "999", 1000, Direction::Forward, false)?;
    for setting in settings {
        println!("- {:?}", setting);
    }

    // Print archived rooms
    println!("\nArchived Rooms:");
    print_cf_size("archived_rooms")?;
    let archived_rooms: Vec<Room> = db.get_range_cf(
        "archived_rooms",
        "0",
        "999",
        1000,
        Direction::Forward,
        false,
    )?;
    for room in archived_rooms {
        println!("- {:?}", room);
    }

    Ok(())
}

fn print_queries(db: &RocksDB) -> Result<(), KvStoreError> {
    // Basic root selector
    println!("\n--- Root and Basic Selectors ---");

    // Root element with wildcard
    println!("Get all rooms (root with wildcard):");
    let query = "$[*]";
    let all_rooms = db.query_cf::<Room>("rooms", query, false)?;
    println!("All rooms: {:?}", all_rooms);

    // Index selectors
    println!("\n--- Array Index Selectors ---");

    // Single index
    let query = "$[0]";
    println!("Get room at index 0: {query}");
    let first_room = db.query_cf::<Room>("rooms", query, false)?;
    println!("First room: {:?}", first_room);

    // Negative index (from end)
    let query = "$[-1]";
    println!("\nGet last room (negative index): {query}");
    let last_room = db.query_cf::<Room>("rooms", query, false)?;
    println!("Last room: {:?}", last_room);

    // Multiple indices
    let query = "$[0,2]";
    println!("\nGet rooms at specific indices (0 and 2): {query}");
    let selected_rooms = db.query_cf::<Room>("rooms", query, false)?;
    println!("Selected rooms: {:?}", selected_rooms);

    // Slice Operators
    println!("\n--- Array Slice Selectors ---");

    // Basic slice
    let query = "$[0:2]";
    println!("Get rooms from index 0 to 2 (exclusive): {query}");
    let first_two = db.query_cf::<Room>("rooms", query, false)?;
    println!("First two rooms: {:?}", first_two);

    // Open-ended slice (from index to end)
    let query = "$[1:]";
    println!("\nGet rooms from index 1 to end: {query}");
    let from_index_one = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms from index 1: {:?}", from_index_one);

    // Negative start index slice
    let query = "$[-2:]";
    println!("\nGet last two rooms: {query}");
    let last_two = db.query_cf::<Room>("rooms", query, false)?;
    println!("Last two rooms: {:?}", last_two);

    // Filter Expressions
    println!("\n--- Filter Expressions ---");

    // Existence check
    let query = "$[?@.owner]";
    println!("Find rooms with owner property: {query}");
    let rooms_with_owner = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms with owner: {:?}", rooms_with_owner);

    // Equality filter
    let query = "$[?@.id==2]";
    println!("\nFind rooms with id equal to 2: {query}");
    let room_id_2 = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms with id==2: {:?}", room_id_2);

    // Inequality filter
    let query = "$[?@.id!=2]";
    println!("\nFind rooms with id not equal to 2: {query}");
    let not_id_2 = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms with id!=2: {:?}", not_id_2);

    // Less than filter
    let query = "$[?@.id<3]";
    println!("\nFind rooms with id less than 3: {query}");
    let id_lt_3 = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms with id<3: {:?}", id_lt_3);

    // Greater than filter
    let query = "$[?@.id>1]";
    println!("\nFind rooms with id greater than 1: {query}");
    let id_gt_1 = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms with id>1: {:?}", id_gt_1);

    // Less than or equal filter
    let query = "$[?@.id<=2]";
    println!("\nFind rooms with id less than or equal to 2: {query}");
    let id_lte_2 = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms with id<=2: {:?}", id_lte_2);

    // Greater than or equal filter
    let query = "$[?@.id>=2]";
    println!("\nFind rooms with id greater than or equal to 2: {query}");
    let id_gte_2 = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms with id>=2: {:?}", id_gte_2);

    // Logical operators
    println!("\n--- Logical Operators ---");

    // AND operator
    let query = "$[?@.id>1&&@.style=='Team']";
    println!("Find rooms with id > 1 AND style == Team: {query}");
    let complex_and = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms matching AND condition: {:?}", complex_and);

    // OR operator
    let query = "$[?@.id==1||@.style=='Dm']";
    println!("\nFind rooms with id == 1 OR style == Dm: {query}");
    let complex_or = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms matching OR condition: {:?}", complex_or);

    // NOT operator
    let query = "$[?!(@.style=='Dm')]";
    println!("\nFind rooms that don't have style == Dm: {query}");
    let not_condition = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms not matching condition: {:?}", not_condition);

    // Alternative to custom extensions
    println!("\n--- Alternative to Custom Extensions ---");

    // Equivalent to 'in' function
    let query = "$[?@.id==1||@.id==3]";
    println!("Find rooms with id equal to 1 or 3 (alternative to 'in'): {query}");
    let in_equiv = db.query_cf::<Room>("rooms", query, false)?;
    println!("Rooms with id in [1,3]: {:?}", in_equiv);

    // Name Selectors
    println!("\n--- Name Selectors ---");

    // Direct lookup of specific property
    let query = "$[0].name";
    println!("Get specific room property (accessing through index then name): {query}");
    let first_room_name = db.query_cf::<Room>("rooms", query, false)?;
    println!("First room name: {:?}", first_room_name);

    // Settings with specific property value
    let query = "$[?@.color=='blue']";
    println!("\nFind settings with color = 'blue': {query}");
    let blue_settings = db.query_cf::<Settings>("settings", query, false)?;
    println!("Blue settings: {:?}", blue_settings);
    Ok(())
}
