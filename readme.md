# rocksdb-client

A lightweight Rust wrapper around [RocksDB](https://github.com/rust-rocksdb/rust-rocksdb) with JSON storage, JSONPath queries, and typed serialization.

## Features

- **JSON storage** — values are stored as raw JSON, readable with standard RocksDB tools
- **JSONPath queries** — filter documents using [RFC 9535](https://www.rfc-editor.org/rfc/rfc9535) JSONPath expressions
- **Typed serialization** — insert and retrieve Rust structs with automatic serde (de)serialization
- **Column families** — organize data into separate namespaces
- **Range queries** — key-based seeking with direction and limit controls
- **Backup / restore** — export and import column families via SST files
- **Preset configurations** — `default_opts()` and `write_optimized_opts()` with sensible RocksDB tuning

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
rocksdb-client = { git = "https://github.com/mpwsh/rocksdb-client" }
```

### Basic CRUD

```rust
use rocksdb_client::{Options, RocksDB};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: u64,
    name: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = RocksDB::default_opts();
    let db = RocksDB::open("./mydb", &opts)?;

    // Typed insert and get
    let user = User { id: 1, name: "Alice".to_string() };
    db.insert("user:1", &user)?;
    let retrieved: User = db.get("user:1")?;

    // Raw bytes
    db.save("raw_key", b"raw_value")?;
    let bytes = db.find("raw_key")?;

    db.delete("raw_key")?;
    Ok(())
}
```

### Column Families

```rust
let db = RocksDB::open_cf(&opts, "./mydb", vec!["users", "rooms"])?;

// Or open with whatever CFs already exist in the database
let db = RocksDB::open_with_existing_cfs(&opts, "./mydb")?;

db.create_cf("settings")?;
db.insert_cf("settings", "theme", &"dark")?;
let theme: String = db.get_cf("settings", "theme")?;
db.drop_cf("settings")?;
```

### Batch Inserts

```rust
let items: Vec<(&str, &User)> = vec![
    ("user:1", &user1),
    ("user:2", &user2),
    ("user:3", &user3),
];
db.batch_insert_cf("users", &items)?;
```

### Range Queries

```rust
use rocksdb_client::Direction;

// Values only
let rooms = db.get_range_cf("rooms", "a", "z", 100, Direction::Forward, false)?;

// With keys included
let rooms = db.get_range_cf("rooms", "a", "z", 100, Direction::Forward, true)?;

// Reverse order
let latest = db.get_range_cf("rooms", "a", "z", 10, Direction::Reverse, false)?;
```

> **Note:** Keys are sorted lexicographically. Numeric keys like `"1"`, `"10"`, `"2"` sort as `1 → 10 → 2`, not `1 → 2 → 10`. Use zero-padded keys (`"001"`, `"002"`, `"010"`) if you need numeric ordering.

### JSONPath Queries

Filter documents using JSONPath expressions. Returns `Vec<serde_json::Value>`.

```rust
// Values only
let results = db.query_cf("rooms", "$[?@.style=='Team']", false)?;

// With keys
let results = db.query_cf("rooms", "$[?@.style=='Team']", true)?;

// Typed convenience method
let rooms: Vec<Room> = db.query_cf_as("rooms", "$[?@.style=='Team']")?;
```

#### JSONPath Cheat Sheet

```
$[?@.field == 'value']              exact match
$[?@.field > 10]                    comparison (>, <, >=, <=, !=)
$[?@.field > 10 && @.x < 5]        AND
$[?@.field == 1 || @.field == 2]    OR
$[?!(@.field == 'value')]           NOT
$[?@.nested.field > 100]            nested field access
$[?@.tags[0] == 'alpha']            array element access
$[*]                                all items
$[0]                                first item
$[-1]                               last item
$[0:5]                              slice (first 5)
```

### Backup and Restore

```rust
// Export a column family to an SST file
db.create_backup("rooms", "./backups/rooms.sst")?;

// Import from SST file
db.restore_backup("rooms", "./backups/rooms.sst")?;
```

### Configuration Presets

```rust
// General purpose — Lz4 compression, auto-parallelism
let opts = RocksDB::default_opts();

// Write-heavy workloads — pipelined writes, tiered compression, manual WAL flush
// Trades durability for throughput
let opts = RocksDB::write_optimized_opts();

// Custom — start from a preset and override
let mut opts = RocksDB::default_opts();
opts.set_compression_type(DBCompressionType::None);
opts.set_write_buffer_size(128 * 1024 * 1024);
```

### Column Family Size

```rust
let size = db.get_cf_size("rooms")?;
println!("Total: {:.2} MB", size.total_mb());
println!("SST: {} bytes", size.sst_bytes);
println!("Memtable: {} bytes", size.mem_table_bytes);
```

## Inspecting Data with ldb

Since values are stored as raw JSON, you can inspect your database directly with RocksDB's `ldb` tool.

### Install

```bash
# macOS
brew install rocksdb
# The binary is installed as rocksdb_ldb
# Optional: alias it
alias ldb=rocksdb_ldb

# Ubuntu / Debian
apt install rocksdb-tools
```

### Common Commands

```bash
# List all column families
ldb --db=./mydb list_column_families

# Scan all keys in a column family
ldb --db=./mydb scan --column_family=rooms

# Scan with a limit
ldb --db=./mydb scan --column_family=rooms --max_keys=10

# Get a specific key
ldb --db=./mydb get user:1 --column_family=users

# Get database stats
ldb --db=./mydb get_property rocksdb.stats --column_family=rooms

# Get estimated key count
ldb --db=./mydb get_property rocksdb.estimate-num-keys --column_family=rooms

# Get level stats
ldb --db=./mydb get_property rocksdb.levelstats --column_family=rooms
```

## Examples

See the `examples/` directory:

```bash
# Basic CRUD, column families, backup/restore
cargo run --example basic

# JSONPath query benchmarks
cargo run --release --example queries

# Mixed read/write throughput benchmark
cargo run --release --example bench
```

## Error Handling

All fallible operations return `Result<T, KvStoreError>`:

```rust
use rocksdb_client::KvStoreError;

match db.get_cf::<User>("users", "unknown") {
    Ok(user) => println!("{:?}", user),
    Err(KvStoreError::KeyNotFound(key)) => println!("No user: {}", key),
    Err(KvStoreError::InvalidColumnFamily(cf)) => println!("No CF: {}", cf),
    Err(e) => println!("Error: {}", e),
}
```

## License

Apache-2.0
