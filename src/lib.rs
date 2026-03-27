use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use jsonpath_rust::JsonPath;
pub use rocksdb::{ColumnFamilyDescriptor, ColumnFamilyTtl, DBCompressionType, Direction, Options};
use rocksdb::{IngestExternalFileOptions, IteratorMode, SstFileWriter, WriteBatch, DB};
use serde::{de::DeserializeOwned, Serialize};

pub mod errors;
pub use errors::KvStoreError;

// --- Serialization ---

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, KvStoreError> {
    serde_json::to_vec(value).map_err(|e| KvStoreError::Serialization(e.to_string()))
}

fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, KvStoreError> {
    serde_json::from_slice(bytes).map_err(|e| KvStoreError::Serialization(e.to_string()))
}

// --- Types ---

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyValuePair<T> {
    pub key: String,
    pub value: T,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CFSize {
    pub total_bytes: u64,
    pub sst_bytes: u64,
    pub mem_table_bytes: u64,
    pub blob_bytes: u64,
}

impl CFSize {
    pub fn total_mb(&self) -> f64 {
        self.total_bytes as f64 / (1024.0 * 1024.0)
    }
}

/// Metadata for a column family's TTL configuration.
/// Stored as JSON in the secrets CF under `_cf_meta:{cf_name}`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CfMetadata {
    /// TTL in seconds. 0 means disabled (no expiry).
    pub ttl_secs: u64,
    /// Unix timestamp when this CF was created.
    #[serde(default)]
    pub created_at: Option<u64>,
}

const CF_META_PREFIX: &str = "_cf_meta:";

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
}

// Opening / lifecycle
impl RocksDB {
    pub fn open<P: AsRef<Path>>(path: P, opts: &Options) -> Result<Self, KvStoreError> {
        Ok(Self {
            db: Arc::new(DB::open(opts, path)?),
        })
    }

    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, KvStoreError> {
        Ok(Self {
            db: Arc::new(DB::open_default(path)?),
        })
    }

    pub fn open_cf<P, I, N>(opts: &Options, path: P, cfs: I) -> Result<Self, KvStoreError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cf_names: Vec<String> = cfs.into_iter().map(|n| n.as_ref().to_string()).collect();
        let db = DB::open_cf(opts, path, cf_names)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Open with a default TTL applied to all column families.
    /// RocksDB internally appends a timestamp to each value and removes
    /// expired entries during compaction.
    pub fn open_cf_with_ttl<P, I, N>(
        opts: &Options,
        path: P,
        cfs: I,
        ttl: Duration,
    ) -> Result<Self, KvStoreError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cf_names: Vec<String> = cfs.into_iter().map(|n| n.as_ref().to_string()).collect();
        let db = DB::open_cf_with_ttl(opts, path, cf_names, ttl)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Open with per-column-family TTL control via descriptors.
    /// Use `ColumnFamilyDescriptor::new_with_ttl` to set TTL per CF.
    pub fn open_cf_descriptors_with_ttl<P, I>(
        opts: &Options,
        path: P,
        cfs: I,
        default_ttl: Duration,
    ) -> Result<Self, KvStoreError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let db = DB::open_cf_descriptors_with_ttl(opts, path, cfs, default_ttl)?;
        Ok(Self { db: Arc::new(db) })
    }

    pub fn open_with_existing_cfs<P: AsRef<Path>>(
        opts: &Options,
        path: P,
    ) -> Result<Self, KvStoreError> {
        let db_path = path.as_ref();
        let cf_names = if db_path.join("CURRENT").exists() {
            Self::list_cf(db_path)?
        } else {
            if !db_path.exists() {
                std::fs::create_dir_all(db_path)?;
            }
            vec!["default".to_string()]
        };
        Self::open_cf(opts, path, cf_names)
    }

    pub fn list_cf<P: AsRef<Path>>(path: P) -> Result<Vec<String>, KvStoreError> {
        DB::list_cf(&Options::default(), path).map_err(Into::into)
    }

    pub fn compact_cf(&self, cf: &str) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        self.db
            .compact_range_cf(&cf_handle, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }

    pub fn default_opts() -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cpus = std::thread::available_parallelism()
            .map(|p| p.get() as i32)
            .unwrap_or(4);
        opts.increase_parallelism(cpus);
        opts.set_compression_type(DBCompressionType::Lz4);
        opts
    }

    pub fn write_optimized_opts() -> Options {
        let mut opts = Self::default_opts();
        opts.set_enable_pipelined_write(true);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(8);
        opts.set_min_write_buffer_number_to_merge(1);
        opts.set_allow_concurrent_memtable_write(true);
        opts.set_manual_wal_flush(true);
        opts.set_max_background_jobs(
            std::thread::available_parallelism()
                .map(|p| p.get() as i32 * 2)
                .unwrap_or(8),
        );
        opts.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);
        opts
    }

    fn cf_handle(&self, cf: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, KvStoreError> {
        self.db
            .cf_handle(cf)
            .ok_or_else(|| KvStoreError::InvalidColumnFamily(cf.to_string()))
    }
}

// Default CF operations
impl RocksDB {
    pub fn save(&self, key: &str, value: &[u8]) -> Result<(), KvStoreError> {
        self.db.put(key.as_bytes(), value).map_err(Into::into)
    }

    pub fn find(&self, key: &str) -> Result<Option<Vec<u8>>, KvStoreError> {
        self.db.get(key.as_bytes()).map_err(Into::into)
    }

    pub fn delete(&self, key: &str) -> Result<(), KvStoreError> {
        self.db.delete(key.as_bytes()).map_err(Into::into)
    }

    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T, KvStoreError> {
        let value = self
            .find(key)?
            .ok_or_else(|| KvStoreError::KeyNotFound(key.to_string()))?;
        deserialize(&value)
    }

    pub fn insert<T: Serialize>(&self, key: &str, value: &T) -> Result<(), KvStoreError> {
        self.save(key, &serialize(value)?)
    }

    pub fn batch_insert<T: Serialize>(&self, items: &[(&str, &T)]) -> Result<(), KvStoreError> {
        let mut batch = WriteBatch::default();
        for (key, value) in items {
            batch.put(key.as_bytes(), &serialize(value)?);
        }
        self.db.write(batch).map_err(Into::into)
    }
}

// Column family operations
impl RocksDB {
    pub fn create_cf(&self, name: &str) -> Result<(), KvStoreError> {
        if !self.cf_exists(name) {
            self.db.create_cf(name, &Options::default())?;
        }
        Ok(())
    }
    pub fn create_cf_with_ttl(&self, name: &str, ttl_secs: u64) -> Result<(), KvStoreError> {
        if !self.cf_exists(name) {
            let mut cf_opts = Options::default();
            cf_opts.set_periodic_compaction_seconds(ttl_secs.clamp(300, 86400));
            self.db.create_cf_with_ttl(
                name,
                &Options::default(),
                std::time::Duration::from_secs(ttl_secs),
            )?;
        }
        Ok(())
    }

    pub fn cf_exists(&self, name: &str) -> bool {
        self.db.cf_handle(name).is_some()
    }

    pub fn drop_cf(&self, cf: &str) -> Result<(), KvStoreError> {
        self.db.drop_cf(cf).map_err(Into::into)
    }

    pub fn insert_cf<T: Serialize>(
        &self,
        cf: &str,
        key: &str,
        value: &T,
    ) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        self.db
            .put_cf(&cf_handle, key.as_bytes(), serialize(value)?)
            .map_err(Into::into)
    }

    pub fn get_cf<T: DeserializeOwned>(&self, cf: &str, key: &str) -> Result<T, KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let value = self
            .db
            .get_cf(&cf_handle, key.as_bytes())?
            .ok_or_else(|| KvStoreError::KeyNotFound(key.to_string()))?;
        deserialize(&value)
    }

    pub fn delete_cf(&self, cf: &str, key: &str) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        self.db
            .delete_cf(&cf_handle, key.as_bytes())
            .map_err(Into::into)
    }

    pub fn batch_insert_cf<T: Serialize>(
        &self,
        cf: &str,
        items: &[(&str, &T)],
    ) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let mut batch = WriteBatch::default();
        for (key, value) in items {
            batch.put_cf(&cf_handle, key.as_bytes(), &serialize(value)?);
        }
        self.db.write(batch).map_err(Into::into)
    }

    pub fn get_cf_size(&self, cf: &str) -> Result<CFSize, KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let prop = |name| -> Result<u64, KvStoreError> {
            self.db
                .property_int_value_cf(&cf_handle, name)
                .map_err(|e| KvStoreError::Serialization(e.to_string()))
                .map(|v| v.unwrap_or(0))
        };
        let sst = prop("rocksdb.total-sst-files-size")?;
        let mem = prop("rocksdb.size-all-mem-tables")?;
        let blob = prop("rocksdb.total-blob-file-size")?;
        Ok(CFSize {
            total_bytes: sst + mem + blob,
            sst_bytes: sst,
            mem_table_bytes: mem,
            blob_bytes: blob,
        })
    }
}

// Range queries
impl RocksDB {
    pub fn get_range_cf(
        &self,
        cf: &str,
        from: &str,
        to: &str,
        limit: usize,
        direction: Direction,
        include_keys: bool,
    ) -> Result<Vec<serde_json::Value>, KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let mode = match direction {
            Direction::Forward => IteratorMode::From(from.as_bytes(), Direction::Forward),
            Direction::Reverse => IteratorMode::From(to.as_bytes(), Direction::Reverse),
        };

        let mut results = Vec::with_capacity(1024);
        for item in self.db.iterator_cf(&cf_handle, mode).take(limit) {
            let (key, value_bytes) = item?;
            let key_str = String::from_utf8_lossy(&key);

            match direction {
                Direction::Forward if key_str.as_ref() > to => break,
                Direction::Reverse if key_str.as_ref() < from => break,
                _ => {}
            }

            let value: serde_json::Value = serde_json::from_slice(&value_bytes)
                .map_err(|e| KvStoreError::Serialization(e.to_string()))?;

            if include_keys {
                results.push(serde_json::json!({
                    "key": key_str,
                    "value": value,
                }));
            } else {
                results.push(value);
            }
        }
        Ok(results)
    }

    pub fn get_range_cf_as<T: DeserializeOwned>(
        &self,
        cf: &str,
        from: &str,
        to: &str,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<T>, KvStoreError> {
        self.get_range_cf(cf, from, to, limit, direction, false)?
            .into_iter()
            .map(|v| {
                serde_json::from_value(v).map_err(|e| KvStoreError::Serialization(e.to_string()))
            })
            .collect()
    }
}

// JSONPath queries
impl RocksDB {
    pub fn query_cf(
        &self,
        cf: &str,
        query: &str,
        include_keys: bool,
    ) -> Result<Vec<serde_json::Value>, KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;

        let mut keys = Vec::new();
        let mut originals = Vec::new();
        let mut tagged = Vec::new();

        for item in self.db.iterator_cf(&cf_handle, IteratorMode::Start) {
            let (key, value_bytes) = item?;
            let value: serde_json::Value = serde_json::from_slice(&value_bytes)
                .map_err(|e| KvStoreError::Serialization(e.to_string()))?;
            keys.push(String::from_utf8_lossy(&key).into_owned());
            originals.push(value.clone());

            let mut obj = value;
            if let Some(map) = obj.as_object_mut() {
                map.insert("__idx".to_string(), serde_json::json!(tagged.len()));
            }
            tagged.push(obj);
        }

        let json_array = serde_json::Value::Array(tagged);
        let matches = json_array
            .query(query)
            .map_err(|e| KvStoreError::InvalidQuery(format!("JSONPath error: {}", e)))?;

        let mut results = Vec::with_capacity(matches.len());
        for matched in &matches {
            if let Some(i) = matched
                .get("__idx")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
            {
                if i < originals.len() {
                    if include_keys {
                        results.push(serde_json::json!({
                            "key": keys[i],
                            "value": originals[i],
                        }));
                    } else {
                        results.push(originals[i].clone());
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn query_cf_as<T: DeserializeOwned>(
        &self,
        cf: &str,
        query: &str,
    ) -> Result<Vec<T>, KvStoreError> {
        self.query_cf(cf, query, false)?
            .into_iter()
            .map(|v| {
                serde_json::from_value(v).map_err(|e| KvStoreError::Serialization(e.to_string()))
            })
            .collect()
    }
}

// Backup / restore
impl RocksDB {
    pub fn create_backup(&self, cf: &str, path: &str) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);
        writer.open(path)?;

        for item in self.db.iterator_cf(&cf_handle, IteratorMode::Start) {
            let (key, value) = item?;
            writer.put(&key, &value)?;
        }
        writer.finish()?;
        Ok(())
    }

    pub fn restore_backup(&self, cf: &str, path: &str) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let ingest_opts = IngestExternalFileOptions::default();
        self.db
            .ingest_external_file_cf_opts(&cf_handle, &ingest_opts, vec![path])?;
        Ok(())
    }
}

impl CfMetadata {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl_secs: ttl.as_secs(),
            created_at: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            ),
        }
    }

    pub fn to_cf_ttl(&self) -> ColumnFamilyTtl {
        if self.ttl_secs == 0 {
            ColumnFamilyTtl::Disabled
        } else {
            ColumnFamilyTtl::Duration(Duration::from_secs(self.ttl_secs))
        }
    }
}

// ── CF metadata persistence ──────────────────────────────────────────────────

impl RocksDB {
    /// Store CF metadata in the given meta CF (e.g. "secrets").
    pub fn store_cf_metadata(
        &self,
        meta_cf: &str,
        cf_name: &str,
        meta: &CfMetadata,
    ) -> Result<(), KvStoreError> {
        let key = format!("{CF_META_PREFIX}{cf_name}");
        let cf_handle = self.cf_handle(meta_cf)?;
        let bytes = serialize(meta)?;
        self.db
            .put_cf(&cf_handle, key.as_bytes(), bytes)
            .map_err(Into::into)
    }

    /// Load CF metadata from the meta CF.
    pub fn load_cf_metadata(
        &self,
        meta_cf: &str,
        cf_name: &str,
    ) -> Result<Option<CfMetadata>, KvStoreError> {
        let key = format!("{CF_META_PREFIX}{cf_name}");
        let cf_handle = self.cf_handle(meta_cf)?;
        match self.db.get_cf(&cf_handle, key.as_bytes())? {
            Some(bytes) => Ok(Some(deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Load all CF metadata entries from the meta CF.
    pub fn load_all_cf_metadata(
        &self,
        meta_cf: &str,
    ) -> Result<HashMap<String, CfMetadata>, KvStoreError> {
        let cf_handle = self.cf_handle(meta_cf)?;
        let prefix = CF_META_PREFIX.as_bytes();
        let mut result = HashMap::new();

        for item in self.db.iterator_cf(&cf_handle, IteratorMode::Start) {
            let (key_bytes, val_bytes) = item?;
            let key = String::from_utf8_lossy(&key_bytes);
            if let Some(cf_name) = key.strip_prefix(CF_META_PREFIX) {
                if let Ok(meta) = deserialize::<CfMetadata>(&val_bytes) {
                    result.insert(cf_name.to_string(), meta);
                }
            }
            // Since iterator is sorted and our prefix starts with '_',
            // we can break once we pass the prefix range
            if key_bytes.len() >= prefix.len() && &key_bytes[..prefix.len()] > prefix {
                break;
            }
        }
        Ok(result)
    }

    /// Delete CF metadata from the meta CF.
    pub fn delete_cf_metadata(&self, meta_cf: &str, cf_name: &str) -> Result<(), KvStoreError> {
        let key = format!("{CF_META_PREFIX}{cf_name}");
        self.delete_cf(meta_cf, &key)
    }
}

// ── TTL-aware open ───────────────────────────────────────────────────────────

impl RocksDB {
    /// Open the database with per-CF TTL from saved metadata.
    ///
    /// Two-phase open:
    ///   1. Open all existing CFs with TTL disabled → read metadata from `meta_cf`
    ///   2. Close → reopen with correct per-CF TTLs
    ///
    /// `system_cfs` (e.g. "secrets", "backups", "restores") always get
    /// `ColumnFamilyTtl::Disabled`.
    pub fn open_with_ttl_metadata<P: AsRef<Path>>(
        opts: &Options,
        path: P,
        meta_cf: &str,
        system_cfs: &[&str],
    ) -> Result<Self, KvStoreError> {
        let db_path = path.as_ref();

        // Discover existing CFs
        let cf_names = if db_path.join("CURRENT").exists() {
            Self::list_cf(db_path)?
        } else {
            if !db_path.exists() {
                std::fs::create_dir_all(db_path)?;
            }
            vec!["default".to_string()]
        };

        // Phase 1: Open everything with TTL disabled to read metadata
        let phase1_descriptors: Vec<ColumnFamilyDescriptor> = cf_names
            .iter()
            .map(|name| {
                ColumnFamilyDescriptor::new_with_ttl(
                    name.as_str(),
                    Options::default(),
                    ColumnFamilyTtl::Disabled,
                )
            })
            .collect();

        let temp_db =
            DB::open_cf_descriptors_with_ttl(opts, db_path, phase1_descriptors, Duration::ZERO)?;
        let temp = Self {
            db: Arc::new(temp_db),
        };

        // Ensure meta CF exists
        if !temp.cf_exists(meta_cf) {
            temp.create_cf(meta_cf)?;
        }

        // Read all CF metadata
        let cf_metadata = temp.load_all_cf_metadata(meta_cf).unwrap_or_default();

        // Drop phase 1 handle
        drop(temp);

        // Phase 2: Reopen with correct per-CF TTLs
        let cf_names = Self::list_cf(db_path)?;

        let descriptors: Vec<ColumnFamilyDescriptor> = cf_names
            .iter()
            .map(|name| {
                let ttl = if name == "default" || system_cfs.contains(&name.as_str()) {
                    ColumnFamilyTtl::Disabled
                } else if let Some(meta) = cf_metadata.get(name.as_str()) {
                    meta.to_cf_ttl()
                } else {
                    ColumnFamilyTtl::Disabled
                };
                ColumnFamilyDescriptor::new_with_ttl(name.as_str(), Options::default(), ttl)
            })
            .collect();

        let db = DB::open_cf_descriptors_with_ttl(opts, db_path, descriptors, Duration::ZERO)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Create a new CF with TTL and persist metadata.
    /// The per-CF TTL takes effect on next restart (RocksDB limitation).
    pub fn create_cf_with_ttl_meta(
        &self,
        meta_cf: &str,
        cf_name: &str,
        ttl: Duration,
    ) -> Result<(), KvStoreError> {
        let meta = CfMetadata::new(ttl);
        self.store_cf_metadata(meta_cf, cf_name, &meta)?;

        if ttl.is_zero() {
            self.create_cf(cf_name)?;
        } else {
            self.create_cf_with_ttl(cf_name, ttl.as_secs())?;
        }
        Ok(())
    }

    /// Drop a CF and clean up its metadata.
    pub fn drop_cf_with_meta(&self, meta_cf: &str, cf_name: &str) -> Result<(), KvStoreError> {
        let _ = self.delete_cf_metadata(meta_cf, cf_name);
        self.drop_cf(cf_name)
    }
}
