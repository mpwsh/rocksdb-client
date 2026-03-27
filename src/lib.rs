use std::{path::Path, sync::Arc};

use jsonpath_rust::JsonPath;
pub use rocksdb::{ColumnFamilyDescriptor, DBCompressionType, Direction, Options};
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

// --- Database ---

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
        Ok(Self {
            db: Arc::new(DB::open_cf(opts, path, cf_names)?),
        })
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
        // Tiered compression: fast on hot data, dense on cold
        opts.set_compression_per_level(&[
            DBCompressionType::None, // L0 — active writes, skip compression
            DBCompressionType::Lz4,  // L1
            DBCompressionType::Lz4,  // L2
            DBCompressionType::Zstd, // L3+
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

    pub fn create_cf(&self, name: &str) -> Result<(), KvStoreError> {
        if !self.cf_exists(name) {
            self.db.create_cf(name, &Options::default())?;
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

        let mut results = Vec::with_capacity(limit);
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

            // Inject index for O(1) result mapping
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
    // Convenience — nice ergonomics for library users
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

    // Same for range
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
