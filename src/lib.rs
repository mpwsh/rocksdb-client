use std::path::Path;

use jsonpath_rust::JsonPath;
pub use rocksdb::{ColumnFamilyDescriptor, CuckooTableOptions, Direction, Options};
use rocksdb::{IngestExternalFileOptions, IteratorMode, SstFileWriter, WriteBatch, DB};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
pub mod errors;
use std::sync::Arc;

pub use errors::KvStoreError;

#[derive(Serialize, Deserialize)]
pub struct KeyValuePair<T> {
    pub key: String,
    pub value: T,
}

pub trait ByteSerializer {
    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, KvStoreError>;
    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, KvStoreError>;
}

pub struct MessagePackSerializer;

impl ByteSerializer for MessagePackSerializer {
    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, KvStoreError> {
        rmp_serde::to_vec(value).map_err(Into::into)
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, KvStoreError> {
        rmp_serde::from_slice(bytes).map_err(Into::into)
    }
}
#[derive(Debug, Clone, Serialize)]
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
pub trait KVStore: Sized {
    fn open<P: AsRef<Path>>(path: P, opts: &Options) -> Result<Self, KvStoreError>;
    fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, KvStoreError>;
    fn open_cf<P, I, N>(opts: &Options, path: P, cfs: I) -> Result<Self, KvStoreError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>;
    fn open_with_existing_cfs<P: AsRef<Path>>(
        opts: &Options,
        path: P,
    ) -> Result<Self, KvStoreError>;
    fn cf_handle(&self, cf: &str) -> Result<Arc<rocksdb::BoundColumnFamily>, KvStoreError>;
    fn save(&self, k: &str, v: &[u8]) -> Result<(), KvStoreError>;
    fn find(&self, k: &str) -> Result<Option<Vec<u8>>, KvStoreError>;
    fn delete(&self, k: &str) -> Result<(), KvStoreError>;
    fn list_cf(path: &str) -> Result<Vec<String>, KvStoreError>;
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T, KvStoreError>;
    fn insert<T: Serialize>(&self, key: &str, v: &T) -> Result<(), KvStoreError>;
    fn batch_insert<T: Serialize>(&self, items: &[(&str, &T)]) -> Result<(), KvStoreError>;
    fn batch_insert_cf<T: Serialize>(
        &self,
        cf: &str,
        items: &[(&str, &T)],
    ) -> Result<(), KvStoreError>;
    fn create_cf(&self, name: &str) -> Result<(), KvStoreError>;
    fn cf_exists(&self, name: &str) -> bool;
    fn insert_cf<T: Serialize>(&self, cf: &str, key: &str, value: &T) -> Result<(), KvStoreError>;
    fn get_cf<T: DeserializeOwned>(&self, cf: &str, key: &str) -> Result<T, KvStoreError>;
    fn get_range_cf<T: DeserializeOwned + Serialize>(
        &self,
        cf: &str,
        from: &str,
        to: &str,
        limit: usize,
        direction: Direction,
        include_keys: bool,
    ) -> Result<Vec<T>, KvStoreError>;
    fn delete_cf(&self, cf: &str, key: &str) -> Result<(), KvStoreError>;
    fn drop_cf(&self, cf: &str) -> Result<(), KvStoreError>;
    fn get_cf_size(&self, cf: &str) -> Result<CFSize, KvStoreError>;
    fn query_cf<T: DeserializeOwned + Serialize + Clone>(
        &self,
        cf: &str,
        query: &str,
        include_keys: bool,
    ) -> Result<Vec<T>, KvStoreError>;
    fn create_backup(&self, cf: &str, path: &str) -> Result<(), KvStoreError>;
    fn restore_backup(&self, cf: &str, path: &str) -> Result<(), KvStoreError>;
}

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
}

impl KVStore for RocksDB {
    fn open<P: AsRef<Path>>(path: P, opts: &Options) -> Result<Self, KvStoreError> {
        DB::open(opts, path)
            .map(|db| RocksDB { db: Arc::new(db) })
            .map_err(KvStoreError::from)
    }

    fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, KvStoreError> {
        DB::open_default(path)
            .map(|db| RocksDB { db: Arc::new(db) })
            .map_err(KvStoreError::from)
    }

    fn open_cf<P, I, N>(opts: &Options, path: P, cfs: I) -> Result<Self, KvStoreError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cf_names: Vec<String> = cfs.into_iter().map(|n| n.as_ref().to_string()).collect();
        let db = DB::open_cf(opts, path, cf_names)?;
        Ok(RocksDB { db: Arc::new(db) })
    }
    fn open_with_existing_cfs<P: AsRef<Path>>(
        opts: &Options,
        path: P,
    ) -> Result<Self, KvStoreError> {
        let db_path = path.as_ref();
        let current_file = db_path.join("CURRENT");

        let cf_names = if db_path.exists() && current_file.exists() {
            // Existing database
            Self::list_cf(db_path.to_str().unwrap())?
        } else {
            // New database or not initialized
            if !db_path.exists() {
                std::fs::create_dir_all(db_path)?;
            }
            vec!["default".to_string()]
        };

        Self::open_cf(opts, path, cf_names)
    }
    fn cf_handle(&self, cf: &str) -> Result<Arc<rocksdb::BoundColumnFamily>, KvStoreError> {
        self.db
            .cf_handle(cf)
            .ok_or_else(|| KvStoreError::InvalidColumnFamily(cf.to_string()))
    }
    fn list_cf(path: &str) -> Result<Vec<String>, KvStoreError> {
        let cf_names = DB::list_cf(&Options::default(), path).map_err(KvStoreError::from)?;
        Ok(cf_names)
    }
    fn save(&self, k: &str, v: &[u8]) -> Result<(), KvStoreError> {
        self.db.put(k.as_bytes(), v).map_err(KvStoreError::from)
    }

    fn find(&self, k: &str) -> Result<Option<Vec<u8>>, KvStoreError> {
        self.db.get(k.as_bytes()).map_err(KvStoreError::from)
    }

    fn delete(&self, k: &str) -> Result<(), KvStoreError> {
        self.db.delete(k.as_bytes()).map_err(KvStoreError::from)
    }

    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T, KvStoreError> {
        let value = self
            .find(key)?
            .ok_or_else(|| KvStoreError::KeyNotFound(key.to_string()))?;
        MessagePackSerializer.deserialize(&value)
    }

    fn insert<T: Serialize>(&self, key: &str, v: &T) -> Result<(), KvStoreError> {
        let serialized = MessagePackSerializer.serialize(v)?;
        self.save(key, &serialized)
    }
    fn batch_insert<T: Serialize>(&self, items: &[(&str, &T)]) -> Result<(), KvStoreError> {
        let mut batch = WriteBatch::default();

        for (key, value) in items {
            let serialized = MessagePackSerializer.serialize(value)?;
            batch.put(key.as_bytes(), &serialized);
        }

        self.db.write(batch).map_err(KvStoreError::from)
    }
    fn batch_insert_cf<T: Serialize>(
        &self,
        cf: &str,
        items: &[(&str, &T)],
    ) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;

        let mut batch = WriteBatch::default();
        for (key, value) in items {
            let serialized = MessagePackSerializer.serialize(value)?;
            batch.put_cf(&cf_handle, key.as_bytes(), &serialized);
        }

        self.db.write(batch).map_err(KvStoreError::from)
    }
    fn create_cf(&self, name: &str) -> Result<(), KvStoreError> {
        if !self.cf_exists(name) {
            self.db
                .create_cf(name, &Options::default())
                .map_err(KvStoreError::from)?;
        }
        Ok(())
    }

    fn cf_exists(&self, name: &str) -> bool {
        self.db.cf_handle(name).is_some()
    }
    fn insert_cf<T: Serialize>(&self, cf: &str, key: &str, value: &T) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;

        let serialized = MessagePackSerializer.serialize(value)?;
        self.db
            .put_cf(&cf_handle, key.as_bytes(), serialized)
            .map_err(KvStoreError::from)
    }

    fn get_cf<T: DeserializeOwned>(&self, cf: &str, key: &str) -> Result<T, KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let value = self
            .db
            .get_cf(&cf_handle, key.as_bytes())?
            .ok_or(KvStoreError::KeyNotFound(key.to_string()))?;
        MessagePackSerializer.deserialize(&value)
    }

    fn get_range_cf<T: DeserializeOwned + Serialize>(
        &self,
        cf: &str,
        from: &str,
        to: &str,
        limit: usize,
        direction: Direction,
        include_keys: bool,
    ) -> Result<Vec<T>, KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let iter = self.db.iterator_cf(&cf_handle, IteratorMode::Start);
        let all_keys: Vec<Vec<u8>> = iter
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;

        let from_idx = from.parse::<usize>().unwrap_or(0);
        let to_idx = to.parse::<usize>().unwrap_or(all_keys.len());
        let from_idx = from_idx.min(all_keys.len());
        let to_idx = (to_idx + 1).min(all_keys.len());

        let keys_to_fetch = match direction {
            Direction::Forward => all_keys[from_idx..to_idx].to_vec(),
            Direction::Reverse => {
                let mut keys = all_keys[from_idx..to_idx].to_vec();
                keys.reverse();
                keys
            }
        };

        let mut results = Vec::new();
        for key in keys_to_fetch.iter().take(limit) {
            if let Some(value) = self.db.get_cf(&cf_handle, key)? {
                let value: T = MessagePackSerializer.deserialize(&value)?;
                if include_keys {
                    let key_str = String::from_utf8_lossy(key).into_owned();
                    let kv = serde_json::from_value(serde_json::json!({
                        "key": key_str,
                        "value": value
                    }))
                    .map_err(|e| KvStoreError::SerializationError(e.to_string()))?;
                    results.push(kv);
                } else {
                    results.push(value);
                }
            }
        }

        Ok(results)
    }
    fn delete_cf(&self, cf: &str, key: &str) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;

        let _ = self
            .db
            .get_cf(&cf_handle, key.as_bytes())?
            .ok_or(KvStoreError::KeyNotFound(key.to_string()))?;
        self.db
            .delete_cf(&cf_handle, key.as_bytes())
            .map_err(KvStoreError::from)
    }

    fn drop_cf(&self, cf: &str) -> Result<(), KvStoreError> {
        self.db.drop_cf(cf).map_err(KvStoreError::from)
    }
    fn get_cf_size(&self, cf: &str) -> Result<CFSize, KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;

        let live_sst_size = self
            .db
            .property_int_value_cf(&cf_handle, "rocksdb.total-sst-files-size")
            .map_err(|e| KvStoreError::PropertyAccessError(e.to_string()))?
            .unwrap_or(0);

        let mem_table_size = self
            .db
            .property_int_value_cf(&cf_handle, "rocksdb.size-all-mem-tables")
            .map_err(|e| KvStoreError::PropertyAccessError(e.to_string()))?
            .unwrap_or(0);

        let blob_size = self
            .db
            .property_int_value_cf(&cf_handle, "rocksdb.total-blob-file-size")
            .map_err(|e| KvStoreError::PropertyAccessError(e.to_string()))?
            .unwrap_or(0);

        Ok(CFSize {
            total_bytes: live_sst_size + mem_table_size + blob_size,
            sst_bytes: live_sst_size,
            mem_table_bytes: mem_table_size,
            blob_bytes: blob_size,
        })
    }
    fn query_cf<T: DeserializeOwned + Serialize + Clone>(
        &self,
        cf: &str,
        query: &str,
        include_keys: bool,
    ) -> Result<Vec<T>, KvStoreError> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| KvStoreError::InvalidColumnFamily(cf.to_string()))?;

        // Get all documents from the column family
        let mut all_docs = Vec::new();
        for item in self.db.iterator_cf(&cf_handle, IteratorMode::Start) {
            if let Ok((key, value_bytes)) = item {
                if let Ok(value) = MessagePackSerializer.deserialize::<T>(&value_bytes) {
                    all_docs.push((key.to_vec(), value));
                }
            }
        }

        // Create a JSON array containing all documents for proper querying
        let json_docs: Vec<serde_json::Value> = all_docs
            .iter()
            .map(|(_, value)| serde_json::to_value(value).unwrap_or(serde_json::Value::Null))
            .collect();

        let json_array = serde_json::Value::Array(json_docs.clone());

        // Run the JSONPath query on the array
        let results = match json_array.query(query) {
            Ok(matches) => matches,
            Err(e) => return Err(KvStoreError::InvalidQuery(format!("JSONPath error: {}", e))),
        };

        // Transform results back to the original type
        let mut final_results = Vec::new();
        for result in results {
            // Find the matching document by comparing serialized JSON values
            for (idx, doc_value) in json_docs.iter().enumerate() {
                if result == doc_value {
                    let (key, value) = &all_docs[idx];

                    if include_keys {
                        // Create a KeyValuePair structure with key and value
                        let key_str = String::from_utf8_lossy(key).to_string();
                        let kv_json = serde_json::json!({
                            "key": key_str,
                            "value": value
                        });

                        if let Ok(typed_kv) = serde_json::from_value::<T>(kv_json) {
                            final_results.push(typed_kv);
                        }
                    } else {
                        // Just add the value directly
                        final_results.push(value.clone());
                    }

                    break;
                }
            }
        }

        Ok(final_results)
    }

    fn create_backup(&self, cf: &str, path: &str) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;

        // Create a new SST file writer
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut writer = SstFileWriter::create(&opts);

        // Open the writer with the specified file path
        writer.open(path)?;

        // Get an iterator over the CF
        let iter = self.db.iterator_cf(&cf_handle, IteratorMode::Start);

        // Write all KV pairs to the SST file
        for item in iter {
            let (key, value) = item?;
            writer.put(&key, &value)?;
        }

        // Finish writing and close the file
        writer.finish()?;

        Ok(())
    }

    fn restore_backup(&self, cf: &str, path: &str) -> Result<(), KvStoreError> {
        let cf_handle = self.cf_handle(cf)?;

        // Create ingest options
        let ingest_opts = IngestExternalFileOptions::default();

        // Ingest the SST file
        self.db
            .ingest_external_file_cf_opts(&cf_handle, &ingest_opts, vec![path])?;

        Ok(())
    }
}
