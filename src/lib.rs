use std::path::Path;

pub use rocksdb::{ColumnFamilyDescriptor, Options};
use rocksdb::{Direction, IteratorMode, WriteBatch, DB};
use serde::{de::DeserializeOwned, Serialize};
pub mod errors;
use std::sync::Arc;

pub use errors::KvStoreError;

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
    fn save(&self, k: &str, v: &[u8]) -> Result<(), KvStoreError>;
    fn find(&self, k: &str) -> Result<Option<Vec<u8>>, KvStoreError>;
    fn delete(&self, k: &str) -> Result<(), KvStoreError>;
    fn list_cf(path: &str) -> Result<Vec<String>, KvStoreError>;
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T, KvStoreError>;
    fn insert<T: Serialize>(&self, key: &str, v: &T) -> Result<(), KvStoreError>;
    fn batch_insert<T: Serialize>(&self, items: &[(&str, &T)]) -> Result<(), KvStoreError>;
    fn create_cf(&self, name: &str) -> Result<(), KvStoreError>;
    fn cf_exists(&self, name: &str) -> bool;
    fn insert_cf<T: Serialize>(&self, cf: &str, key: &str, value: &T) -> Result<(), KvStoreError>;
    fn get_cf<T: DeserializeOwned>(&self, cf: &str, key: &str) -> Result<T, KvStoreError>;
    fn get_range_cf<T: DeserializeOwned>(
        &self,
        cf: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<T>, KvStoreError>;
    fn delete_cf(&self, cf: &str, key: &str) -> Result<(), KvStoreError>;
    fn drop_cf(&self, cf: &str) -> Result<(), KvStoreError>;
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
        serde_json::from_slice(&value).map_err(KvStoreError::from)
    }

    fn insert<T: Serialize>(&self, key: &str, v: &T) -> Result<(), KvStoreError> {
        let serialized = serde_json::to_vec(v)?;
        self.save(key, &serialized)
    }
    fn batch_insert<T: Serialize>(&self, items: &[(&str, &T)]) -> Result<(), KvStoreError> {
        let mut batch = WriteBatch::default();

        for (key, value) in items {
            let serialized = serde_json::to_vec(value)?;
            batch.put(key.as_bytes(), &serialized);
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
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or(KvStoreError::InvalidColumnFamily(cf.to_string()))?;
        let serialized = serde_json::to_vec(value)?;
        self.db
            .put_cf(&cf_handle, key.as_bytes(), serialized)
            .map_err(KvStoreError::from)
    }

    fn get_cf<T: DeserializeOwned>(&self, cf: &str, key: &str) -> Result<T, KvStoreError> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or(KvStoreError::InvalidColumnFamily(cf.to_string()))?;
        let value = self
            .db
            .get_cf(&cf_handle, key.as_bytes())?
            .ok_or(KvStoreError::KeyNotFound(key.to_string()))?;
        serde_json::from_slice(&value).map_err(KvStoreError::from)
    }

    fn get_range_cf<T: DeserializeOwned>(
        &self,
        cf: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<T>, KvStoreError> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or(KvStoreError::InvalidColumnFamily(cf.to_string()))?;
        let iter = self.db.iterator_cf(
            &cf_handle,
            IteratorMode::From(from.as_bytes(), Direction::Forward),
        );
        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key > to.as_bytes().into() {
                break;
            }
            let deserialized: T = serde_json::from_slice(&value)?;
            results.push(deserialized);
        }
        Ok(results)
    }
    fn delete_cf(&self, cf: &str, key: &str) -> Result<(), KvStoreError> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or(KvStoreError::InvalidColumnFamily(cf.to_string()))?;
        self.db
            .delete_cf(&cf_handle, key.as_bytes())
            .map_err(KvStoreError::from)
    }

    fn drop_cf(&self, cf: &str) -> Result<(), KvStoreError> {
        self.db.drop_cf(cf).map_err(KvStoreError::from)
    }
}
