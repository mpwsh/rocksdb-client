use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvStoreError {
    #[error("Database error: {0}")]
    Db(#[from] rocksdb::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    #[error("Column family not found: {0}")]
    InvalidColumnFamily(String),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}
