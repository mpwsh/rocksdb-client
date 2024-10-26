use std::io;
use std::string::FromUtf8Error;
use std::time::SystemTimeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvStoreError {
    #[error("Database operation failed: {0}")]
    DbError(#[from] rocksdb::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    #[error("Invalid column family: {0}")]
    InvalidColumnFamily(String),
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("UTF-8 conversion error: {0}")]
    Utf8Error(#[from] FromUtf8Error),
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
}
