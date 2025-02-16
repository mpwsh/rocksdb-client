use std::io;
use std::string::FromUtf8Error;
use std::time::SystemTimeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvStoreError {
    #[error("Database operation failed: {0}")]
    DbError(#[from] rocksdb::Error),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
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
    #[error("Property access error: {0}")]
    PropertyAccessError(String),
    #[error("Query error: {0}")]
    InvalidQuery(String),
}

impl From<rmp_serde::encode::Error> for KvStoreError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        KvStoreError::SerializationError(err.to_string())
    }
}

impl From<rmp_serde::decode::Error> for KvStoreError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        KvStoreError::DeserializationError(err.to_string())
    }
}
