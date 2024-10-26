#[cfg(test)]
mod tests {
    use rocksdb_client::{KVStore, KvStoreError, Options, RocksDB};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestUser {
        id: u32,
        name: String,
    }

    fn create_temp_db() -> (TempDir, RocksDB) {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksDB::open_default(temp_dir.path()).unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_open_default() {
        let (_temp_dir, db) = create_temp_db();
        assert!(db.save("test_key", b"test_value").is_ok());
    }

    #[test]
    fn test_open_with_options() {
        let temp_dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_max_open_files(10);
        let db = RocksDB::open(temp_dir.path(), &opts).unwrap();
        assert!(db.save("test_key", b"test_value").is_ok());
    }

    #[test]
    fn test_open_cf() {
        let temp_dir = TempDir::new().unwrap();
        let cfs = vec!["cf1".to_string(), "cf2".to_string()];
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = RocksDB::open_cf(&opts, temp_dir.path(), cfs).unwrap();
        assert!(db.save("test_key", b"test_value").is_ok());
    }

    #[test]
    fn test_crud_operations() {
        let (_temp_dir, db) = create_temp_db();

        // Create
        let save_result = db.save("key1", b"value1");
        assert!(save_result.is_ok(), "Save failed: {:?}", save_result);

        // Read
        let found = db.find("key1");
        eprintln!("After save, found: {:?}", found); // Debug print
        assert_eq!(
            found.unwrap(),
            Some(b"value1".to_vec()),
            "Value mismatch after save"
        );

        // Update
        let update_result = db.save("key1", b"new_value1");
        assert!(update_result.is_ok(), "Update failed: {:?}", update_result);

        let updated = db.find("key1");
        eprintln!("After update, found: {:?}", updated); // Debug print
        assert_eq!(
            updated.unwrap(),
            Some(b"new_value1".to_vec()),
            "Value mismatch after update"
        );

        // Delete
        let delete_result = db.delete("key1");
        assert!(delete_result.is_ok(), "Delete failed: {:?}", delete_result);

        let after_delete = db.find("key1");
        eprintln!("After delete, found: {:?}", after_delete); // Debug print
        assert_eq!(after_delete.unwrap(), None, "Key still exists after delete");
    }

    #[test]
    fn test_serialization() {
        let (_temp_dir, db) = create_temp_db();
        let user = TestUser {
            id: 1,
            name: "Alice".to_string(),
        };

        // Insert
        assert!(db.insert("user:1", &user).is_ok());

        // Get
        let retrieved_user: TestUser = db.get("user:1").unwrap();
        assert_eq!(user, retrieved_user);
    }

    #[test]
    fn test_key_not_found() {
        let (_temp_dir, db) = create_temp_db();
        match db.get::<TestUser>("non_existent_key") {
            Err(KvStoreError::KeyNotFound(_)) => (),
            _ => panic!("Expected KeyNotFound error"),
        }
    }

    #[test]
    fn test_large_value() {
        let (_temp_dir, db) = create_temp_db();
        let large_value = vec![0u8; 1_000_000]; // 1MB value
        assert!(db.save("large_key", &large_value).is_ok());
        assert_eq!(db.find("large_key").unwrap(), Some(large_value));
    }

    #[test]
    fn test_multiple_operations() {
        let (_temp_dir, db) = create_temp_db();
        for i in 0..1000 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            assert!(db.save(&key, value.as_bytes()).is_ok());
        }

        for i in 0..1000 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);
            assert_eq!(db.find(&key).unwrap(), Some(expected_value.into_bytes()));
        }
    }
}
