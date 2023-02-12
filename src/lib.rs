//!
//! An in-memory key-value store.
//! Use hashmap to implement the get, set and remove menthod to support basic db behavior.
//!

use std::collections::HashMap;
/// `KvStore` structure contains a [`std::collections::HashMap`] as attribute `map` for supporting key/value in memory.
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// create a `KvStore`, initial an empty HashMap.
    pub fn new() -> Self {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// The function set a key and value to `KvStore`.
    /// # Example
    /// ``` rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("lcy".to_string(), "2002".to_string());
    /// assert_eq!(Some("2002".to_owned()), store.get("lcy".to_owned()));
    /// assert_eq!(None, store.get("cmj".to_owned()));
    ///
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// The function is to get a `value` for a `key`, return none if input `key` is not exists.
    /// # Example
    /// ``` rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("lcy".to_string(), "2002".to_string());
    /// assert_eq!(Some("2002".to_owned()), store.get("lcy".to_owned()));
    /// assert_eq!(None, store.get("cmj".to_owned()));
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        if let Some(value) = self.map.get(&key) {
            Some(value.to_owned())
        } else {
            None
        }
    }

    /// The function remove a key-value pair from `KvStore`.
    /// # Example
    /// ``` rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("lcy".to_string(), "2002".to_string());
    /// assert_eq!(Some("2002".to_owned()), store.get("lcy".to_owned()));
    /// store.remove("lcy".to_owned());
    /// assert_eq!(None, store.get("lcy".to_owned()));
    /// ```
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
