use std::collections::HashMap;

/// `Caching` trait gives the basic representation of what
/// caches should be able to do
///
/// They should be able to:
///
/// - check if a key [is_in_range]
/// - [remove] a given key-value pair
/// - [update] the value corresponding to a given key
/// - [get] the value corresponding to the given key
///
/// [is_in_range]: Caching::is_in_range
/// [remove]: Caching::remove
/// [update]: Caching::update
/// [get]: Caching::get
pub(crate) trait Caching {
    /// Checks whether the passed `key` is within the cache's bounds
    fn is_in_range(&self, key: &str) -> bool;

    /// Removes the value corresponding to the passed `key`
    fn remove(&mut self, key: &str);

    /// Updates the value corresponding to the passed `key` with the
    /// given `value`
    fn update(&mut self, key: &str, value: &str);

    /// Retrieves the value corresponding to the given `key`
    fn get(&self, key: &str) -> Option<&String>;
}

/// `Cache` is the actual cache struct that caches data in memory
/// for a given data file. All the data on disk for the given
/// bounds `start` and `end` is loaded into `data`
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Cache {
    pub data: HashMap<String, String>,
    pub start: String,
    pub end: String,
}

impl Cache {
    /// Initializes a new Cache with the given `data`, and bounds (`start`, `end`)
    // #[inline]
    pub(crate) fn new(data: HashMap<String, String>, start: &str, end: &str) -> Cache {
        Cache {
            data,
            start: start.to_string(),
            end: end.to_string(),
        }
    }

    /// Initializes a new empty Cache with start: "0", end: "0" and data as empty Hashmap
    // #[inline]
    pub(crate) fn new_empty() -> Cache {
        Cache {
            data: Default::default(),
            start: "0".to_string(),
            end: "0".to_string(),
        }
    }
}

impl Caching for Cache {
    // #[inline]
    fn is_in_range(&self, key: &str) -> bool {
        let key = key.to_string();
        self.start <= key && key <= self.end
    }

    // #[inline]
    fn remove(&mut self, key: &str) {
        self.data.remove(key);
    }

    // #[inline]
    fn update(&mut self, key: &str, value: &str) {
        self.data.insert(key.to_string(), value.to_string());
    }

    // #[inline]
    fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }
}
