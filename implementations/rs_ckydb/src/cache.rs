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
    fn remove(&self, key: &str);

    /// Updates the value corresponding to the passed `key` with the
    /// given `value`
    fn update(&self, key: &str, value: &str);

    /// Retrieves the value corresponding to the given `key`
    fn get(&self, key: &str) -> String;
}

/// `Cache` is the actual cache struct that caches data in memory
/// for a given data file. All the data on disk for the given
/// bounds `start` and `end` is loaded into `data`
pub(crate) struct Cache {
    data: HashMap<String, String>,
    start: String,
    end: String,
}

impl Cache {
    /// Initializes a new Cache with the given `data`, and bounds (`start`, `end`)
    fn new(data: &HashMap<String, String>, start: &str, end: &str) -> Cache {
        todo!()
    }
}

impl Caching for Cache {
    fn is_in_range(&self, key: &str) -> bool {
        todo!()
    }

    fn remove(&self, key: &str) {
        todo!()
    }

    fn update(&self, key: &str, value: &str) {
        todo!()
    }

    fn get(&self, key: &str) -> String {
        todo!()
    }
}