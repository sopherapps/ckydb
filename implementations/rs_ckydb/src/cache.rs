use crate::cky_map::CkyMap;
use crate::errors as ckydb;

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
    fn remove(&mut self, key: &str) -> ckydb::Result<String>;

    /// Updates the value corresponding to the passed `key` with the
    /// given `value`, returning the old value if any
    fn update(&mut self, key: &str, value: &str) -> ckydb::Result<Option<String>>;

    /// Retrieves the value corresponding to the given `key`
    fn get(&self, key: &str) -> ckydb::Result<String>;
}

/// `Cache` is the actual cache struct that caches data in memory
/// for a given data file. All the data on disk for the given
/// bounds `start` and `end` is loaded into `data`
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Cache {
    pub data: CkyMap,
    pub start: String,
    pub end: String,
}

impl Cache {
    /// Initializes a new Cache with the given `data`, and bounds (`start`, `end`)
    #[inline(always)]
    pub(crate) fn new(content: String, start: String, end: String) -> Cache {
        Cache {
            data: CkyMap::from(content),
            start,
            end,
        }
    }

    /// Initializes a new empty Cache with start: "0", end: "0" and data as empty Hashmap
    #[inline(always)]
    pub(crate) fn new_empty() -> Cache {
        Cache {
            data: Default::default(),
            start: "0".to_string(),
            end: "0".to_string(),
        }
    }

    /// Reloads the cache with the given data
    pub(crate) fn reload(&mut self, content: String, start: String, end: String) {
        self.data.reload_from_str(content);
        self.start = start;
        self.end = end;
    }
}

impl Caching for Cache {
    #[inline(always)]
    fn is_in_range(&self, key: &str) -> bool {
        &self.start[..] <= key && key <= &self.end[..]
    }

    #[inline(always)]
    fn remove(&mut self, key: &str) -> ckydb::Result<String> {
        self.data.delete(key)
    }

    #[inline(always)]
    fn update(&mut self, key: &str, value: &str) -> ckydb::Result<Option<String>> {
        self.data.insert(key, value)
    }

    #[inline(always)]
    fn get(&self, key: &str) -> ckydb::Result<String> {
        self.data.get(key)
    }
}
