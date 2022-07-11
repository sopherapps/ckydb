use crate::constants::{
    KEY_VALUE_SEPARATOR, KEY_VALUE_SEPARATOR_LENGTH, TOKEN_SEPARATOR, TOKEN_SEPARATOR_LENGTH,
};
use crate::errors as ckydb;
use crate::errors::Error::{CorruptedDataError, NotFoundError};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct CkyMap {
    pub(crate) kv_string: String,
    offsets: HashMap<String, usize>,
    pub(crate) inner_map: HashMap<String, String>,
}

impl Default for CkyMap {
    #[inline(always)]
    fn default() -> Self {
        CkyMap {
            kv_string: "".to_owned(),
            offsets: Default::default(),
            inner_map: Default::default(),
        }
    }
}

impl CkyMap {
    /// Returns a map of the key, values that are found in this CkyMap
    #[inline(always)]
    pub(crate) fn map(&self) -> HashMap<String, String> {
        self.inner_map.to_owned()
    }

    /// Deletes a key-value pair for the given key. It returns the old value if there was any.
    ///
    /// # Errors
    ///
    /// - Returns a [crate::errors::Error::CorruptedDataError] if the inner string is out of sync with
    /// the offsets
    /// - Returns a [crate::errors::Error::NotFoundError] when the key does not exist
    pub(crate) fn delete(&mut self, key: &str) -> ckydb::Result<String> {
        if let Some(value) = self.inner_map.remove(key) {
            if let Some(start) = self.offsets.get(key) {
                let start = start.to_owned();
                let end = start
                    + key.len()
                    + KEY_VALUE_SEPARATOR_LENGTH
                    + value.len()
                    + TOKEN_SEPARATOR_LENGTH;
                self.___replace_kv_string_section(start, end, "")?;
                self.offsets.remove(key);
                return Ok(value);
            }
            return Err(CorruptedDataError {
                data: Some("offsets and map out of sync in Tokneized string".to_string()),
            });
        }

        Err(NotFoundError {
            key: key.to_owned(),
        })
    }

    /// Gets the value corresponding to the given key
    ///
    /// # Errors
    ///
    /// - Returns a [crate::errors::Error::CorruptedDataError] if the inner string is out of sync with
    /// the offsets
    /// - Returns a [crate::errors::Error::NotFoundError] when the key does not exist
    #[inline(always)]
    pub(crate) fn get(&self, key: &str) -> ckydb::Result<String> {
        self.inner_map
            .get(key)
            .map(|v| v.to_owned())
            .ok_or(NotFoundError {
                key: key.to_owned(),
            })
    }

    /// Inserts a key-value pair into this CkyMap. It returns the old value if there was any or
    /// None if this is a new key
    ///
    /// # Errors
    ///
    /// Returns a [crate::errors::Error::CorruptedDataError] if the inner string is out of sync with
    /// the offsets
    pub(crate) fn insert(&mut self, key: &str, value: &str) -> ckydb::Result<Option<String>> {
        if let Some(old_value) = self.inner_map.insert(key.to_owned(), value.to_owned()) {
            if let Some(start) = self.offsets.get(key) {
                let start = start.to_owned() + key.len() + KEY_VALUE_SEPARATOR_LENGTH;
                let end = start + old_value.len();
                self.___replace_kv_string_section(start, end, value)?;
                return Ok(Some(old_value));
            }

            return Err(CorruptedDataError {
                data: Some("offsets and map out of sync in CkyVector".to_string()),
            });
        }

        self.offsets.insert(key.to_owned(), self.kv_string.len());
        self.kv_string.push_str(&format!(
            "{}{}{}{}",
            key, KEY_VALUE_SEPARATOR, value, TOKEN_SEPARATOR
        ));

        Ok(None)
    }

    /// Clears all data in this CkyMap
    pub(crate) fn clear(&mut self) {
        self.inner_map.clear();
        self.offsets.clear();
        self.kv_string.clear();
    }

    /// Reloads its internal structure to match the given string
    pub(crate) fn reload_from_str(&mut self, content: String) {
        self.offsets.clear();
        self.inner_map.clear();
        self.kv_string = content;
        let trimmed = self.kv_string.trim_end_matches(TOKEN_SEPARATOR);
        if trimmed == "" {
            return;
        }

        let s_start = trimmed.as_ptr() as isize;

        for kv_pair_str in trimmed.split(TOKEN_SEPARATOR) {
            let pair: Vec<&str> = kv_pair_str.split(KEY_VALUE_SEPARATOR).collect();

            let (key, value, offset) = match pair.len() {
                2 => {
                    let (key, value) = (pair[0], pair[1]);
                    let start = (key.as_ptr() as isize - s_start) as usize;
                    (key.to_owned(), value.to_owned(), start)
                }
                _ => continue,
            };

            self.offsets.insert(key.clone(), offset);
            self.inner_map.insert(key, value);
        }
    }

    /// Replaces the section of the kv_string under the hood with the given replacement
    ///
    /// # Errors
    ///
    /// It throws a [crate::errors::Error::CorruptedDataError] if the range given is beyond the permissible
    #[inline]
    fn ___replace_kv_string_section(
        &mut self,
        start: usize,
        end: usize,
        replacement: &str,
    ) -> ckydb::Result<()> {
        let string_length = self.kv_string.len();

        if end > string_length {
            return Err(CorruptedDataError {
                data: Some(format!(
                    "{} is beyond length of raw string of length {}",
                    end, string_length
                )),
            });
        }

        self.kv_string.replace_range(start..end, replacement);

        Ok(())
    }
}

impl From<String> for CkyMap {
    fn from(s: String) -> Self {
        let mut cky_map = CkyMap::default();
        cky_map.reload_from_str(s);
        cky_map
    }
}

impl From<&str> for CkyMap {
    #[inline(always)]
    fn from(s: &str) -> Self {
        CkyMap::from(s.to_owned())
    }
}

impl From<&String> for CkyMap {
    #[inline(always)]
    fn from(s: &String) -> Self {
        CkyMap::from(s.to_owned())
    }
}

impl ToString for CkyMap {
    #[inline(always)]
    fn to_string(&self) -> String {
        return self.kv_string.to_string();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn to_string_returns_raw_string() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let cky_map = CkyMap::from(&s);
        assert_eq!(s, cky_map.to_string());
    }

    #[test]
    fn clear_removes_all_data() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let mut cky_map = CkyMap::from(&s);
        let empty_str_map: HashMap<String, String> = Default::default();
        let empty_str_usize_map: HashMap<String, usize> = Default::default();

        cky_map.clear();

        assert_eq!("", cky_map.to_string());
        assert_eq!(empty_str_map, cky_map.map());
        assert_eq!(empty_str_usize_map, cky_map.offsets);
    }

    #[test]
    fn reload_from_str_matches_internal_state_to_str() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let new_string = "goat><?&(^#big$%#@*&^&hen><?&(^#ben$%#@*&^&pig><?&(^#bar$%#@*&^&fish><?&(^#bear$%#@*&^&".to_string();
        let expected_map = HashMap::from(
            [
                ("goat", "big"),
                ("hen", "ben"),
                ("pig", "bar"),
                ("fish", "bear"),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );
        let mut cky_map = CkyMap::from(&s);
        cky_map.reload_from_str(new_string.clone());
        let kv: HashMap<String, String> = expected_map
            .clone()
            .into_iter()
            .map(|(k, _)| (k.clone(), cky_map.get(&k).unwrap()))
            .collect();

        assert_eq!(new_string, cky_map.to_string());
        assert_eq!(expected_map, cky_map.map());
        assert_eq!(expected_map, kv);
    }

    #[test]
    fn map_converts_string_to_hash_map() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let cky_map = CkyMap::from(s);
        let expected_map = HashMap::from(
            [
                ("1655404770518678-goat", "678 months"),
                ("1655404670510698-hen", "567 months"),
                ("1655404770534578-pig", "70 months"),
                ("1655403775538278-fish", "8990 months"),
                ("1655403795838278-foo", "890 months"),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );

        assert_eq!(expected_map, cky_map.map());
    }

    #[test]
    fn get_returns_the_value_for_given_key() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let cky_map = CkyMap::from(s);
        let expected_records = [
            ("1655404770518678-goat", "678 months"),
            ("1655404670510698-hen", "567 months"),
            ("1655404770534578-pig", "70 months"),
            ("1655403775538278-fish", "8990 months"),
            ("1655403795838278-foo", "890 months"),
        ];
        let missing_keys = ["foo", "bar", "milk"];

        for (k, v) in expected_records {
            let got = cky_map.get(k).expect(&format!("get key: {}", k)).to_owned();
            assert_eq!(v, got);
        }

        for key in missing_keys {
            match cky_map.get(key) {
                Ok(_) => {
                    panic!("{} was supposed to return a NotFoundError", key)
                }
                Err(err) => {
                    assert_eq!(
                        err,
                        NotFoundError {
                            key: key.to_string()
                        }
                    )
                }
            }
        }
    }

    #[test]
    fn insert_key_value_adds_key_value_to_map() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let mut cky_map = CkyMap::from(&s);
        let (key, value) = ("1655404770534578-pig", "70 months");
        let expected_map = HashMap::from(
            [
                ("1655404770518678-goat", "678 months"),
                ("1655404670510698-hen", "567 months"),
                ("1655403775538278-fish", "8990 months"),
                ("1655403795838278-foo", "890 months"),
                (key, value),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );
        let expected_kv_string = format!(
            "{}{}{}{}{}",
            s, key, KEY_VALUE_SEPARATOR, value, TOKEN_SEPARATOR
        );

        let result = cky_map
            .insert(key, value)
            .expect(&format!("insert key: {}, value: {}", key, value));

        assert_eq!(result, None);
        assert_eq!(expected_map, cky_map.map());
        assert_eq!(expected_kv_string, cky_map.to_string());
    }

    #[test]
    fn insert_updates_a_preexisting_key_value() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let expected_kv_string = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#new stuff$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let mut cky_map = CkyMap::from(s);
        let (key, value) = ("1655404770534578-pig", "new stuff");
        let expected_map = HashMap::from(
            [
                ("1655404770518678-goat", "678 months"),
                ("1655404670510698-hen", "567 months"),
                (key, value),
                ("1655403775538278-fish", "8990 months"),
                ("1655403795838278-foo", "890 months"),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );

        let result = cky_map
            .insert(key, value)
            .expect(&format!("insert key: {}, value: {}", key, value));

        assert_eq!(result, Some("70 months".to_owned()));
        assert_eq!(expected_map, cky_map.map());
        assert_eq!(expected_kv_string, cky_map.to_string());
    }

    #[test]
    fn double_insert_overwrites_first_key() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&");
        let expected_kv_string = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#hello$%#@*&^&");
        let mut cky_map = CkyMap::from(s);
        let (key, value, new_value) = ("1655403795838278-foo", "890 months", "hello");
        let expected_map = HashMap::from(
            [
                ("1655404770518678-goat", "678 months"),
                ("1655404670510698-hen", "567 months"),
                ("1655403775538278-fish", "8990 months"),
                ("1655403795838278-foo", "hello"),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );

        cky_map
            .insert(key, value)
            .expect(&format!("insert key: {}, value: {}", key, value));

        let result = cky_map
            .insert(key, new_value)
            .expect(&format!("insert key: {}, value: {}", key, new_value));

        assert_eq!(result, Some("890 months".to_owned()));
        assert_eq!(expected_map, cky_map.map());
        assert_eq!(expected_kv_string, cky_map.to_string());
    }

    #[test]
    fn delete_removes_key_value_from_map() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let expected_kv_string = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let mut cky_map = CkyMap::from(s);
        let expected_map = HashMap::from(
            [
                ("1655404770518678-goat", "678 months"),
                ("1655404670510698-hen", "567 months"),
                ("1655403775538278-fish", "8990 months"),
                ("1655403795838278-foo", "890 months"),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );
        let key_to_delete = "1655404770534578-pig";

        let result = cky_map
            .delete(key_to_delete)
            .expect(&format!("delete {}", key_to_delete));

        assert_eq!(result, "70 months");
        assert_eq!(expected_map, cky_map.map());
        assert_eq!(expected_kv_string, cky_map.to_string());
    }
}
