use crate::constants::{
    KEY_VALUE_SEPARATOR, KEY_VALUE_SEPARATOR_LENGTH, TOKEN_SEPARATOR, TOKEN_SEPARATOR_LENGTH,
};
use crate::errors as ckydb;
use crate::errors::Error::{CorruptedDataError, NotFoundError};
use std::collections::HashMap;

struct TokenizedString {
    kv_string: String,
    offsets: Vec<[(usize, usize); 2]>,
}

impl TokenizedString {
    /// Returns a map of the key, values that are found in this TokenizedString
    pub(crate) fn map(&self) -> HashMap<&str, &str> {
        let mut map: HashMap<&str, &str> = HashMap::with_capacity(self.offsets.len());

        for [(k_start, k_end), (v_start, v_end)] in &self.offsets {
            map.insert(
                &self.kv_string[*k_start..*k_end],
                &self.kv_string[*v_start..*v_end],
            );
        }

        map
    }

    /// Returns a vector of the keys that are found in this TokenizedString
    pub(crate) fn keys(&self) -> Vec<&str> {
        let mut keys: Vec<&str> = Vec::with_capacity(self.offsets.len());

        for [(k_start, k_end), _] in &self.offsets {
            keys.push(&self.kv_string[*k_start..*k_end]);
        }

        keys
    }

    /// Deletes a key-value pair for the given key
    ///
    /// # Errors
    ///
    /// - Returns a [crate::errors::Error::CorruptedDataError] if the inner string is out of sync with
    /// the offsets
    /// - Returns a [crate::errors::Error::NotFoundError] when the key does not exist
    pub(crate) fn delete(&mut self, key: &str) -> ckydb::Result<()> {
        // TODO: Later add some caching, memoizing; so as not to recalculate the keys
        for i in 0..self.offsets.len() {
            let [(k_start, k_end), (_, v_end)] = self.offsets[i];
            if &self.kv_string[k_start..k_end] == key {
                let end = v_end + TOKEN_SEPARATOR_LENGTH;
                self.___replace_kv_string_section(k_start, end, "")?;
                self.__remove_offset(i);
                return Ok(());
            }
        }

        Err(NotFoundError {
            key: key.to_owned(),
        })
    }

    /// Gets the value corresponding to the given key
    ///
    /// # Errors
    ///
    /// Returns a [crate::errors::Error::NotFoundError] when the key does not exist
    pub(crate) fn get(&self, key: &str) -> ckydb::Result<&str> {
        let map = self.map();
        map.get(key).and_then(|v| Some(*v)).ok_or(NotFoundError {
            key: key.to_owned(),
        })
    }

    /// Inserts a key-value pair into this TokenizedString
    ///
    /// # Errors
    ///
    /// Returns a [crate::errors::Error::CorruptedDataError] if the inner string is out of sync with
    /// the offsets
    pub(crate) fn insert(&mut self, key: &str, value: &str) -> ckydb::Result<()> {
        for i in 0..self.offsets.len() {
            let [(k_start, k_end), (v_start, v_end)] = self.offsets[i];
            if &self.kv_string[k_start..k_end] == key {
                self.___replace_kv_string_section(v_start, v_end, value)?;
                self.__replace_offset(i, [(k_start, k_end), (v_start, v_start + value.len())]);
                return Ok(());
            }
        }

        self.__append_key_value(key, value);
        Ok(())
    }

    /// Appends the key value pair to the end of this instance
    fn __append_key_value(&mut self, key: &str, value: &str) {
        let k_start = self.kv_string.len();
        let k_end = k_start + key.len();
        let v_start = if value == "" {
            k_end
        } else {
            k_end + KEY_VALUE_SEPARATOR_LENGTH
        };
        let v_end = v_start + value.len();

        self.kv_string.push_str(&format!(
            "{}{}{}{}",
            key, KEY_VALUE_SEPARATOR, value, TOKEN_SEPARATOR
        ));

        self.offsets.push([(k_start, k_end), (v_start, v_end)]);
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

    /// Removes the offset at the given index
    /// And lowers the offsets after it by that offset's
    #[inline]
    fn __remove_offset(&mut self, index: usize) {
        let [(start, _), (_, end)] = self.offsets[index];
        let delta = (end + TOKEN_SEPARATOR_LENGTH) - start;

        self.offsets.remove(index);
        for i in index..self.offsets.len() {
            let [(k_start, k_end), (v_start, v_end)] = self.offsets[i];
            self.offsets[i] = [
                (k_start - delta, k_end - delta),
                (v_start - delta, v_end - delta),
            ]
        }
    }

    /// Replaces the offset at the given index with the new offset
    /// And adds the appropriate delta to the offsets after it by that offset's increment in size
    fn __replace_offset(&mut self, index: usize, new: [(usize, usize); 2]) {
        let [_, (_, old_end)] = self.offsets[index];
        let [_, (_, new_end)] = new;
        let delta = old_end as isize - new_end as isize;

        self.offsets[index] = new;
        for i in (index + 1)..self.offsets.len() {
            let [(k_start, k_end), (v_start, v_end)] = self.offsets[i];

            let k_start = (k_start as isize + delta) as usize;
            let k_end = (k_end as isize + delta) as usize;
            let v_start = (v_start as isize + delta) as usize;
            let v_end = (v_end as isize + delta) as usize;

            self.offsets[i] = [(k_start, k_end), (v_start, v_end)]
        }
    }
}

impl From<String> for TokenizedString {
    fn from(s: String) -> Self {
        let mut token_string = TokenizedString {
            kv_string: s,
            offsets: vec![],
        };

        let trimmed = token_string.kv_string.trim_end_matches(TOKEN_SEPARATOR);
        if trimmed == "" {
            return token_string;
        }

        let s_start = trimmed.as_ptr() as isize;

        for kv_pair_str in trimmed.split(TOKEN_SEPARATOR) {
            let pair: Vec<&str> = kv_pair_str.split(KEY_VALUE_SEPARATOR).collect();
            let mut offsets: [(usize, usize); 2] = [(0, 0), (0, 0)];

            match pair.len() {
                1 => {
                    let token = pair[0];
                    let k_start = (token.as_ptr() as isize - s_start) as usize;
                    let k_end = k_start + token.len();
                    offsets = [(k_start, k_end), (k_end, k_end)]
                }
                2 => {
                    let (key, value) = (pair[0], pair[1]);
                    let k_start = (key.as_ptr() as isize - s_start) as usize;
                    let k_end = k_start + key.len();

                    let v_start = (value.as_ptr() as isize - s_start) as usize;
                    let v_end = v_start + value.len();
                    offsets = [(k_start, k_end), (v_start, v_end)]
                }
                _ => {}
            }

            token_string.offsets.push(offsets);
        }

        token_string
    }
}

impl ToString for TokenizedString {
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
        let token_string = TokenizedString::from(s.clone());
        assert_eq!(s, token_string.to_string());
    }

    #[test]
    fn map_converts_string_to_hash_map() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let token_string = TokenizedString::from(s.clone());
        let expected_map = HashMap::from([
            ("1655404770518678-goat", "678 months"),
            ("1655404670510698-hen", "567 months"),
            ("1655404770534578-pig", "70 months"),
            ("1655403775538278-fish", "8990 months"),
            ("1655403795838278-foo", "890 months"),
        ]);

        assert_eq!(expected_map, token_string.map());
    }

    #[test]
    fn keys_converts_string_to_vector_of_keys() {
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&");
        let token_string = TokenizedString::from(s.clone());
        let expected_keys = vec!["1655403795838278-foo", "1655375171402014000-bar"];

        assert_eq!(expected_keys, token_string.keys());
    }

    #[test]
    fn get_returns_the_value_for_given_key() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let token_string = TokenizedString::from(s.clone());
        let expected_records = [
            ("1655404770518678-goat", "678 months"),
            ("1655404670510698-hen", "567 months"),
            ("1655404770534578-pig", "70 months"),
            ("1655403775538278-fish", "8990 months"),
            ("1655403795838278-foo", "890 months"),
        ];
        let missing_keys = ["foo", "bar", "milk"];

        for (k, v) in expected_records {
            assert_eq!(v, token_string.get(k).expect(&format!("get key: {}", k)));
        }

        for key in missing_keys {
            match token_string.get(key) {
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
        let mut token_string = TokenizedString::from(s.clone());
        let (key, value) = ("1655404770534578-pig", "70 months");
        let expected_map = HashMap::from([
            ("1655404770518678-goat", "678 months"),
            ("1655404670510698-hen", "567 months"),
            ("1655403775538278-fish", "8990 months"),
            ("1655403795838278-foo", "890 months"),
            (key, value),
        ]);

        token_string
            .insert(key, value)
            .expect(&format!("insert key: {}, value: {}", key, value));

        assert_eq!(expected_map, token_string.map());
    }

    #[test]
    fn insert_updates_a_preexisting_key_value() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let mut token_string = TokenizedString::from(s.clone());
        let (key, value) = ("1655404770534578-pig", "new stuff");
        let expected_map = HashMap::from([
            ("1655404770518678-goat", "678 months"),
            ("1655404670510698-hen", "567 months"),
            (key, value),
            ("1655403775538278-fish", "8990 months"),
            ("1655403795838278-foo", "890 months"),
        ]);

        token_string
            .insert(key, value)
            .expect(&format!("insert key: {}, value: {}", key, value));

        assert_eq!(expected_map, token_string.map());
    }

    #[test]
    fn delete_removes_key_value_from_map() {
        let s = String::from("1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&");
        let mut token_string = TokenizedString::from(s.clone());
        let expected_map = HashMap::from([
            ("1655404770518678-goat", "678 months"),
            ("1655404670510698-hen", "567 months"),
            ("1655403775538278-fish", "8990 months"),
            ("1655403795838278-foo", "890 months"),
        ]);
        let key_to_delete = "1655404770534578-pig";

        token_string
            .delete(key_to_delete)
            .expect(&format!("delete {}", key_to_delete));

        assert_eq!(expected_map, token_string.map());
    }

    #[test]
    fn insert_key_adds_key_to_keys() {
        let (key, value) = ("1655404770534578-pig", "");
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&");
        let mut token_string = TokenizedString::from(s.clone());
        let expected_keys = vec!["1655403795838278-foo", "1655375171402014000-bar", key];

        token_string
            .insert(key, value)
            .expect(&format!("insert key: {}, value: {}", key, value));

        assert_eq!(expected_keys, token_string.keys());
    }

    #[test]
    fn insert_updates_a_preexisting_key() {
        let (key, value) = ("1655403795838278-foo", "");
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&");
        let mut token_string = TokenizedString::from(s.clone());
        let expected_keys = vec!["1655403795838278-foo", "1655375171402014000-bar"];

        token_string
            .insert(key, value)
            .expect(&format!("insert key: {}, value: {}", key, value));

        assert_eq!(expected_keys, token_string.keys());
    }

    #[test]
    fn delete_removes_key_from_keys() {
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&16553751714020132000-bear$%#@*&^&");
        let mut token_string = TokenizedString::from(s.clone());
        let key_to_delete = "1655375171402014000-bar";
        let expected_keys = vec!["1655403795838278-foo", "16553751714020132000-bear"];

        token_string
            .delete(key_to_delete)
            .expect(&format!("delete {}", key_to_delete));

        assert_eq!(expected_keys, token_string.keys());
    }
}
