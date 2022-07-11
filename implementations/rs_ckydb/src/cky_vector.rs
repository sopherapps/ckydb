use crate::constants::{TOKEN_SEPARATOR, TOKEN_SEPARATOR_LENGTH};
use crate::errors as ckydb;
use crate::errors::Error::{CorruptedDataError, NotFoundError};

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct CkyVector {
    k_string: String,
    inner_vector: Vec<String>,
    offsets: Vec<usize>,
}

impl Default for CkyVector {
    #[inline(always)]
    fn default() -> Self {
        CkyVector {
            k_string: "".to_owned(),
            inner_vector: Default::default(),
            offsets: Default::default(),
        }
    }
}

impl CkyVector {
    /// Returns a vector of the items that are found in this CkyVector
    #[inline(always)]
    pub(crate) fn items(&self) -> Vec<String> {
        self.inner_vector.to_owned()
    }

    /// Appends to the end of the vector a new value
    #[inline(always)]
    pub(crate) fn push(&mut self, value: &str) {
        self.inner_vector.push(value.to_owned());
        self.offsets.push(self.k_string.len());
        self.k_string
            .push_str(&format!("{}{}", value, TOKEN_SEPARATOR));
    }

    /// Gets the number of items in vector
    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.inner_vector.len()
    }

    /// Returns the value at the given index or returns None
    #[inline(always)]
    pub(crate) fn get(&self, i: usize) -> Option<&String> {
        self.inner_vector.get(i)
    }

    /// Removes the item at the given index and returns it or errors as NotFound
    ///
    /// # Errors
    ///
    /// See [CkyVector::___replace_k_string_section]
    #[inline(always)]
    pub(crate) fn remove(&mut self, i: usize) -> ckydb::Result<String> {
        if i < self.inner_vector.len() {
            let old_value = self.inner_vector.remove(i);
            let start = self.offsets[i];
            let end = start + old_value.len() + TOKEN_SEPARATOR_LENGTH;
            self.replace_k_string_section(start, end, "")?;
            self.offsets.remove(i);
            self.shift_offsets(i, start as isize - end as isize);
            return Ok(old_value);
        }

        Err(NotFoundError { key: i.to_string() })
    }

    /// Removes many indices at once atomically
    ///
    /// # Errors
    ///
    /// See [CkyVector::___replace_k_string_section]
    #[inline(always)]
    pub(crate) fn remove_many(&mut self, indices: Vec<usize>) -> ckydb::Result<Vec<String>> {
        let mut old_values: Vec<String> = vec![];
        for i in 0..indices.len() {
            // Every time an index is removed, the old indices become invalid
            // what was originally index 1, now points to 0
            let index = indices[i] - i;
            let v = self.remove(index)?;
            old_values.push(v);
        }

        Ok(old_values)
    }

    /// Reloads its internal structure to match the given string
    pub(crate) fn reload_from_str(&mut self, content: String) {
        self.offsets.clear();
        self.inner_vector.clear();
        self.k_string = content;

        let trimmed = self.k_string.trim_end_matches(TOKEN_SEPARATOR);
        if trimmed == "" {
            return;
        }

        let s_start = trimmed.as_ptr() as isize;

        for token in trimmed.split(TOKEN_SEPARATOR) {
            let start = (token.as_ptr() as isize - s_start) as usize;
            self.inner_vector.push(token.to_owned());
            self.offsets.push(start);
        }
    }

    /// Clears all data in this CkyVector
    pub(crate) fn clear(&mut self) {
        self.inner_vector.clear();
        self.offsets.clear();
        self.k_string.clear();
    }

    /// Replaces the section of the k_string under the hood with the given replacement
    ///
    /// # Errors
    ///
    /// It throws a [crate::errors::Error::CorruptedDataError] if the range given is beyond the permissible
    #[inline]
    fn replace_k_string_section(
        &mut self,
        start: usize,
        end: usize,
        replacement: &str,
    ) -> ckydb::Result<()> {
        let string_length = self.k_string.len();

        if end > string_length {
            return Err(CorruptedDataError {
                data: Some(format!(
                    "{} is beyond length of raw string of length {}",
                    end, string_length
                )),
            });
        }

        self.k_string.replace_range(start..end, replacement);

        Ok(())
    }

    /// Shifts all offsets to the right of start (inclusive) by the given delta
    pub fn shift_offsets(&mut self, index: usize, delta: isize) {
        for i in index..self.offsets.len() {
            let v = &self.offsets[i];
            let new_value = (v.to_owned() as isize + delta) as usize;
            self.offsets[i] = new_value;
        }
    }
}

impl From<String> for CkyVector {
    fn from(s: String) -> Self {
        let mut cky_vector = CkyVector::default();
        cky_vector.reload_from_str(s);
        cky_vector
    }
}

impl From<&str> for CkyVector {
    #[inline(always)]
    fn from(s: &str) -> Self {
        CkyVector::from(s.to_owned())
    }
}

impl From<&String> for CkyVector {
    #[inline(always)]
    fn from(s: &String) -> Self {
        CkyVector::from(s.to_owned())
    }
}

impl ToString for CkyVector {
    #[inline(always)]
    fn to_string(&self) -> String {
        return self.k_string.to_string();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn to_string_returns_raw_string() {
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&");
        let cky_vector = CkyVector::from(&s);
        assert_eq!(s, cky_vector.to_string());
    }

    #[test]
    fn clear_removes_all_data() {
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&");
        let mut cky_vector = CkyVector::from(&s);
        let empty_str_vector: Vec<String> = Default::default();
        let empty_usize_vector: Vec<usize> = Default::default();

        cky_vector.clear();

        assert_eq!("", cky_vector.to_string());
        assert_eq!(empty_str_vector, cky_vector.items());
        assert_eq!(empty_usize_vector, cky_vector.offsets);
    }

    #[test]
    fn reload_from_str_matches_internal_state_to_str() {
        let s = "1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&";
        let new_string = "foo$%#@*&^&bar$%#@*&^&bear$%#@*&^&";
        let expected_items = vec!["foo".to_string(), "bar".to_string(), "bear".to_string()];
        let mut got_items: Vec<String> = vec![];
        let mut cky_vector = CkyVector::from(s);
        cky_vector.reload_from_str(new_string.to_owned());

        for i in 0..expected_items.len() {
            let v = cky_vector.get(i).unwrap();
            got_items.push(v.to_owned());
        }

        assert_eq!(new_string, cky_vector.to_string());
        assert_eq!(expected_items, cky_vector.items());
        assert_eq!(cky_vector.len(), 3);
        assert_eq!(expected_items, got_items);
    }

    #[test]
    fn len_gets_number_of_items() {
        let s = "1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&";
        let cky_vector = CkyVector::from(s);
        assert_eq!(cky_vector.len(), 2);
    }

    #[test]
    fn get_returns_the_item_at_index_or_none() {
        let s = "1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&";
        let cky_vector = CkyVector::from(s);
        let mut got_items: Vec<String> = vec![];
        let expected_items = vec![
            "1655403795838278-foo".to_string(),
            "1655375171402013000-bar".to_string(),
        ];

        for i in 0..expected_items.len() {
            let v = cky_vector.get(i).unwrap();
            got_items.push(v.to_owned());
        }

        assert_eq!(cky_vector.len(), 2);
        assert_eq!(expected_items, got_items);
        assert_eq!(cky_vector.get(7), None);
    }

    #[test]
    fn remove_deletes_the_item_at_index_and_returns_it_or_errs() {
        let s = "1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&1655375171402014000-bear$%#@*&^&";
        let mut cky_vector = CkyVector::from(s);
        let expected_items = vec![
            "1655403795838278-foo".to_string(),
            "1655375171402014000-bear".to_string(),
        ];

        let result = cky_vector.remove(1).unwrap();
        match cky_vector.remove(7) {
            Ok(_) => {
                panic!("expected a Not Found Err")
            }
            Err(e) => {
                assert!(e.get_data().unwrap().contains(&format!("{}", 7)))
            }
        }

        assert_eq!(cky_vector.len(), 2);
        assert_eq!(result, "1655375171402013000-bar");
        assert_eq!(expected_items, cky_vector.items());
    }

    #[test]
    fn remove_many_deletes_items_at_indices_and_returns_them_or_errs() {
        let s = "1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&1655375171402014000-bear$%#@*&^&";
        let mut cky_vector = CkyVector::from(s);
        let expected_items = vec!["1655403795838278-foo".to_string()];

        let result = cky_vector.remove_many(vec![1, 2]).unwrap();
        match cky_vector.remove_many(vec![3]) {
            Ok(_) => {
                panic!("expected a Not Found Err")
            }
            Err(e) => {
                assert!(e.get_data().unwrap().contains(&format!("{}", 3)))
            }
        }

        assert_eq!(cky_vector.len(), 1);
        assert_eq!(
            result,
            vec![
                "1655375171402013000-bar".to_string(),
                "1655375171402014000-bear".to_string()
            ]
        );
        assert_eq!(expected_items, cky_vector.items());
    }

    #[test]
    fn items_converts_string_to_vector_of_items() {
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&");
        let cky_vector = CkyVector::from(s);
        let expected_items = vec!["1655403795838278-foo", "1655375171402013000-bar"];
        assert_eq!(expected_items, cky_vector.items());
    }

    #[test]
    fn push_appends_value_to_end_of_vector() {
        let value = "1655404770534578-pig";
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402013000-bar$%#@*&^&");
        let mut cky_vector = CkyVector::from(&s);
        let expected_items = vec!["1655403795838278-foo", "1655375171402013000-bar", value];
        let expected_kv_string = format!("{}{}{}", s, value, TOKEN_SEPARATOR);

        cky_vector.push(value);

        assert_eq!(expected_items, cky_vector.items());
        assert_eq!(expected_kv_string, cky_vector.to_string());
    }
}
