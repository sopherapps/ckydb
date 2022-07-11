use crate::constants::TOKEN_SEPARATOR;

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct CkyVector {
    pub(crate) k_string: String,
    pub(crate) inner_vector: Vec<String>,
}

impl Default for CkyVector {
    #[inline(always)]
    fn default() -> Self {
        CkyVector {
            k_string: "".to_owned(),
            inner_vector: Default::default(),
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
        self.k_string
            .push_str(&format!("{}{}", value, TOKEN_SEPARATOR));
    }
}

impl From<String> for CkyVector {
    fn from(s: String) -> Self {
        let mut cky_vector = CkyVector {
            k_string: s,
            inner_vector: Default::default(),
        };

        let trimmed = cky_vector.k_string.trim_end_matches(TOKEN_SEPARATOR);
        if trimmed == "" {
            return cky_vector;
        }

        for key in trimmed.split(TOKEN_SEPARATOR) {
            cky_vector.inner_vector.push(key.to_owned());
        }

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
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&");
        let cky_vector = CkyVector::from(&s);
        assert_eq!(s, cky_vector.to_string());
    }

    #[test]
    fn keys_converts_string_to_vector_of_items() {
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&");
        let cky_vector = CkyVector::from(s);
        let expected_items = vec!["1655403795838278-foo", "1655375171402014000-bar"];
        assert_eq!(expected_items, cky_vector.items());
    }

    #[test]
    fn push_appends_value_to_end_of_vector() {
        let value = "1655404770534578-pig";
        let s = String::from("1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&");
        let mut cky_vector = CkyVector::from(&s);
        let expected_items = vec!["1655403795838278-foo", "1655375171402014000-bar", value];
        let expected_kv_string = format!("{}{}{}", s, value, TOKEN_SEPARATOR);

        cky_vector.push(value);

        assert_eq!(expected_items, cky_vector.items());
        assert_eq!(expected_kv_string, cky_vector.to_string());
    }
}
