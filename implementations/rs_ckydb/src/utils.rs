use crate::constants::{KEY_VALUE_SEPARATOR, TOKEN_SEPARATOR};
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::ErrorKind::AlreadyExists;
use std::io::{self, ErrorKind, ErrorKind::NotFound, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const DUMMY_FILE_DATA: [(&str, &str); 5] = [
    ("1655375120328185000.cky", "1655375120328185000-cow><?&(^#500 months$%#@*&^&1655375120328185100-dog><?&(^#23 months$%#@*&^&"),
    ("1655375120328186000.cky", "1655375171402014000-bar><?&(^#foo$%#@*&^&"),
    ("1655375171402014000.log", "1655404770518678-goat><?&(^#678 months$%#@*&^&1655404670510698-hen><?&(^#567 months$%#@*&^&1655404770534578-pig><?&(^#70 months$%#@*&^&1655403775538278-fish><?&(^#8990 months$%#@*&^&1655403795838278-foo><?&(^#890 months$%#@*&^&"),
    ("delete.del", "1655403795838278-foo$%#@*&^&1655375171402014000-bar$%#@*&^&"),
    ("index.idx", "cow><?&(^#1655375120328185000-cow$%#@*&^&dog><?&(^#1655375120328185100-dog$%#@*&^&goat><?&(^#1655404770518678-goat$%#@*&^&hen><?&(^#1655404670510698-hen$%#@*&^&pig><?&(^#1655404770534578-pig$%#@*&^&fish><?&(^#1655403775538278-fish$%#@*&^&"),
];

/// clears the dummy data files in the `db_path` to database
///
/// # Errors
///
/// See [fs::remove_dir_all]
// #[inline]
pub(crate) fn clear_dummy_file_data_in_db<P: AsRef<Path>>(db_path: P) -> io::Result<()> {
    fs::remove_dir_all(db_path).or_else(|err| match err.kind() {
        NotFound => Ok(()),
        _ => Err(err),
    })
}

/// Adds dummy file data to the database folder to fill the database with dummy data.
///
/// # Errors
///
/// See [fs::create_dir_all]
// #[inline]
pub(crate) fn add_dummy_file_data_in_db(db_path: &str) -> io::Result<()> {
    let db_path = Path::new(db_path);

    fs::create_dir_all(db_path)?;

    for (filename, content) in DUMMY_FILE_DATA {
        let file_path = db_path.join(filename);
        fs::write(file_path, content)?;
    }

    Ok(())
}

/// Reads all files in the `db_path` folder with the given extension e.g. only files with "log"
/// would include "user.log"
///
/// # Errors
///
/// See [fs::read_dir] and [fs::read_to_string]
// #[inline]
pub(crate) fn read_files_with_extension<P: AsRef<Path>>(
    db_path: P,
    ext: &str,
) -> io::Result<Vec<String>> {
    let mut contents: Vec<String> = vec![];

    for entry in fs::read_dir(db_path)? {
        let path = entry?.path();
        if let Some(extension) = path.extension() {
            if extension == ext {
                contents.push(fs::read_to_string(path)?);
            }
        }
    }

    Ok(contents)
}

/// Retrieves all files in the `db_path` folder with the given extensions `exts` e.g. only files with "log"
/// would include "user.log"
///
/// # Errors
///
/// See [fs::read_dir]
// #[inline]
pub(crate) fn get_files_with_extensions<P: AsRef<Path>>(
    db_path: P,
    exts: Vec<&str>,
) -> io::Result<Vec<String>> {
    let mut contents: Vec<String> = vec![];

    for entry in fs::read_dir(db_path)? {
        let entry = entry?;
        if let Some(extension) = entry.path().extension() {
            if exts.contains(&extension.to_os_string().to_str().unwrap_or("")) {
                let filename = entry.file_name().into_string().unwrap_or("".to_string());
                contents.push(filename);
            }
        }
    }

    Ok(contents)
}

/// Gets all the names of the files in the given folder
///
/// # Errors
///
/// See [fs::read_dir]
// #[inline]
pub(crate) fn get_file_names_in_folder<P: AsRef<Path>>(path: P) -> io::Result<Vec<String>> {
    fs::read_dir(path)?
        .map(|res| res.map(|e| e.file_name().into_string().unwrap_or("".to_string())))
        .collect()
}

/// Creates a given file if it does not exist
///
/// # Errors
///
/// See [fs::OpenOptions::open]
// #[inline]
pub(crate) fn create_file_if_not_exist<P: AsRef<Path>>(path: P) -> io::Result<()> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .and(Ok(()))
        .or_else(|err| {
            if err.kind() == AlreadyExists {
                Ok(())
            } else {
                Err(err)
            }
        })
}

/// Appends the supplied content to the file
///
/// # Errors
///
/// See [fs::OpenOptions::open] and [std::io::Write::write_all]
// #[inline]
pub(crate) fn append_to_file<P: AsRef<Path>>(path: P, content: &str) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).append(true).open(path)?;
    file.write_all(content.as_bytes())
}

/// Returns the current timestamp as a string.
///
/// # Errors
///
/// See [std::time::SystemTime::duration_since]
// #[inline]
pub(crate) fn get_current_timestamp_str() -> io::Result<String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .and_then(|d| Ok(d.as_nanos().to_string()))
        .or_else(|err| Err(io::Error::new(ErrorKind::Other, err)))
}

/// Extracts a hashmap of items and values from a string
///
/// # Error
///
/// This function might throw an [std::io::Error] of kind [std::io::InvalidData]
/// if the `content` string is malformed e.g. the key-values are not appropriately separated by
/// [crate::constants::KEY_VALUE_SEPARATOR]
// #[inline]
pub(crate) fn extract_key_values_from_str(content: &str) -> io::Result<HashMap<String, String>> {
    let kv_pair_strings = extract_tokens_from_str(content);
    let mut results: HashMap<String, String> = Default::default();

    for kv_pair_string in kv_pair_strings {
        let pair: Vec<&str> = kv_pair_string.split(KEY_VALUE_SEPARATOR).collect();
        if pair.len() != 2 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "key-value pair not separated on file",
            ));
        }

        results.insert(pair[0].to_string(), pair[1].to_string());
    }

    Ok(results)
}

/// Extracts tokens from a byte array
// #[inline]
pub(crate) fn extract_tokens_from_str(content: &str) -> Vec<String> {
    let trimmed_content = content.trim_end_matches(TOKEN_SEPARATOR);

    if trimmed_content == "" {
        return vec![];
    }

    trimmed_content
        .split(TOKEN_SEPARATOR)
        .map(String::from)
        .collect()
}

/// Deletes the key values corresponding to the keysToDelete
/// if those items exist in that file
///
/// # Errors
///
/// See [fs::read_to_string] and [fs::write]
pub(crate) fn delete_key_values_from_file<P: AsRef<Path>>(
    path: P,
    keys_to_delete: &Vec<String>,
) -> io::Result<()> {
    let keys_to_del_length = keys_to_delete.len();

    let content = fs::read_to_string(&path)?;
    let kv_pair_strings = extract_tokens_from_str(&content);
    let mut prefixes_to_delete: Vec<String> = Vec::with_capacity(keys_to_del_length);

    for i in 0..keys_to_del_length {
        prefixes_to_delete.push(format!("{}{}", keys_to_delete[i], KEY_VALUE_SEPARATOR));
    }

    let new_content = kv_pair_strings
        .into_iter()
        .filter(|kv| !has_any_of_prefixes(kv, &prefixes_to_delete))
        .fold("".to_string(), |accum, item| {
            format!("{}{}{}", accum, item, TOKEN_SEPARATOR)
        });

    fs::write(path, new_content)
}

/// checks if the string phrase has any of the prefixes i.e. starts with any of those prefixes
// #[inline]
fn has_any_of_prefixes(phrase: &str, prefixes: &Vec<String>) -> bool {
    for prefix in prefixes {
        if phrase.starts_with(prefix) {
            return true;
        }
    }

    false
}

/// Returns the size of the file at the given `path` in kilobytes
///
/// # Errors
///
/// See [std::fs::metadata]
// #[inline]
pub(crate) fn get_file_size<P: AsRef<Path>>(path: P) -> io::Result<f64> {
    let file_size_in_bytes = fs::metadata(path)?.len();
    Ok(file_size_in_bytes as f64 / 1024.0)
}
