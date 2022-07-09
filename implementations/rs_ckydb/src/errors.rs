use crate::errors::Error::CorruptedDataError;
use std::fmt::{Display, Formatter};
use std::{error, io, result};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// Error thrown when key is not found in store
    NotFoundError { key: String },
    /// Error thrown when the data in the database is inconsistent
    CorruptedDataError { data: Option<String> },
    /// Error thrown when a back ground tasks is already running
    /// and an attempt is made to start it again
    AlreadyRunningError,
    /// Error thrown when a background task is not running
    /// and an attempt to stop it
    NotRunningError,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFoundError { key } => {
                write!(f, "{} not found", key)
            }
            Error::CorruptedDataError { data } => {
                write!(f, "corrupted: {}", data.clone().unwrap_or("".to_string()))
            }
            Error::AlreadyRunningError => {
                write!(f, "already running")
            }
            Error::NotRunningError => {
                write!(f, "not running")
            }
        }
    }
}

impl error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        CorruptedDataError {
            data: Some(err.to_string()),
        }
    }
}

impl Error {
    /// Returns the inner data in the error for ease of printing
    #[inline]
    pub fn get_data(&self) -> Option<String> {
        match self {
            Error::NotFoundError { key } => Some(key.to_string()),
            Error::CorruptedDataError { data } => data.clone(),
            Error::AlreadyRunningError => None,
            Error::NotRunningError => None,
        }
    }
}
