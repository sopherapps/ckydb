use std::error::Error;
use std::fmt::{Display, Formatter};

/// Error thrown when key is not found in store
#[derive(Debug, Clone)]
pub struct NotFoundError;

impl Display for NotFoundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "not found")
    }
}

impl Error for NotFoundError {}

/// Error thrown when the data in the database is inconsistent
#[derive(Debug, Clone)]
pub struct CorruptedDataError;

impl Display for CorruptedDataError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "corrupted data: data on disk is inconsistent with that in memory")
    }
}

impl Error for CorruptedDataError {}

/// Error thrown when a back ground tasks is already running
/// and an attempt is made to start it again
#[derive(Debug, Clone)]
pub struct AlreadyRunningError;

impl Display for AlreadyRunningError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "task is already running")
    }
}

impl Error for AlreadyRunningError {}

/// Error thrown when a background task is not running
/// and an attempt to stop it
#[derive(Debug, Clone)]
pub struct NotRunningError;

impl Display for NotRunningError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "task is not running")
    }
}

impl Error for NotRunningError {}