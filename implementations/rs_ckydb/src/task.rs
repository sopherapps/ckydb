use std::io;

/// The `Worker` trait gives the basic representation of what a background
/// worker/task should be able to do
///
/// It should be able to:
/// - be [started]
/// - be [stopped]
/// - check whether is it [is_running]
///
/// [started]: Worker::start
/// [stopped]: Worker::stop
/// [is_running]: Worker::is_running
pub(crate) trait Worker {
    /// Starts the worker in the background (a separate thread)
    ///
    /// # Errors
    /// - [io::Error] errors may occur may be if the thread fails to start
    ///
    /// [io::Error]: std::io::Error
    fn start(&self) -> io::Result<()>;

    /// Stops the worker's execution, and only returns after it really has stopped
    ///
    /// # Errors
    /// - [io::Error] errors may occur may be if the thread fails to start
    ///
    /// [io::Error]: std::io::Error
    fn stop(&self) -> io::Result<()>;

    /// Checks to see if the worker is still running i.e. has been started and
    /// has not been stopped yet.
    fn is_running(&self) -> bool;
}

/// `Task` is a type of [Worker]
pub(crate) struct Task {}

impl Task {
    /// Initializes a new `Task`
    pub(crate) fn new() -> Task {
        todo!()
    }
}

impl Worker for Task {
    fn start(&self) -> io::Result<()> {
        todo!()
    }

    fn stop(&self) -> io::Result<()> {
        todo!()
    }

    fn is_running(&self) -> bool {
        todo!()
    }
}
