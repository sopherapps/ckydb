use std::sync::Mutex;

pub(crate) type Lock = Mutex<u8>;
