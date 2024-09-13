use std::io;

pub type Error = io::Error;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
