// Group backend-specific implementations here to keep top-level stores clean.

pub mod fs;

// Test-only in-memory backends mirroring the S3 layout.
#[cfg(test)]
pub mod mem;
