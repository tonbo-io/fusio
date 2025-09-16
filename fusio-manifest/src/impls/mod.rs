// Group backend-specific implementations here to keep top-level stores clean.

#[cfg(any(feature = "aws-tokio", feature = "aws-wasm"))]
pub mod s3;

// Test-only in-memory backends mirroring the S3 layout.
#[cfg(test)]
pub mod mem;
