#[cfg(all(feature = "executor-web", not(target_arch = "wasm32")))]
compile_error!("`executor-web` is only supported on wasm32 targets.");

pub mod reader;
pub mod writer;
