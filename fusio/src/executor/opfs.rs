//! Back-compat shims for the OPFS executor.
//! Prefer `executor::web::WebExecutor` for wasm environments without OPFS.

pub use super::web::{WebExecutor, WebJoinHandle};

/// Alias maintained for existing OPFS users; the implementation lives in
/// `executor::web`.
pub type OpfsExecutor = WebExecutor;

/// Alias maintained for existing OPFS users; see [`WebJoinHandle`].
pub type OpfsJoinHandle<R> = WebJoinHandle<R>;
