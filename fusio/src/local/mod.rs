#[cfg(feature = "monoio")]
pub(crate) mod monoio;
#[cfg(feature = "tokio")]
pub(crate) mod tokio;
#[cfg(all(feature = "tokio-uring", target_os = "linux"))]
mod tokio_uring;

#[cfg(all(feature = "monoio", feature = "fs"))]
#[allow(unused)]
pub use monoio::fs::*;
#[cfg(all(feature = "tokio", feature = "fs"))]
#[allow(unused)]
pub use tokio::fs::*;
#[cfg(all(feature = "tokio-uring", target_os = "linux", feature = "fs"))]
#[allow(unused)]
pub use tokio_uring::fs::*;
