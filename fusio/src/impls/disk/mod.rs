#[cfg(feature = "monoio")]
pub(crate) mod monoio;
#[cfg(feature = "tokio")]
pub(crate) mod tokio;
#[cfg(all(feature = "tokio-uring", target_os = "linux"))]
pub(crate) mod tokio_uring;

#[cfg(all(feature = "monoio", feature = "fs"))]
#[allow(unused)]
pub use monoio::fs::*;
#[cfg(all(feature = "monoio", feature = "fs"))]
#[allow(unused)]
pub use monoio::MonoioFile;
#[cfg(all(feature = "tokio", feature = "fs"))]
#[allow(unused)]
pub use tokio::fs::*;
#[cfg(all(feature = "tokio-uring", target_os = "linux", feature = "fs"))]
#[allow(unused)]
pub use tokio_uring::fs::*;

#[cfg(feature = "fs")]
cfg_if::cfg_if! {
    if #[cfg(feature = "tokio")] {
        pub type LocalFs = TokioFs;
    } else if #[cfg(feature = "monoio")] {
        pub type LocalFs = MonoIoFs;
    } else if #[cfg(feature = "tokio-uring")] {
        pub type LocalFs = TokioUringFs;
    }
}
