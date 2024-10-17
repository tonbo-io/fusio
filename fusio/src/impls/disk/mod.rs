#[cfg(feature = "monoio")]
pub(crate) mod monoio;
#[cfg(feature = "tokio")]
pub(crate) mod tokio;
#[cfg(all(feature = "tokio-uring", target_os = "linux"))]
pub(crate) mod tokio_uring;

#[cfg(all(feature = "monoio", feature = "fs"))]
#[allow(unused)]
pub use monoio::fs::*;
#[cfg(feature = "monoio")]
#[allow(unused)]
pub use monoio::MonoioFile;
#[cfg(all(feature = "tokio", feature = "fs"))]
#[allow(unused)]
pub use tokio::fs::*;
#[cfg(all(feature = "tokio-uring", target_os = "linux", feature = "fs"))]
#[allow(unused)]
pub use tokio_uring::fs::*;
#[cfg(all(feature = "tokio-uring", target_os = "linux"))]
#[allow(unused)]
pub use tokio_uring::TokioUringFile;

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

#[cfg(any(feature = "monoio", feature = "tokio-uring"))]
pub(crate) struct Position {
    read_pos: u64,
    write_pos: u64,
}

#[cfg(any(feature = "monoio", feature = "tokio-uring"))]
impl Default for Position {
    fn default() -> Self {
        Position {
            read_pos: 0,
            write_pos: 0,
        }
    }
}

#[cfg(any(feature = "monoio", feature = "tokio-uring"))]
impl Position {
    pub(crate) fn write_pos(&self) -> u64 {
        self.write_pos
    }
    pub(crate) fn read_pos(&self) -> u64 {
        self.read_pos
    }
    pub(crate) fn write_accumulation(&mut self, len: u64) {
        self.write_pos += len;
    }
    pub(crate) fn read_accumulation(&mut self, len: u64) {
        self.read_pos += len;
    }
    pub(crate) fn seek(&mut self, pos: u64) {
        self.read_pos = pos;
    }
}
