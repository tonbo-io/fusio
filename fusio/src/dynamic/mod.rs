//! Dyn compatible(object safety) version of [`Read`], [`Write`] and others.

#[cfg(feature = "fs")]
pub mod fs;

#[cfg(feature = "fs")]
pub use fs::{DynFile, DynFs};
pub use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use futures_core::Stream;

pub trait MaybeSendStream: Stream + MaybeSend {}

impl<S> MaybeSendStream for S where S: Stream + MaybeSend {}
