use core::future::Future;

/// A trait representing types that may or may not require [`Send`].
///
/// Many async runtimes do not require [`Send`] for futures and streams. This trait
/// provides a way to represent types that may not require [`Send`]. Users can
/// switch the feature `no-send` at compile-time to disable the [`Send`] bound.
///
/// # Safety
///
/// Do not implement this trait directly. It is automatically implemented for all
/// types based on the feature flags.
#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSend: Send {}

/// A trait representing types that may or may not require [`Send`].
///
/// When the `no-send` feature is enabled, this trait has no `Send` bound,
/// allowing use with single-threaded async runtimes.
///
/// # Safety
///
/// Do not implement this trait directly. It is automatically implemented for all types.
#[cfg(feature = "no-send")]
pub unsafe trait MaybeSend {}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Send> MaybeSend for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSend for T {}

#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSync: Sync {
    //! Same as [`MaybeSend`], but for [`std::marker::Sync`].
    //!
    //! # Safety
    //! Do not implement it directly.
}

#[cfg(feature = "no-send")]
pub unsafe trait MaybeSync {
    //! Same as [`MaybeSend`], but for [`std::marker::Sync`].
    //!
    //! # Safety
    //! Do not implement it directly.
}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Sync> MaybeSync for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSync for T {}

#[cfg(not(feature = "completion-based"))]
pub unsafe trait MaybeOwned {
    //! A trait for determining whether the buffer is owned or borrowed.
    //! Poll-based I/O operations require the buffer to be borrowed, while completion-based I/O
    //! operations require the buffer to be owned. This trait provides a way to abstract over
    //! the ownership of the buffer. Users could switch between poll-based and completion-based
    //! I/O operations at compile-time by enabling or disabling the `completion-based` feature.
    //!
    //! # Safety
    //! Do not implement this trait manually.
}
#[cfg(not(feature = "completion-based"))]
unsafe impl<T> MaybeOwned for T {}

#[cfg(feature = "completion-based")]
pub unsafe trait MaybeOwned: 'static {}

#[cfg(feature = "completion-based")]
unsafe impl<T: 'static> MaybeOwned for T {}

pub trait MaybeSendFuture: Future + MaybeSend {}

impl<F> MaybeSendFuture for F where F: Future + MaybeSend {}
