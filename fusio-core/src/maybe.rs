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

/// A trait representing types that may or may not require [`Sync`].
///
/// Same as [`MaybeSend`], but for [`Sync`]. Users can switch the feature `no-send`
/// at compile-time to disable the [`Sync`] bound.
///
/// # Safety
///
/// Do not implement this trait directly. It is automatically implemented for all
/// types based on the feature flags.
#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSync: Sync {}

/// A trait representing types that may or may not require [`Sync`].
///
/// When the `no-send` feature is enabled, this trait has no `Sync` bound,
/// allowing use with single-threaded async runtimes.
///
/// # Safety
///
/// Do not implement this trait directly. It is automatically implemented for all types.
#[cfg(feature = "no-send")]
pub unsafe trait MaybeSync {}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Sync> MaybeSync for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSync for T {}

/// A trait for determining whether the buffer is owned or borrowed.
///
/// Poll-based I/O operations require the buffer to be borrowed, while completion-based I/O
/// operations require the buffer to be owned. This trait provides a way to abstract over
/// the ownership of the buffer. Users can switch between poll-based and completion-based
/// I/O operations at compile-time by enabling or disabling the `completion-based` feature.
///
/// # Safety
///
/// Do not implement this trait manually. It is automatically implemented based on
/// the feature flags.
#[cfg(not(feature = "completion-based"))]
pub unsafe trait MaybeOwned {}
#[cfg(not(feature = "completion-based"))]
unsafe impl<T> MaybeOwned for T {}

/// A trait for determining whether the buffer is owned or borrowed.
///
/// When the `completion-based` feature is enabled, this trait requires a `'static`
/// lifetime bound to ensure buffers can be safely used in completion-based I/O.
///
/// # Safety
///
/// Do not implement this trait manually. It is automatically implemented for all
/// types with a `'static` lifetime.
#[cfg(feature = "completion-based")]
pub unsafe trait MaybeOwned: 'static {}

#[cfg(feature = "completion-based")]
unsafe impl<T: 'static> MaybeOwned for T {}

/// A trait representing futures that may or may not require [`Send`].
///
/// This trait combines [`Future`] with [`MaybeSend`], providing a convenient
/// bound for async functions that may or may not require `Send` based on
/// the feature flags.
pub trait MaybeSendFuture: Future + MaybeSend {}

impl<F> MaybeSendFuture for F where F: Future + MaybeSend {}
