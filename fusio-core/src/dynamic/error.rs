use alloc::boxed::Box;

pub type BoxedError = Box<dyn core::error::Error + Send + Sync + 'static>;

pub struct Error {
    inner: BoxedError,
}

impl core::fmt::Debug for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(&self.inner, f)
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(&self.inner, f)
    }
}

impl core::error::Error for Error {}

impl From<BoxedError> for Error {
    fn from(inner: BoxedError) -> Self {
        Self { inner }
    }
}
