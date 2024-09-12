use std::future::Future;
use std::io;
use std::sync::Arc;

use http::{Request, Response};
use hyper::body::{Body, Incoming};
use hyper::rt::{Executor, Read, Write};
use thiserror::Error;

#[cfg(feature = "no-send")]
pub trait Client: 'static {
    type Stream: Read + Write + Unpin + 'static;

    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<Self::Stream>>;

    fn spawn<F>(&self, future: F)
    where
        F: Future + 'static;
}

#[cfg(not(feature = "no-send"))]
pub trait Client: Send + Sync + 'static {
    type Stream: Read + Write + Unpin + Send + 'static;

    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<Self::Stream>> + Send;

    fn spawn<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send;
}

pub(crate) struct HyperClient<C: Client>(Arc<C>);

impl<C: Client> Clone for HyperClient<C> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<C: Client> HyperClient<C> {
    pub(crate) async fn send<B>(&self, request: Request<B>) -> Result<Response<Incoming>, HttpError>
    where
        B: Body + Unpin + 'static + Send, // TODO: only require Send in send mode
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let addr = request
            .uri()
            .authority()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing authority in URI"))?
            .as_str();
        let stream = self.0.connect(addr).await?;
        match request.uri().scheme_str() {
            Some("http") | None => {
                let (mut io, conn) = hyper::client::conn::http1::handshake(stream).await?;
                self.0.spawn(async move {
                    if let Err(err) = conn.await {
                        println!("Connection failed: {:?}", err);
                    }
                });
                Ok(io.send_request(request).await?)
            }
            Some("https") => {
                let (mut io, conn) =
                    hyper::client::conn::http2::handshake(self.clone(), stream).await?;
                self.0.spawn(async move {
                    if let Err(err) = conn.await {
                        println!("Connection failed: {:?}", err);
                    }
                });
                Ok(io.send_request(request).await?)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "unsupported scheme").into()),
        }
    }
}

#[cfg(feature = "no-send")]
impl<F, T> Executor<F> for HyperClient<T>
where
    F: Future + 'static,
    T: Client,
{
    fn execute(&self, fut: F) {
        self.0.spawn(fut);
    }
}

#[cfg(not(feature = "no-send"))]
impl<F, T> Executor<F> for HyperClient<T>
where
    F: Future + Send + 'static,
    F::Output: Send,
    T: Client,
{
    fn execute(&self, fut: F) {
        self.0.spawn(fut);
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub enum HttpError {
    Io(#[from] io::Error),
    Hyper(#[from] hyper::Error),
    Http(#[from] http::Error),
}

#[cfg(all(feature = "tokio", not(feature = "no-send")))]
pub mod tokio_impl {
    use super::Client;
    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use hyper::rt::{Read, Write};
    use pin_project_lite::pin_project;
    use tokio::net::TcpStream;

    pub struct TokioClient;

    impl Client for TokioClient {
        type Stream = TokioIo<TcpStream>;

        async fn connect(&self, addr: &str) -> io::Result<Self::Stream> {
            TcpStream::connect(addr).await.map(TokioIo::new)
        }

        fn spawn<F>(&self, future: F)
        where
            F: Future + Send + 'static,
            F::Output: Send,
        {
            tokio::spawn(future);
        }
    }

    pin_project! {
        /// A wrapper that implements Tokio's IO traits for an inner type that
        /// implements hyper's IO traits, or vice versa (implements hyper's IO
        /// traits for a type that implements Tokio's IO traits).
        #[derive(Debug)]
        pub struct TokioIo<T> {
            #[pin]
            inner: T,
        }
    }

    impl<T> TokioIo<T> {
        /// Wrap a type implementing Tokio's or hyper's IO traits.
        pub fn new(inner: T) -> Self {
            Self { inner }
        }

        /// Borrow the inner type.
        pub fn inner(&self) -> &T {
            &self.inner
        }

        /// Mut borrow the inner type.
        pub fn inner_mut(&mut self) -> &mut T {
            &mut self.inner
        }

        /// Consume this wrapper and get the inner type.
        pub fn into_inner(self) -> T {
            self.inner
        }
    }

    impl<T> hyper::rt::Read for TokioIo<T>
    where
        T: tokio::io::AsyncRead,
    {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            mut buf: hyper::rt::ReadBufCursor<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            let n = unsafe {
                let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
                match tokio::io::AsyncRead::poll_read(self.project().inner, cx, &mut tbuf) {
                    Poll::Ready(Ok(())) => tbuf.filled().len(),
                    other => return other,
                }
            };

            unsafe {
                buf.advance(n);
            }
            Poll::Ready(Ok(()))
        }
    }

    impl<T> hyper::rt::Write for TokioIo<T>
    where
        T: tokio::io::AsyncWrite,
    {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            tokio::io::AsyncWrite::poll_write(self.project().inner, cx, buf)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            tokio::io::AsyncWrite::poll_flush(self.project().inner, cx)
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            tokio::io::AsyncWrite::poll_shutdown(self.project().inner, cx)
        }

        fn is_write_vectored(&self) -> bool {
            tokio::io::AsyncWrite::is_write_vectored(&self.inner)
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> Poll<Result<usize, std::io::Error>> {
            tokio::io::AsyncWrite::poll_write_vectored(self.project().inner, cx, bufs)
        }
    }

    impl<T> tokio::io::AsyncRead for TokioIo<T>
    where
        T: hyper::rt::Read,
    {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            tbuf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            //let init = tbuf.initialized().len();
            let filled = tbuf.filled().len();
            let sub_filled = unsafe {
                let mut buf = hyper::rt::ReadBuf::uninit(tbuf.unfilled_mut());

                match hyper::rt::Read::poll_read(self.project().inner, cx, buf.unfilled()) {
                    Poll::Ready(Ok(())) => buf.filled().len(),
                    other => return other,
                }
            };

            let n_filled = filled + sub_filled;
            // At least sub_filled bytes had to have been initialized.
            let n_init = sub_filled;
            unsafe {
                tbuf.assume_init(n_init);
                tbuf.set_filled(n_filled);
            }

            Poll::Ready(Ok(()))
        }
    }
}
