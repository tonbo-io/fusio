use std::{mem, sync::Arc};
use bytes::{BufMut, BytesMut};
use futures_util::StreamExt;
use http_body_util::Full;
use tokio::task;
use tokio::task::JoinHandle;
use crate::{
    dynamic::MaybeSendFuture,
    remotes::{aws::multipart_upload::MultipartUpload, serde::MultipartPart},
    Error, IoBuf, Write,
};

const S3_PART_MINIMUM_SIZE: usize = 5 * 1024 * 1024;

pub struct S3Writer {
    inner: Arc<MultipartUpload>,
    upload_id: Option<Arc<String>>,
    next_part_numer: usize,
    buf: BytesMut,

    handlers: Vec<JoinHandle<Result<MultipartPart, Error>>>,
}

impl S3Writer {
    pub fn new(inner: Arc<MultipartUpload>) -> Self {
        Self {
            inner,
            upload_id: None,
            next_part_numer: 0,
            buf: BytesMut::with_capacity(S3_PART_MINIMUM_SIZE),
            handlers: vec![],
        }
    }

    async fn upload_part<F>(&mut self, fn_bytes_init: F) -> Result<(), Error>
    where
        F: FnOnce() -> BytesMut,
    {
        let upload_id = match self.upload_id.clone() {
            None => {
                let upload_id = Arc::new(self.inner.initiate().await?);
                self.upload_id = Some(upload_id.clone());
                upload_id
            }
            Some(upload_id) => upload_id,
        };
        let part_num = self.next_part_numer;
        self.next_part_numer += 1;

        let upload = self.inner.clone();
        let bytes = mem::replace(&mut self.buf, fn_bytes_init()).freeze();
        self.handlers.push(task::spawn(async move {
            upload
                .upload_part(&upload_id, part_num, bytes.len(), Full::new(bytes))
                .await
        }));

        Ok(())
    }
}

impl Write for S3Writer {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        if self.buf.len() > S3_PART_MINIMUM_SIZE {
            if let Err(e) = self
                .upload_part(|| BytesMut::with_capacity(S3_PART_MINIMUM_SIZE))
                .await
            {
                return (Err(e), buf);
            }
        }
        self.buf.put(buf.as_slice());

        (Ok(()), buf)
    }

    async fn flush(&mut self) -> Result<(), Error> {
        if self.buf.len() > S3_PART_MINIMUM_SIZE {
            self.upload_part(BytesMut::new).await?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let Some(upload_id) = self.upload_id.clone() else {
            if !self.buf.is_empty() {
                let bytes = mem::replace(&mut self.buf, BytesMut::new()).freeze();

                self.inner
                    .upload_once(bytes.len(), Full::new(bytes))
                    .await?;
            }
            return Ok(());
        };
        if !self.buf.is_empty() {
            self.upload_part(BytesMut::new).await?;
        }
        let mut parts = Vec::with_capacity(self.handlers.len());
        for handle in self.handlers.drain(..) {
            parts.push(handle.await.map_err(|e| Error::Io(e.into()))??)
        }
        assert_eq!(self.next_part_numer, parts.len());
        self.inner.complete_part(&upload_id, &parts).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
}
