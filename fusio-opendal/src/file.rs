use fusio::{fs::OpenOptions, Error, IoBuf, IoBufMut, Read, Write};
use opendal::{Operator, Reader, Writer};

use crate::utils::parse_opendal_error;

/// OpendalFile is the file created by OpendalFs.
pub struct OpendalFile {
    op: Operator,
    path: String,

    state: FileState,
}

enum FileState {
    Read(Reader),
    Write(Writer),
}

impl OpendalFile {
    /// Open a new file with given options.
    ///
    /// We mark this function as `pub(crate)` to make sure users can only create it from OpendalFs.
    pub(crate) async fn open(
        op: Operator,
        path: String,
        options: OpenOptions,
    ) -> Result<Self, Error> {
        // open as read
        if options.read && !options.write && !options.create && !options.truncate {
            let r = op
                .reader(path.as_ref())
                .await
                .map_err(parse_opendal_error)?;

            return Ok(Self {
                op,
                path,
                state: FileState::Read(r),
            });
        }

        // open as truncate write
        //
        // TODO: we only support `create && truncate` for now, maybe we can check if the file exists
        // before writing.
        if !options.read && options.write && options.create && options.truncate {
            let w = op
                .writer(path.as_ref())
                .await
                .map_err(parse_opendal_error)?;
            return Ok(Self {
                op,
                path,
                state: FileState::Write(w),
            });
        }

        // FIXME: `truncate` has different semantics with `append`, we should re-think it.
        if !options.read && options.write && !options.create && !options.truncate {
            let w = op
                .writer_with(path.as_ref())
                .append(true)
                .await
                .map_err(parse_opendal_error)?;
            return Ok(Self {
                op,
                path,
                state: FileState::Write(w),
            });
        }

        Err(Error::Unsupported {
            message: format!("open options {:?} is not supported", options),
        })
    }
}

impl Read for OpendalFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let FileState::Read(r) = &mut self.state else {
            return (
                Err(Error::Other("file is not open as read mode".into())),
                buf,
            );
        };
        let size = buf.as_slice_mut().len() as u64;
        let res = r
            .read_into(&mut buf.as_slice_mut(), pos..pos + size)
            .await
            .map(|_| ())
            .map_err(parse_opendal_error);

        (res, buf)
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let FileState::Read(r) = &mut self.state else {
            return (
                Err(Error::Other("file is not open as read mode".into())),
                buf,
            );
        };
        let res = r
            .read_into(&mut buf, pos..)
            .await
            .map(|_| ())
            .map_err(parse_opendal_error);

        (res, buf)
    }

    async fn size(&self) -> Result<u64, Error> {
        let meta = self
            .op
            .stat(&self.path)
            .await
            .map_err(parse_opendal_error)?;
        Ok(meta.content_length())
    }
}

impl Write for OpendalFile {
    /// TODO: opendal has native buffer support, maybe we can tune it while open writer.
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let FileState::Write(w) = &mut self.state else {
            return (
                Err(Error::Other("file is not open as write mode".into())),
                buf,
            );
        };

        let res = w
            .write(buf.as_bytes())
            .await
            .map(|_| ())
            .map_err(parse_opendal_error);
        (res, buf)
    }

    /// flush is no-op on opendal.
    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let FileState::Write(w) = &mut self.state else {
            return Err(Error::Other("file is not open as write mode".into()));
        };
        w.close().await.map_err(parse_opendal_error)
    }
}
