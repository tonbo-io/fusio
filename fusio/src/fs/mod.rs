//! This module contains the `Fs` trait, which is used to abstract file system operations across
//! different file systems.

mod options;

use std::future::Future;

use futures_core::Stream;
pub use options::*;

use crate::{path::Path, Error, MaybeSend, MaybeSync, Read, Write};

#[derive(Debug)]
pub struct FileMeta {
    pub path: Path,
    pub size: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileSystemTag {
    Local,
    OPFS,
    // TODO: Remote needs to check whether endpoint and other remote fs are consistent
    S3,
}

pub trait Fs: MaybeSend + MaybeSync {
    //! This trait is used to abstract file system operations across different file systems.

    type File: Read + Write + MaybeSend + 'static;

    fn file_system(&self) -> FileSystemTag;

    fn open(&self, path: &Path) -> impl Future<Output = Result<Self::File, Error>> {
        self.open_options(path, OpenOptions::default())
    }

    fn open_options(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> impl Future<Output = Result<Self::File, Error>> + MaybeSend;

    fn create_dir_all(path: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn list(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<FileMeta, Error>>, Error>> + MaybeSend;

    fn remove(&self, path: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn copy(&self, from: &Path, to: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn link(&self, from: &Path, to: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

#[cfg(test)]
mod tests {
    #[ignore]
    #[cfg(all(
        feature = "tokio-http",
        feature = "tokio",
        feature = "aws",
        not(feature = "completion-based")
    ))]
    #[tokio::test]
    async fn test_diff_fs_copy() -> Result<(), crate::Error> {
        use std::sync::Arc;

        use tempfile::TempDir;

        use crate::{
            fs::{Fs, OpenOptions},
            impls::disk::tokio::fs::TokioFs,
            path::Path,
            remotes::{
                aws::{credential::AwsCredential, fs::AmazonS3, options::S3Options, s3::S3File},
                http::tokio::TokioClient,
            },
            DynFs, Read, Write,
        };

        let tmp_dir = TempDir::new()?;
        let local_path = Path::from_absolute_path(&tmp_dir.as_ref().join("test.file"))?;
        let s3_path: Path = "s3_copy_test.file".into();

        let key_id = "user".to_string();
        let secret_key = "password".to_string();

        let client = TokioClient::new();
        let region = "ap-southeast-1";
        let options = S3Options {
            endpoint: "http://localhost:9000/data".into(),
            bucket: "data".to_string(),
            credential: Some(AwsCredential {
                key_id,
                secret_key,
                token: None,
            }),
            region: region.into(),
            sign_payload: true,
            checksum: false,
        };

        let s3_fs = Arc::new(AmazonS3::new(Box::new(client), options));
        let local_fs = Arc::new(TokioFs);

        {
            let mut local_file = Fs::open_options(
                local_fs.as_ref(),
                &local_path,
                OpenOptions::default().create(true).write(true),
            )
            .await?;
            local_file
                .write_all("🎵never gonna give you up🎵".as_bytes())
                .await
                .0?;
            local_file.close().await.unwrap();
        }
        {
            let s3_fs = s3_fs.clone() as Arc<dyn DynFs>;
            let local_fs = local_fs.clone() as Arc<dyn DynFs>;
            crate::dynamic::fs::copy(&local_fs, &local_path, &s3_fs, &s3_path).await?;
        }

        let mut s3 = S3File::new(Arc::into_inner(s3_fs).unwrap(), s3_path.clone());

        let size = s3.size().await.unwrap();
        assert_eq!(size, 31);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, "🎵never gonna give you up🎵".as_bytes());

        Ok(())
    }
}
