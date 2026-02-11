//! This module contains the `Fs` trait, which is used to abstract file system operations across
//! different file systems.

mod options;

use core::{future::Future, pin::Pin};

use fusio_core::MaybeSendFuture;
use futures_core::Stream;
pub use options::*;

use crate::{error::Error, path::Path, MaybeSend, MaybeSync, Read, Write};

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
    Memory,
}

pub trait Fs: MaybeSend + MaybeSync {
    //! This trait is used to abstract file system operations across different file systems.

    type File: Read + Write + MaybeSend + 'static;

    fn file_system(&self) -> FileSystemTag;

    fn open(&self, path: &Path) -> impl Future<Output = Result<Self::File, Error>> + MaybeSend {
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
    ) -> impl Future<Output = Result<impl Stream<Item = Result<FileMeta, Error>> + MaybeSend, Error>>
           + MaybeSend;

    fn remove(&self, path: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn copy(&self, from: &Path, to: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn link(&self, from: &Path, to: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn exists(&self, path: &Path) -> impl Future<Output = Result<bool, Error>> + MaybeSend;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasCondition {
    IfNotExists,
    IfMatch(String),
}

/// Optional CAS extensions for filesystems that support conditional object updates.
pub trait FsCas: MaybeSend + MaybeSync {
    fn load_with_tag(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(Vec<u8>, String)>, Error>> + '_>>;

    fn put_conditional(
        &self,
        path: &Path,
        payload: &[u8],
        content_type: Option<&str>,
        metadata: Option<Vec<(String, String)>>,
        condition: CasCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<String, Error>> + '_>>;
}

#[cfg(test)]
mod tests {
    #[cfg(all(
        feature = "tokio-http",
        feature = "tokio",
        feature = "aws",
        not(feature = "completion-based")
    ))]
    use crate::error::Error;

    #[ignore]
    #[cfg(all(
        feature = "tokio-http",
        feature = "tokio",
        feature = "aws",
        not(feature = "completion-based")
    ))]
    #[tokio::test]
    async fn test_diff_fs_copy() -> Result<(), Error> {
        use std::sync::Arc;

        use tempfile::TempDir;

        use crate::{
            fs::{Fs, OpenOptions},
            impls::disk::tokio::fs::TokioFs,
            path::Path,
            remotes::aws::{credential::AwsCredential, fs::AmazonS3Builder, s3::S3File},
            DynFs, Read, Write,
        };

        let tmp_dir = TempDir::new()?;
        let local_path = Path::from_absolute_path(&tmp_dir.as_ref().join("test.file"))
            .map_err(|err| Error::Path(Box::new(err)))?;
        let s3_path: Path = "s3_copy_test.file".into();

        let s3_fs = Arc::new(
            AmazonS3Builder::new("data".to_string())
                .endpoint("http://localhost:9000".to_string())
                .region("ap-southeast-1".to_string())
                .credential(AwsCredential {
                    key_id: "user".to_string(),
                    secret_key: "password".to_string(),
                    token: None,
                })
                .sign_payload(true)
                .build(),
        );
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

        let mut s3 = S3File::new(Arc::into_inner(s3_fs).unwrap(), s3_path.clone(), false);

        let size = s3.size().await.unwrap();
        assert_eq!(size, 31);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, "🎵never gonna give you up🎵".as_bytes());

        Ok(())
    }
}
