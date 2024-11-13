//! This module contains the `Fs` trait, which is used to abstract file system operations across
//! different file systems.

mod options;

use std::{cmp, future::Future};

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

    fn link<F: Fs>(
        &self,
        from: &Path,
        to_fs: &F,
        to: &Path,
    ) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

pub async fn copy<F, T>(from_fs: &F, from: &Path, to_fs: &T, to: &Path) -> Result<(), Error>
where
    F: Fs,
    T: Fs,
{
    if from_fs.file_system() == to_fs.file_system() {
        from_fs.copy(from, to).await?;
        return Ok(());
    }
    let mut from_file = from_fs
        .open_options(from, OpenOptions::default().read(true))
        .await?;
    let from_file_size = from_file.size().await? as usize;

    let mut to_file = to_fs
        .open_options(to, OpenOptions::default().create(true).write(true))
        .await?;
    let buf_size = cmp::min(from_file_size, 4 * 1024);
    let mut buf = Some(vec![0u8; buf_size]);
    let mut read_pos = 0u64;

    while (read_pos as usize) < from_file_size - 1 {
        let tmp = buf.take().unwrap();
        let (result, tmp) = from_file.read_exact_at(tmp, read_pos).await;
        result?;
        read_pos += tmp.len() as u64;

        let (result, tmp) = to_file.write_all(tmp).await;
        result?;
        buf = Some(tmp);
    }
    to_file.close().await?;

    Ok(())
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
            fs,
            fs::{Fs, OpenOptions},
            impls::disk::tokio::fs::TokioFs,
            path::Path,
            remotes::{
                aws::{credential::AwsCredential, fs::AmazonS3, options::S3Options, s3::S3File},
                http::{tokio::TokioClient, DynHttpClient, HttpClient},
            },
            Read, Write,
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

        let s3_fs = AmazonS3::new(Box::new(client), options);
        let local_fs = TokioFs;

        {
            let mut local_file = local_fs
                .open_options(&local_path, OpenOptions::default().create(true).write(true))
                .await?;
            local_file
                .write_all("🎵never gonna give you up🎵".as_bytes())
                .await
                .0?;
            local_file.close().await.unwrap();
        }
        fs::copy(&local_fs, &local_path, &s3_fs, &s3_path).await?;

        let mut s3 = S3File::new(s3_fs, s3_path.clone());

        let size = s3.size().await.unwrap();
        assert_eq!(size, 31);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, "🎵never gonna give you up🎵".as_bytes());

        Ok(())
    }
}
