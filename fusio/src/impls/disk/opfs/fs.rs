use std::future::Future;

use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use js_sys::{
    wasm_bindgen::{JsCast, JsValue},
    Array, JsString,
};
use wasm_bindgen_futures::{stream::JsStream, JsFuture};
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemRemoveOptions,
};

use super::OPFSFile;
use crate::{
    disk::opfs::{promise, storage},
    error::wasm_err,
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::Path,
    Error,
};

/// [OPFS](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system) backend
pub struct OPFS;

impl Fs for OPFS {
    type File = OPFSFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::Local
    }

    /// Open a [`OPFSFile`] with options.
    ///
    /// It is not permitted to use paths that temporarily step outside the sandbox with something
    /// like `../foo` or `./bar`. It is recommended to call [`Path::from_opfs_path`] or
    /// [`Path::parse`]
    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let segments: Vec<&str> = path.as_ref().trim_matches('/').split("/").collect();

        if segments.len() == 1 && segments[0].is_empty() {
            return Err(Error::PathError(crate::path::Error::EmptySegment {
                path: path.to_string(),
            }));
        }

        let dir_options = FileSystemGetDirectoryOptions::new();
        dir_options.set_create(options.create);
        let parent = Self::access_parent_dir(path, &dir_options).await?;

        let file_name = segments.last().unwrap();
        let option = FileSystemGetFileOptions::new();
        option.set_create(options.create);

        let file_handle = promise::<FileSystemFileHandle>(
            parent.get_file_handle_with_options(file_name, &option),
        )
        .await?;

        Ok(OPFSFile::new(file_handle))
    }

    /// Recursively creates a directory and all of its parent components if they are missing.
    async fn create_dir_all(path: &Path) -> Result<(), Error> {
        let options = FileSystemGetDirectoryOptions::new();
        options.set_create(true);

        Self::access_dir(path, &options).await?;

        Ok(())
    }

    /// Returns an iterator over the entries within a directory.
    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let dir_options = FileSystemGetDirectoryOptions::new();
        dir_options.set_create(false);

        let dir = Self::access_dir(path, &dir_options).await?;

        let entries = JsStream::from(dir.entries())
            .map(|x| {
                let array: Vec<JsValue> = x.unwrap().dyn_into::<Array>().unwrap().to_vec();
                let path: String = array[0].clone().dyn_into::<JsString>().unwrap().into();
                path
            })
            .collect::<Vec<String>>()
            .await;

        Ok(stream! {
            for path in entries {
                yield Ok(FileMeta{ path: path.into(), size: 0 })
            }
        })
    }

    /// Recursively removes an entry from OPFS. See more detail in [removeEntry](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemDirectoryHandle/removeEntry)
    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let dir_options = FileSystemGetDirectoryOptions::new();
        dir_options.set_create(false);
        let parent = Self::access_parent_dir(path, &dir_options).await?;

        let removed_entry = path.as_ref().trim_matches('/').split("/").last().unwrap();
        let options = FileSystemRemoveOptions::new();
        options.set_recursive(true);
        JsFuture::from(parent.remove_entry_with_options(removed_entry, &options))
            .await
            .map_err(wasm_err)?;
        Ok(())
    }

    async fn copy(&self, _: &Path, _: &Path) -> Result<(), Error> {
        Err(Error::Unsupported {
            message: "opfs does not support copy file".to_string(),
        })
    }

    async fn link(&self, _: &Path, _: &Path) -> Result<(), Error> {
        Err(Error::Unsupported {
            message: "opfs does not support link file".to_string(),
        })
    }
}

impl OPFS {
    async fn access_dir(
        path: &Path,
        options: &FileSystemGetDirectoryOptions,
    ) -> Result<FileSystemDirectoryHandle, Error> {
        let mut parent = storage().await?;
        let segments: Vec<&str> = path.as_ref().trim_matches('/').split("/").collect();

        if segments.len() == 1 && segments[0].is_empty() {
            // "" case, return the root directory
            return Ok(parent);
        }
        for segment in segments {
            if segment.is_empty() {
                return Err(Error::PathError(crate::path::Error::EmptySegment {
                    path: path.to_string(),
                }));
            }
            parent = promise::<FileSystemDirectoryHandle>(
                parent.get_directory_handle_with_options(segment.as_ref(), options),
            )
            .await?;
        }
        Ok(parent)
    }

    /// Return the handle of parent directory. e.g. path "a/b/c" will return the handle of b
    async fn access_parent_dir(
        path: &Path,
        options: &FileSystemGetDirectoryOptions,
    ) -> Result<FileSystemDirectoryHandle, Error> {
        let mut parent = storage().await?;
        let segments: Vec<&str> = path.as_ref().trim_matches('/').split("/").collect();
        let part_len = segments.len();

        if part_len == 1 && segments[0].is_empty() {
            // "" case, return the root directory
            return Ok(parent);
        }
        for segment in &segments[0..part_len - 1] {
            if segment.is_empty() {
                return Err(Error::PathError(crate::path::Error::EmptySegment {
                    path: path.to_string(),
                }));
            }
            parent = promise::<FileSystemDirectoryHandle>(
                parent.get_directory_handle_with_options(segment.as_ref(), options),
            )
            .await?;
        }
        Ok(parent)
    }
}
