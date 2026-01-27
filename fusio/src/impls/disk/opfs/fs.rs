use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use js_sys::{
    wasm_bindgen::{JsCast, JsValue},
    Array, ArrayBuffer, JsString, Uint8Array,
};
use wasm_bindgen_futures::{stream::JsStream, JsFuture};
use web_sys::{
    DomException, File, FileSystemCreateWritableOptions, FileSystemDirectoryHandle,
    FileSystemFileHandle, FileSystemGetDirectoryOptions, FileSystemGetFileOptions,
    FileSystemRemoveOptions, FileSystemWritableFileStream,
};

#[cfg(not(feature = "sync"))]
use super::OPFSFile;
#[cfg(feature = "sync")]
use crate::disk::OPFSSyncFile;
use crate::{
    disk::opfs::{promise, storage},
    error::{wasm_err, Error},
    fs::{CasCondition, FileMeta, FileSystemTag, Fs, FsCas, OpenOptions},
    path::Path,
};

/// [OPFS](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system) backend
pub struct OPFS;

impl Fs for OPFS {
    #[cfg(feature = "sync")]
    type File = OPFSSyncFile;
    #[cfg(not(feature = "sync"))]
    type File = OPFSFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::OPFS
    }

    /// Open a [`OPFSFile`] with options.
    ///
    /// It is not permitted to use paths that temporarily step outside the sandbox with something
    /// like `../foo` or `./bar`. It is recommended to call [`Path::from_opfs_path`] or
    /// [`Path::parse`]
    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let segments: Vec<&str> = path.as_ref().trim_matches('/').split("/").collect();

        if segments.len() == 1 && segments[0].is_empty() {
            return Err(Error::Path(Box::new(crate::path::Error::EmptySegment {
                path: path.to_string(),
            })));
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

        cfg_if::cfg_if! {
            if #[cfg(feature = "sync")] {
                Self::File::new(file_handle, options).await
            } else {
                OPFSFile::new(file_handle, options).await
            }
        }
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
            for entry in entries {
                yield Ok(FileMeta{ path: path.child(entry), size: 0 })
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

    async fn exists(&self, path: &Path) -> Result<bool, Error> {
        let (dirs, file) = split_path(&path)?;
        get_file_handle(&dirs, &file, false, false).await
    }
}

fn is_not_found_error(err: &JsValue) -> bool {
    if let Some(dom) = err.dyn_ref::<DomException>() {
        dom.code() == DomException::NOT_FOUND_ERR || dom.name() == "NotFoundError"
    } else {
        false
    }
}

fn split_path(path: &Path) -> Result<(Vec<String>, String), Error> {
    let raw = path.as_ref().trim_matches('/');
    if raw.is_empty() {
        return Err(Error::Path(Box::new(crate::path::Error::EmptySegment {
            path: path.to_string(),
        })));
    }
    let mut segments = Vec::new();
    for segment in raw.split('/') {
        if segment.is_empty() {
            return Err(Error::Path(Box::new(crate::path::Error::EmptySegment {
                path: path.to_string(),
            })));
        }
        segments.push(segment.to_string());
    }
    let file = segments.pop().ok_or_else(|| {
        Error::Path(Box::new(crate::path::Error::EmptySegment {
            path: path.to_string(),
        }))
    })?;
    Ok((segments, file))
}

async fn walk_to_dir(
    dirs: &[String],
    create: bool,
) -> Result<Option<FileSystemDirectoryHandle>, Error> {
    let mut current = storage().await?;
    for dir in dirs {
        let opts = FileSystemGetDirectoryOptions::new();
        opts.set_create(create);
        match JsFuture::from(current.get_directory_handle_with_options(dir, &opts)).await {
            Ok(js) => {
                current = js
                    .dyn_into::<FileSystemDirectoryHandle>()
                    .map_err(|_| Error::CastError)?;
            }
            Err(err) => {
                if !create && is_not_found_error(&err) {
                    return Ok(None);
                }
                return Err(wasm_err(err));
            }
        }
    }
    Ok(Some(current))
}

async fn get_file_handle(
    dirs: &[String],
    file: &str,
    create_file: bool,
    create_dirs: bool,
) -> Result<Option<FileSystemFileHandle>, Error> {
    let Some(parent) = walk_to_dir(dirs, create_dirs).await? else {
        return Ok(None);
    };
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(create_file);
    match JsFuture::from(parent.get_file_handle_with_options(file, &opts)).await {
        Ok(js) => Ok(Some(
            js.dyn_into::<FileSystemFileHandle>()
                .map_err(|_| Error::CastError)?,
        )),
        Err(err) => {
            if !create_file && is_not_found_error(&err) {
                Ok(None)
            } else {
                Err(wasm_err(err))
            }
        }
    }
}

fn tag_from_file(file: &File) -> String {
    let size = file.size().round() as u64;
    let last_modified = file.last_modified() as u64;
    format!("{:016x}:{:016x}", size, last_modified)
}

async fn read_file_bytes(file: &File) -> Result<Vec<u8>, Error> {
    let buffer: ArrayBuffer = JsFuture::from(file.array_buffer())
        .await
        .map_err(wasm_err)?
        .dyn_into()
        .map_err(|_| Error::CastError)?;
    Ok(Uint8Array::new(&buffer).to_vec())
}

async fn current_tag(handle: &FileSystemFileHandle) -> Result<String, Error> {
    let file = promise::<File>(handle.get_file()).await?;
    Ok(tag_from_file(&file))
}

async fn write_fully(handle: &FileSystemFileHandle, payload: &[u8]) -> Result<(), Error> {
    let opts = FileSystemCreateWritableOptions::new();
    opts.set_keep_existing_data(false);
    let writer =
        promise::<FileSystemWritableFileStream>(handle.create_writable_with_options(&opts)).await?;
    let promise = writer.write_with_u8_array(payload).map_err(wasm_err)?;
    JsFuture::from(promise).await.map_err(wasm_err)?;
    JsFuture::from(writer.close()).await.map_err(wasm_err)?;
    Ok(())
}

impl FsCas for OPFS {
    fn load_with_tag(
        &self,
        path: &Path,
    ) -> std::pin::Pin<
        Box<
            dyn fusio_core::MaybeSendFuture<Output = Result<Option<(Vec<u8>, String)>, Error>> + '_,
        >,
    > {
        let path = path.clone();
        Box::pin(async move {
            let (dirs, file) = split_path(&path)?;
            let Some(handle) = get_file_handle(&dirs, &file, false, false).await? else {
                return Ok(None);
            };
            let file_obj = promise::<File>(handle.get_file()).await?;
            let tag = tag_from_file(&file_obj);
            let bytes = read_file_bytes(&file_obj).await?;
            Ok(Some((bytes, tag)))
        })
    }

    fn put_conditional(
        &self,
        path: &Path,
        payload: &[u8],
        content_type: Option<&str>,
        metadata: Option<Vec<(String, String)>>,
        condition: CasCondition,
    ) -> std::pin::Pin<Box<dyn fusio_core::MaybeSendFuture<Output = Result<String, Error>> + '_>>
    {
        let path = path.clone();
        let data = payload.to_vec();
        let _ = content_type;
        let _ = metadata;
        Box::pin(async move {
            let (dirs, file) = split_path(&path)?;
            let existing = get_file_handle(&dirs, &file, false, false).await?;

            match &condition {
                CasCondition::IfNotExists => {
                    if existing.is_some() {
                        return Err(Error::PreconditionFailed);
                    }
                }
                CasCondition::IfMatch(expected) => {
                    let Some(handle) = existing.as_ref() else {
                        return Err(Error::PreconditionFailed);
                    };
                    let tag = current_tag(handle).await?;
                    if &tag != expected {
                        return Err(Error::PreconditionFailed);
                    }
                }
            }

            let handle = match condition {
                CasCondition::IfMatch(_) => existing.expect("validated above"),
                CasCondition::IfNotExists => get_file_handle(&dirs, &file, true, true)
                    .await?
                    .ok_or(Error::PreconditionFailed)?,
            };

            write_fully(&handle, &data).await?;
            current_tag(&handle).await
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
                return Err(Error::Path(Box::new(crate::path::Error::EmptySegment {
                    path: path.to_string(),
                })));
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
                return Err(Error::Path(Box::new(crate::path::Error::EmptySegment {
                    path: path.to_string(),
                })));
            }
            parent = promise::<FileSystemDirectoryHandle>(
                parent.get_directory_handle_with_options(segment.as_ref(), options),
            )
            .await?;
        }
        Ok(parent)
    }
}
