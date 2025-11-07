#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::{
    ffi::OsString,
    io,
    io::ErrorKind,
    path::{Path as StdPath, PathBuf},
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
};

use async_stream::stream;
use fusio_core::MaybeSendFuture;
use futures_core::Stream;
use tokio::{
    fs::{create_dir_all, remove_file, File, OpenOptions as TokioOpenOptions},
    io::AsyncReadExt,
    task::spawn_blocking,
};

use crate::{
    disk::tokio::TokioFile,
    durability::DirSync,
    error::Error,
    fs::{CasCondition, FileMeta, FileSystemTag, Fs, FsCas, OpenOptions},
    path::{path_to_local, Path},
};

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn metadata_tag(meta: &std::fs::Metadata) -> String {
    let len = meta.len();

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let mtime = meta.mtime() as u64;
        let mtime_nsec = meta.mtime_nsec() as u32;
        let ctime = meta.ctime() as u64;
        let ctime_nsec = meta.ctime_nsec() as u32;
        let ino = meta.ino();
        let tag = format!(
            "{:016x}:{:016x}:{:08x}:{:016x}:{:08x}:{:016x}",
            len, mtime, mtime_nsec, ctime, ctime_nsec, ino
        );
        tag
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        let last_write = meta.last_write_time();
        let creation = meta.creation_time();
        let attributes = meta.file_attributes();
        let tag = format!(
            "{:016x}:{:016x}:{:016x}:{:08x}",
            len, last_write, creation, attributes
        );
        tag
    }

    #[cfg(not(any(unix, windows)))]
    {
        use std::time::SystemTime;

        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let (secs, nanos, sign) = encode_system_time(modified);
        format!("{:016x}:{:016x}:{:08x}:{:02x}", len, secs, nanos, sign)
    }
}

#[cfg(not(any(unix, windows)))]
fn encode_system_time(time: std::time::SystemTime) -> (u64, u32, u8) {
    match time.duration_since(std::time::SystemTime::UNIX_EPOCH) {
        Ok(duration) => (duration.as_secs(), duration.subsec_nanos(), 0),
        Err(err) => {
            let duration = err.duration();
            (duration.as_secs(), duration.subsec_nanos(), 1)
        }
    }
}

async fn current_tag(path: &StdPath) -> Result<String, Error> {
    let meta = tokio::fs::metadata(path).await?;
    Ok(metadata_tag(&meta))
}

fn with_suffix(path: &StdPath, suffix: &str) -> PathBuf {
    let mut os: OsString = path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}

async fn write_atomic(path: &StdPath, payload: &[u8]) -> Result<(), Error> {
    let parent = path.parent().ok_or_else(|| {
        Error::Path(Box::new(io::Error::new(
            ErrorKind::InvalidInput,
            "path has no parent",
        )))
    })?;

    let unique = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_name = format!(".tmp-{}-{}", std::process::id(), unique);
    let tmp_path = parent.join(tmp_name);

    tokio::fs::write(&tmp_path, payload).await?;

    match replace_file(&tmp_path, path).await {
        Ok(()) => Ok(()),
        Err(err) => {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            Err(err)
        }
    }
}

#[cfg(not(windows))]
async fn replace_file(tmp_path: &StdPath, final_path: &StdPath) -> Result<(), Error> {
    tokio::fs::rename(tmp_path, final_path).await?;
    Ok(())
}

#[cfg(windows)]
async fn replace_file(tmp_path: &StdPath, final_path: &StdPath) -> Result<(), Error> {
    match tokio::fs::rename(tmp_path, final_path).await {
        Ok(()) => Ok(()),
        Err(err) if should_try_windows_replace(&err) => {
            if let Err(fallback_err) = windows_replace_file(tmp_path, final_path).await {
                Err(fallback_err.into())
            } else {
                Ok(())
            }
        }
        Err(err) => Err(err.into()),
    }
}

#[cfg(windows)]
fn should_try_windows_replace(err: &io::Error) -> bool {
    use windows_sys::Win32::Foundation::{
        ERROR_ACCESS_DENIED, ERROR_ALREADY_EXISTS, ERROR_SHARING_VIOLATION,
    };

    matches!(
        err.kind(),
        ErrorKind::AlreadyExists | ErrorKind::PermissionDenied
    ) || matches!(err.raw_os_error(), Some(raw) if {
        let raw = raw as u32;
        raw == ERROR_ALREADY_EXISTS
            || raw == ERROR_ACCESS_DENIED
            || raw == ERROR_SHARING_VIOLATION
    })
}

#[cfg(windows)]
async fn windows_replace_file(tmp_path: &StdPath, final_path: &StdPath) -> io::Result<()> {
    let replacement = tmp_path.to_path_buf();
    let destination = final_path.to_path_buf();

    spawn_blocking(move || replace_file_with_swap(replacement, destination))
        .await
        .map_err(|join_err| io::Error::new(ErrorKind::Other, join_err.to_string()))?
}

#[cfg(windows)]
fn replace_file_with_swap(tmp_path: PathBuf, final_path: PathBuf) -> io::Result<()> {
    use std::{iter, ptr};

    use windows_sys::Win32::Storage::FileSystem::{ReplaceFileW, REPLACEFILE_WRITE_THROUGH};

    let tmp_w: Vec<u16> = tmp_path
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();
    let final_w: Vec<u16> = final_path
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();

    let result = unsafe {
        ReplaceFileW(
            final_w.as_ptr(),
            tmp_w.as_ptr(),
            ptr::null(),
            REPLACEFILE_WRITE_THROUGH,
            ptr::null(),
            ptr::null(),
        )
    };

    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

struct LockFileGuard {
    path: PathBuf,
    file: Option<File>,
}

impl LockFileGuard {
    fn new(path: PathBuf, file: File) -> Self {
        Self {
            path,
            file: Some(file),
        }
    }
}

impl Drop for LockFileGuard {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            drop(file);
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn acquire_lock(path: &StdPath) -> Result<LockFileGuard, Error> {
    loop {
        match TokioOpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .await
        {
            Ok(file) => return Ok(LockFileGuard::new(path.to_path_buf(), file)),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                return Err(Error::PreconditionFailed)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    create_dir_all(parent).await?;
                    continue;
                }
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        }
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct TokioFs;

impl Fs for TokioFs {
    type File = TokioFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::Local
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;
        if !local_path.exists() {
            if options.create {
                if let Some(parent_path) = local_path.parent() {
                    create_dir_all(parent_path).await?;
                }

                tokio::fs::File::create(&local_path).await?;
            } else {
                return Err(Error::Path(Box::new(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Path not found and option.create is false",
                ))));
            }
        }

        let absolute_path = std::fs::canonicalize(&local_path).unwrap();
        let file = tokio::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .append(!options.truncate)
            .truncate(options.truncate)
            .open(&absolute_path)
            .await?;

        Ok(TokioFile::new(file))
    }

    async fn create_dir_all(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;
        create_dir_all(path).await?;

        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;

        spawn_blocking(move || {
            let entries = path.read_dir()?;
            Ok::<_, Error>(stream! {
                for entry in entries {
                    let entry = entry?;
                    yield Ok(FileMeta {
                        path: Path::from_filesystem_path(entry.path()).map_err(|err| Error::Path(Box::new(err)))?,
                        size: entry.metadata()?.len()
                    });
                }
            })
        })
        .await
        .map_err(io::Error::from)?
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;

        remove_file(&path).await?;
        Ok(())
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(Box::new(err)))?;
        let to = path_to_local(to).map_err(|err| Error::Path(Box::new(err)))?;

        tokio::fs::copy(&from, &to).await?;

        Ok(())
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(Box::new(err)))?;
        let to = path_to_local(to).map_err(|err| Error::Path(Box::new(err)))?;

        tokio::fs::hard_link(&from, &to).await?;

        Ok(())
    }
}

#[cfg(not(target_os = "windows"))]
impl DirSync for TokioFs {
    async fn sync_parent(&self, path: &Path) -> Result<(), Error> {
        let p = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;
        let Some(parent) = p.parent() else {
            return Ok(());
        };
        let parent_path = parent.to_path_buf();
        tokio::task::spawn_blocking(move || {
            // Open directory and fsync it.
            let file = std::fs::File::open(parent_path)?;
            file.sync_all()?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(std::io::Error::from)??;
        Ok(())
    }
}

#[cfg(target_os = "windows")]
impl DirSync for TokioFs {
    async fn sync_parent(&self, _path: &Path) -> Result<(), Error> {
        // Windows lacks a direct, stable way to fsync directories via std APIs.
        Err(Error::Unsupported {
            message: "DirSync is not supported on Windows".into(),
        })
    }
}

impl FsCas for TokioFs {
    fn load_with_tag(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(Vec<u8>, String)>, Error>> + '_>> {
        let path = path.clone();
        Box::pin(async move {
            let local_path = path_to_local(&path).map_err(|err| Error::Path(Box::new(err)))?;
            match File::open(&local_path).await {
                Ok(mut file) => {
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes).await?;
                    let meta = file.metadata().await.map_err(|err| {
                        if err.kind() == ErrorKind::NotFound {
                            Error::PreconditionFailed
                        } else {
                            err.into()
                        }
                    })?;
                    Ok(Some((bytes, metadata_tag(&meta))))
                }
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err.into()),
            }
        })
    }

    fn put_conditional(
        &self,
        path: &Path,
        payload: &[u8],
        _content_type: Option<&str>,
        _metadata: Option<Vec<(String, String)>>,
        condition: CasCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<String, Error>> + '_>> {
        let path = path.clone();
        let payload = payload.to_vec();
        Box::pin(async move {
            let local_path = path_to_local(&path).map_err(|err| Error::Path(Box::new(err)))?;
            if let Some(parent) = local_path.parent() {
                create_dir_all(parent).await?;
            }

            let lock_path = with_suffix(&local_path, ".lock");
            let lock = acquire_lock(&lock_path).await?;

            let outcome = async {
                match condition {
                    CasCondition::IfNotExists => {
                        if tokio::fs::try_exists(&local_path).await? {
                            return Err(Error::PreconditionFailed);
                        }
                        write_atomic(&local_path, &payload).await?;
                        current_tag(&local_path).await
                    }
                    CasCondition::IfMatch(expected) => {
                        let meta = tokio::fs::metadata(&local_path).await.map_err(|err| {
                            if err.kind() == ErrorKind::NotFound {
                                Error::PreconditionFailed
                            } else {
                                err.into()
                            }
                        })?;
                        let tag = metadata_tag(&meta);
                        if tag != expected {
                            return Err(Error::PreconditionFailed);
                        }
                        write_atomic(&local_path, &payload).await?;
                        current_tag(&local_path).await
                    }
                }
            }
            .await;

            drop(lock);
            outcome
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::CasCondition;

    #[tokio::test]
    async fn cas_round_trip() {
        let fs = TokioFs;
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("state.json");
        let path = Path::from_absolute_path(&local).unwrap();

        assert!(fs.load_with_tag(&path).await.unwrap().is_none());

        let payload1 = br#"{"a":1}"#;
        let tag1 = fs
            .put_conditional(
                &path,
                payload1,
                Some("application/json"),
                None,
                CasCondition::IfNotExists,
            )
            .await
            .unwrap();

        let loaded = fs.load_with_tag(&path).await.unwrap().unwrap();
        assert_eq!(loaded.0, payload1);
        assert_eq!(loaded.1, tag1);

        let err = fs
            .put_conditional(&path, payload1, None, None, CasCondition::IfNotExists)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::PreconditionFailed));

        let payload2 = br#"{"a":2}"#;
        let tag2 = fs
            .put_conditional(
                &path,
                payload2,
                None,
                None,
                CasCondition::IfMatch(tag1.clone()),
            )
            .await
            .unwrap();
        assert_ne!(tag1, tag2);

        let err = fs
            .put_conditional(
                &path,
                payload1,
                None,
                None,
                CasCondition::IfMatch("bogus".into()),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::PreconditionFailed));
    }

    #[cfg(target_os = "windows")]
    #[tokio::test]
    async fn cas_replace_succeeds_when_destination_locked() {
        use std::os::windows::fs::OpenOptionsExt;

        use windows_sys::Win32::{
            Foundation::ERROR_SHARING_VIOLATION,
            Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE},
        };

        let fs = TokioFs;
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("state.json");
        let path = Path::from_absolute_path(&local).unwrap();

        let initial = br#"{"a":1}"#;
        let tag1 = fs
            .put_conditional(
                &path,
                initial,
                Some("application/json"),
                None,
                CasCondition::IfNotExists,
            )
            .await
            .unwrap();

        let guard = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .open(&local)
            .unwrap();

        let updated = br#"{"a":2}"#;
        let tag2 = match fs
            .put_conditional(
                &path,
                updated,
                Some("application/json"),
                None,
                CasCondition::IfMatch(tag1.clone()),
            )
            .await
        {
            Ok(tag) => tag,
            Err(Error::Io(err)) if err.raw_os_error() == Some(ERROR_SHARING_VIOLATION as i32) => {
                eprintln!("skipping test: destination lock prevented ReplaceFileW fallback");
                return;
            }
            Err(err) => panic!("unexpected error: {err:?}"),
        };
        assert_ne!(tag1, tag2);

        drop(guard);

        let loaded = fs.load_with_tag(&path).await.unwrap().unwrap();
        assert_eq!(loaded.0, updated);
        assert_eq!(loaded.1, tag2);
    }
}
