use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use fusio_core::MaybeSendFuture;
use futures_core::Stream;
use futures_util::stream;

use crate::{
    durability::FileCommit,
    error::Error,
    fs::{CasCondition, FileMeta, FileSystemTag, Fs, FsCas, OpenOptions},
    path::Path,
    IoBuf, IoBufMut, Read, Write,
};

#[derive(Default)]
struct EntryState {
    data: Vec<u8>,
    metadata: HashMap<String, String>,
    content_type: Option<String>,
    etag: String,
}

#[derive(Default)]
struct Entries {
    objects: RwLock<HashMap<String, Arc<Entry>>>,
    counter: AtomicU64,
}

impl Entries {
    fn next_tag(&self) -> String {
        let next = self.counter.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        format!("{:016x}", next)
    }
}

struct Entry {
    state: Mutex<EntryState>,
}

impl Entry {
    fn new(etag: String) -> Self {
        Self {
            state: Mutex::new(EntryState {
                data: Vec::new(),
                metadata: HashMap::new(),
                content_type: None,
                etag,
            }),
        }
    }
}

#[derive(Clone, Default)]
pub struct InMemoryFs {
    inner: Arc<Entries>,
}

impl InMemoryFs {
    pub fn new() -> Self {
        Self::default()
    }

    fn path_key(path: &Path) -> String {
        path.as_ref().to_string()
    }

    fn or_create_entry(&self, key: &str, create: bool) -> Result<Arc<Entry>, Error> {
        let mut guard = self.inner.objects.write().unwrap();
        if let Some(existing) = guard.get(key) {
            return Ok(existing.clone());
        }
        if !create {
            return Err(Error::Io(IoError::new(
                ErrorKind::NotFound,
                format!("path {key} not found"),
            )));
        }
        let tag = self.inner.next_tag();
        let entry = Arc::new(Entry::new(tag));
        guard.insert(key.to_string(), entry.clone());
        Ok(entry)
    }

    fn get_entry(&self, key: &str) -> Option<Arc<Entry>> {
        let guard = self.inner.objects.read().unwrap();
        guard.get(key).cloned()
    }

    fn remove_entry(&self, key: &str) {
        let mut guard = self.inner.objects.write().unwrap();
        guard.remove(key);
    }

    fn list_keys_with_prefix(&self, prefix: &str) -> Vec<(String, Arc<Entry>)> {
        let guard = self.inner.objects.read().unwrap();
        guard
            .iter()
            .filter_map(|(k, v)| {
                if prefix.is_empty() || k.starts_with(prefix) {
                    Some((k.clone(), v.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}

pub struct InMemoryFile {
    fs: InMemoryFs,
    entry: Arc<Entry>,
    allow_read: bool,
    allow_write: bool,
    truncated: bool,
}

impl InMemoryFile {
    fn new(fs: InMemoryFs, entry: Arc<Entry>, opts: &OpenOptions) -> Self {
        if opts.truncate {
            let mut state = entry.state.lock().unwrap();
            state.data.clear();
        }
        Self {
            fs,
            entry,
            allow_read: opts.read,
            allow_write: opts.write,
            truncated: opts.truncate,
        }
    }
}

impl Read for InMemoryFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        if !self.allow_read {
            return (
                Err(Error::Unsupported {
                    message: "file opened without read permissions".into(),
                }),
                buf,
            );
        }
        let state = self.entry.state.lock().unwrap();
        let pos = pos as usize;
        let len = buf.bytes_init();
        if pos + len > state.data.len() {
            return (
                Err(IoError::new(ErrorKind::UnexpectedEof, "read past end").into()),
                buf,
            );
        }
        buf.as_slice_mut()
            .copy_from_slice(&state.data[pos..pos + len]);
        (Ok(()), buf)
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        if !self.allow_read {
            return (
                Err(Error::Unsupported {
                    message: "file opened without read permissions".into(),
                }),
                buf,
            );
        }
        let state = self.entry.state.lock().unwrap();
        let pos = pos as usize;
        if pos > state.data.len() {
            return (
                Err(IoError::new(ErrorKind::UnexpectedEof, "read past end").into()),
                buf,
            );
        }
        buf.extend_from_slice(&state.data[pos..]);
        (Ok(()), buf)
    }

    async fn size(&self) -> Result<u64, Error> {
        let state = self.entry.state.lock().unwrap();
        Ok(state.data.len() as u64)
    }
}

impl Write for InMemoryFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        if !self.allow_write {
            return (
                Err(Error::Unsupported {
                    message: "file opened without write permissions".into(),
                }),
                buf,
            );
        }
        let mut state = self.entry.state.lock().unwrap();
        if self.truncated {
            state.data.clear();
            self.truncated = false;
        }
        state.data.extend_from_slice(buf.as_slice());
        (Ok(()), buf)
    }

    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl FileCommit for InMemoryFile {
    async fn commit(&mut self) -> Result<(), Error> {
        let new_tag = self.fs.inner.next_tag();
        if let Ok(mut state) = self.entry.state.lock() {
            state.etag = new_tag;
        }
        Ok(())
    }
}

impl Fs for InMemoryFs {
    type File = InMemoryFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::Memory
    }

    async fn open_options(
        &self,
        path: &Path,
        mut options: OpenOptions,
    ) -> Result<Self::File, Error> {
        if !options.read && !options.write {
            // default to read if neither set
            options.read = true;
        }
        let key = Self::path_key(path);
        let entry = self.or_create_entry(&key, options.create)?;
        Ok(InMemoryFile::new(self.clone(), entry, &options))
    }

    async fn create_dir_all(_path: &Path) -> Result<(), Error> {
        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>> + Send, Error> {
        let prefix = path.as_ref();
        let entries = self.list_keys_with_prefix(prefix);
        let iter = entries.into_iter().map(|(k, entry)| {
            let size = entry.state.lock().unwrap().data.len() as u64;
            Ok(FileMeta {
                path: Path::from(k),
                size,
            })
        });
        Ok(stream::iter(iter))
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let key = Self::path_key(path);
        self.remove_entry(&key);
        Ok(())
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from_key = Self::path_key(from);
        let to_key = Self::path_key(to);
        let Some(source) = self.get_entry(&from_key) else {
            return Err(Error::Io(IoError::new(
                ErrorKind::NotFound,
                format!("path {from_key} not found"),
            )));
        };
        let target = self.or_create_entry(&to_key, true)?;
        let src_state = source.state.lock().unwrap();
        if let Ok(mut dst_state) = target.state.lock() {
            dst_state.data = src_state.data.clone();
            dst_state.metadata = src_state.metadata.clone();
            dst_state.content_type = src_state.content_type.clone();
            dst_state.etag = self.inner.next_tag();
        }
        Ok(())
    }

    async fn link(&self, _from: &Path, _to: &Path) -> Result<(), Error> {
        Err(Error::Unsupported {
            message: "in-memory fs does not support hard links".into(),
        })
    }
}

impl FsCas for InMemoryFs {
    fn load_with_tag(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(Vec<u8>, String)>, Error>> + '_>> {
        let key = Self::path_key(path);
        Box::pin(async move {
            let Some(entry) = self.get_entry(&key) else {
                return Ok(None);
            };
            let state = entry.state.lock().unwrap();
            Ok(Some((state.data.clone(), state.etag.clone())))
        })
    }

    fn put_conditional(
        &self,
        path: &Path,
        payload: &[u8],
        content_type: Option<&str>,
        metadata: Option<Vec<(String, String)>>,
        condition: CasCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<String, Error>> + '_>> {
        let key = Self::path_key(path);
        let content_type = content_type.map(|s| s.to_string());
        let payload_vec = payload.to_vec();
        Box::pin(async move {
            match condition {
                CasCondition::IfNotExists => {
                    let mut guard = self.inner.objects.write().unwrap();
                    if guard.contains_key(&key) {
                        return Err(Error::PreconditionFailed);
                    }
                    let entry = Arc::new(Entry::new(self.inner.next_tag()));
                    let tag = {
                        let mut state = entry.state.lock().unwrap();
                        state.data = payload_vec.clone();
                        state.content_type = content_type.clone();
                        if let Some(meta) = metadata {
                            state.metadata = meta.into_iter().collect::<HashMap<_, _>>();
                        }
                        state.etag = self.inner.next_tag();
                        state.etag.clone()
                    };
                    guard.insert(key.clone(), entry);
                    Ok(tag)
                }
                CasCondition::IfMatch(expected) => {
                    let Some(entry) = self.get_entry(&key) else {
                        return Err(Error::PreconditionFailed);
                    };
                    let mut state = entry.state.lock().unwrap();
                    if state.etag != expected {
                        return Err(Error::PreconditionFailed);
                    }
                    state.data = payload_vec;
                    state.content_type = content_type.clone();
                    if let Some(meta) = metadata {
                        state.metadata = meta.into_iter().collect::<HashMap<_, _>>();
                    }
                    state.etag = self.inner.next_tag();
                    Ok(state.etag.clone())
                }
            }
        })
    }
}

pub struct HeadObject {
    pub size: u64,
    pub etag: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl InMemoryFs {
    pub async fn head_object(&self, path: &Path) -> Result<Option<HeadObject>, Error> {
        let key = Self::path_key(path);
        let Some(entry) = self.get_entry(&key) else {
            return Ok(None);
        };
        let state = entry.state.lock().unwrap();
        Ok(Some(HeadObject {
            size: state.data.len() as u64,
            etag: Some(state.etag.clone()),
            metadata: state.metadata.clone(),
        }))
    }
}
