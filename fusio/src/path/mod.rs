//! A path abstraction that can be used to represent paths in a cloud-agnostic way.

use std::{fmt::Formatter, path::PathBuf};

use itertools::Itertools;
use percent_encoding::percent_decode;
use thiserror::Error;
use url::Url;

/// The delimiter to separate object namespaces, creating a directory structure.
pub const DELIMITER: &str = "/";

/// The path delimiter as a single byte
pub const DELIMITER_BYTE: u8 = DELIMITER.as_bytes()[0];

mod parts;

pub use parts::{InvalidPart, PathPart};

#[derive(Debug, Error)]
#[error(transparent)]
pub enum Error {
    #[error("Path \"{path}\" contained empty path segment")]
    EmptySegment { path: String },
    #[error("Error parsing Path \"{path}\": {source}")]
    BadSegment { path: String, source: InvalidPart },
    #[error("Failed to canonicalize path \"{path}\": {source}")]
    Canonicalize {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("Unable to convert path \"{path}\" to URL")]
    InvalidPath { path: PathBuf },
    #[error("Unable to convert url \"{url}\" to Path")]
    InvalidUrl { url: Url },
    #[error("Path \"{path}\" contained non-unicode characters: {source}")]
    NonUnicode {
        path: String,
        source: std::str::Utf8Error,
    },
    #[error("Path {path} does not start with prefix {prefix}")]
    PrefixMismatch { path: String, prefix: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Path {
    raw: String,
}

#[cfg(not(target_arch = "wasm32"))]
impl Path {
    pub fn from_filesystem_path(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        Self::from_absolute_path(path)
    }

    pub fn from_absolute_path(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        Self::from_absolute_path_with_base(path, None)
    }

    pub(crate) fn from_absolute_path_with_base(
        path: impl AsRef<std::path::Path>,
        base: Option<&url::Url>,
    ) -> Result<Self, Error> {
        let url = absolute_path_to_url(path)?;
        let path = match base {
            Some(prefix) => {
                url.path()
                    .strip_prefix(prefix.path())
                    .ok_or_else(|| Error::PrefixMismatch {
                        path: url.path().to_string(),
                        prefix: prefix.to_string(),
                    })?
            }
            None => url.path(),
        };

        // Reverse any percent encoding performed by conversion to URL
        Self::from_url_path(path)
    }
}

#[cfg(target_arch = "wasm32")]
impl Path {
    pub fn from_opfs_path(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        Self::parse(path.as_ref().to_str().unwrap())
    }
}

impl Path {
    pub fn new(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        #[cfg(target_arch = "wasm32")]
        {
            Self::from_opfs_path(path)
        }
        #[cfg(not(target_arch = "wasm32"))]
        Self::from_filesystem_path(path)
    }

    pub fn parse(path: impl AsRef<str>) -> Result<Self, Error> {
        let path = path.as_ref();

        let stripped = path.strip_prefix(DELIMITER).unwrap_or(path);
        if stripped.is_empty() {
            return Ok(Default::default());
        }

        let stripped = stripped.strip_suffix(DELIMITER).unwrap_or(stripped);

        for segment in stripped.split(DELIMITER) {
            if segment.is_empty() {
                return Err(Error::EmptySegment {
                    path: path.to_string(),
                });
            }
            PathPart::parse(segment).map_err(|err| Error::BadSegment {
                path: path.to_string(),
                source: err,
            })?;
        }

        Ok(Self {
            raw: stripped.to_string(),
        })
    }

    pub fn from_url_path(path: impl AsRef<str>) -> Result<Self, Error> {
        let path = path.as_ref();
        let decoded = percent_decode(path.as_bytes())
            .decode_utf8()
            .map_err(|err| Error::NonUnicode {
                path: path.to_string(),
                source: err,
            })?;

        Self::parse(decoded)
    }

    pub fn parts(&self) -> impl Iterator<Item = PathPart<'_>> {
        self.raw
            .split_terminator(DELIMITER)
            .map(|s| PathPart { raw: s.into() })
    }

    pub fn filename(&self) -> Option<&str> {
        match self.raw.is_empty() {
            true => None,
            false => self.raw.rsplit(DELIMITER).next(),
        }
    }

    pub fn extension(&self) -> Option<&str> {
        self.filename()
            .and_then(|f| f.rsplit_once('.'))
            .and_then(|(_, extension)| {
                if extension.is_empty() {
                    None
                } else {
                    Some(extension)
                }
            })
    }

    pub fn prefix_match(&self, prefix: &Self) -> Option<impl Iterator<Item = PathPart<'_>> + '_> {
        let mut stripped = self.raw.strip_prefix(&prefix.raw)?;
        if !stripped.is_empty() && !prefix.raw.is_empty() {
            stripped = stripped.strip_prefix(DELIMITER)?;
        }
        let iter = stripped
            .split_terminator(DELIMITER)
            .map(|x| PathPart { raw: x.into() });
        Some(iter)
    }

    pub fn prefix_matches(&self, prefix: &Self) -> bool {
        self.prefix_match(prefix).is_some()
    }

    pub fn child<'a>(&self, child: impl Into<PathPart<'a>>) -> Self {
        let raw = match self.raw.is_empty() {
            true => format!("{}", child.into().raw),
            false => format!("{}{}{}", self.raw, DELIMITER, child.into().raw),
        };

        Self { raw }
    }
}

#[cfg(feature = "object_store")]
impl From<Path> for object_store::path::Path {
    fn from(value: Path) -> Self {
        object_store::path::Path::from(value.as_ref())
    }
}

#[cfg(feature = "object_store")]
impl From<object_store::path::Path> for Path {
    fn from(value: object_store::path::Path) -> Self {
        Self::from(value.as_ref())
    }
}

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str {
        &self.raw
    }
}

impl From<&str> for Path {
    fn from(path: &str) -> Self {
        Self::from_iter(path.split(DELIMITER))
    }
}

impl From<String> for Path {
    fn from(path: String) -> Self {
        Self::from_iter(path.split(DELIMITER))
    }
}

impl From<Path> for String {
    fn from(path: Path) -> Self {
        path.raw
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.raw.fmt(f)
    }
}

impl<'a, I> FromIterator<I> for Path
where
    I: Into<PathPart<'a>>,
{
    fn from_iter<T: IntoIterator<Item = I>>(iter: T) -> Self {
        let raw = T::into_iter(iter)
            .map(|s| s.into())
            .filter(|s| !s.raw.is_empty())
            .map(|s| s.raw)
            .join(DELIMITER);

        Self { raw }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn absolute_path_to_url(path: impl AsRef<std::path::Path>) -> Result<Url, Error> {
    Url::from_file_path(&path).map_err(|_| Error::InvalidPath {
        path: path.as_ref().into(),
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub fn path_to_local(location: &Path) -> Result<PathBuf, Error> {
    let mut url = Url::parse("file:///").unwrap();
    url.path_segments_mut()
        .expect("url path")
        // technically not necessary as Path ignores empty segments
        // but avoids creating paths with "//" which look odd in error messages.
        .pop_if_empty()
        .extend(location.parts());

    let path = url.to_file_path().map_err(|_| Error::InvalidUrl { url })?;

    #[cfg(target_os = "windows")]
    let path = {
        let path = path.to_string_lossy();

        // Assume the first char is the drive letter and the next is a colon.
        let mut out = String::new();
        let drive = &path[..2]; // The drive letter and colon (e.g., "C:")
        let filepath = &path[2..].replace(':', "%3A"); // Replace subsequent colons
        out.push_str(drive);
        out.push_str(filepath);
        PathBuf::from(out)
    };

    Ok(path)
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use std::fs::canonicalize;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn cloud_prefix_with_trailing_delimiter() {
        let prefix = Path::from_iter(["test"]);
        assert_eq!(prefix.as_ref(), "test");
    }

    #[test]
    fn push_encodes() {
        let location = Path::from_iter(["foo/bar", "baz%2Ftest"]);
        assert_eq!(location.as_ref(), "foo%2Fbar/baz%252Ftest");
    }

    #[test]
    fn test_parse() {
        assert_eq!(Path::parse("/").unwrap().as_ref(), "");
        assert_eq!(Path::parse("").unwrap().as_ref(), "");

        let err = Path::parse("//").unwrap_err();
        assert!(matches!(err, Error::EmptySegment { .. }));

        assert_eq!(Path::parse("/foo/bar/").unwrap().as_ref(), "foo/bar");
        assert_eq!(Path::parse("foo/bar/").unwrap().as_ref(), "foo/bar");
        assert_eq!(Path::parse("foo/bar").unwrap().as_ref(), "foo/bar");

        let err = Path::parse("foo///bar").unwrap_err();
        assert!(matches!(err, Error::EmptySegment { .. }));
    }

    #[test]
    fn convert_raw_before_partial_eq() {
        // dir and file_name
        let cloud = Path::from("test_dir/test_file.json");
        let built = Path::from_iter(["test_dir", "test_file.json"]);

        assert_eq!(built, cloud);

        // dir and file_name w/o dot
        let cloud = Path::from("test_dir/test_file");
        let built = Path::from_iter(["test_dir", "test_file"]);

        assert_eq!(built, cloud);

        // dir, no file
        let cloud = Path::from("test_dir/");
        let built = Path::from_iter(["test_dir"]);
        assert_eq!(built, cloud);

        // file_name, no dir
        let cloud = Path::from("test_file.json");
        let built = Path::from_iter(["test_file.json"]);
        assert_eq!(built, cloud);

        // empty
        let cloud = Path::from("");
        let built = Path::from_iter(["", ""]);

        assert_eq!(built, cloud);
    }

    #[test]
    fn parts_after_prefix_behavior() {
        let existing_path = Path::from("apple/bear/cow/dog/egg.json");

        // Prefix with one directory
        let prefix = Path::from("apple");
        let expected_parts: Vec<PathPart<'_>> = vec!["bear", "cow", "dog", "egg.json"]
            .into_iter()
            .map(Into::into)
            .collect();
        let parts: Vec<_> = existing_path.prefix_match(&prefix).unwrap().collect();
        assert_eq!(parts, expected_parts);

        // Prefix with two directories
        let prefix = Path::from("apple/bear");
        let expected_parts: Vec<PathPart<'_>> = vec!["cow", "dog", "egg.json"]
            .into_iter()
            .map(Into::into)
            .collect();
        let parts: Vec<_> = existing_path.prefix_match(&prefix).unwrap().collect();
        assert_eq!(parts, expected_parts);

        // Not a prefix
        let prefix = Path::from("cow");
        assert!(existing_path.prefix_match(&prefix).is_none());

        // Prefix with a partial directory
        let prefix = Path::from("ap");
        assert!(existing_path.prefix_match(&prefix).is_none());

        // Prefix matches but there aren't any parts after it
        let existing = Path::from("apple/bear/cow/dog");

        assert_eq!(existing.prefix_match(&existing).unwrap().count(), 0);
        assert_eq!(Path::default().parts().count(), 0);
    }

    #[test]
    fn prefix_matches() {
        let haystack = Path::from_iter(["foo/bar", "baz%2Ftest", "something"]);
        // self starts with self
        assert!(
            haystack.prefix_matches(&haystack),
            "{haystack:?} should have started with {haystack:?}"
        );

        // a longer prefix doesn't match
        let needle = haystack.child("longer now");
        assert!(
            !haystack.prefix_matches(&needle),
            "{haystack:?} shouldn't have started with {needle:?}"
        );

        // one dir prefix matches
        let needle = Path::from_iter(["foo/bar"]);
        assert!(
            haystack.prefix_matches(&needle),
            "{haystack:?} should have started with {needle:?}"
        );

        // two dir prefix matches
        let needle = needle.child("baz%2Ftest");
        assert!(
            haystack.prefix_matches(&needle),
            "{haystack:?} should have started with {needle:?}"
        );

        // partial dir prefix doesn't match
        let needle = Path::from_iter(["f"]);
        assert!(
            !haystack.prefix_matches(&needle),
            "{haystack:?} should not have started with {needle:?}"
        );

        // one dir and one partial dir doesn't match
        let needle = Path::from_iter(["foo/bar", "baz"]);
        assert!(
            !haystack.prefix_matches(&needle),
            "{haystack:?} should not have started with {needle:?}"
        );

        // empty prefix matches
        let needle = Path::from("");
        assert!(
            haystack.prefix_matches(&needle),
            "{haystack:?} should have started with {needle:?}"
        );
    }

    #[test]
    fn prefix_matches_with_file_name() {
        let haystack = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "foo.segment"]);

        // All directories match and file name is a prefix
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "foo"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{haystack:?} should not have started with {needle:?}"
        );

        // All directories match but file name is not a prefix
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "e"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{haystack:?} should not have started with {needle:?}"
        );

        // Not all directories match; file name is a prefix of the next directory; this
        // does not match
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "s"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{haystack:?} should not have started with {needle:?}"
        );

        // Not all directories match; file name is NOT a prefix of the next directory;
        // no match
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "p"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{haystack:?} should not have started with {needle:?}"
        );
    }

    #[test]
    fn path_containing_spaces() {
        let a = Path::from_iter(["foo bar", "baz"]);
        let b = Path::from("foo bar/baz");
        let c = Path::parse("foo bar/baz").unwrap();

        assert_eq!(a.raw, "foo bar/baz");
        assert_eq!(a.raw, b.raw);
        assert_eq!(b.raw, c.raw);
    }

    #[test]
    fn from_url_path() {
        let a = Path::from_url_path("foo%20bar").unwrap();
        let b = Path::from_url_path("foo/%2E%2E/bar").unwrap_err();
        let c = Path::from_url_path("foo%2F%252E%252E%2Fbar").unwrap();
        let d = Path::from_url_path("foo/%252E%252E/bar").unwrap();
        let e = Path::from_url_path("%48%45%4C%4C%4F").unwrap();
        let f = Path::from_url_path("foo/%FF/as").unwrap_err();

        assert_eq!(a.raw, "foo bar");
        assert!(matches!(b, Error::BadSegment { .. }));
        assert_eq!(c.raw, "foo/%2E%2E/bar");
        assert_eq!(d.raw, "foo/%2E%2E/bar");
        assert_eq!(e.raw, "HELLO");
        assert!(matches!(f, Error::NonUnicode { .. }));
    }

    #[test]
    fn filename_from_path() {
        let a = Path::from("foo/bar");
        let b = Path::from("foo/bar.baz");
        let c = Path::from("foo.bar/baz");

        assert_eq!(a.filename(), Some("bar"));
        assert_eq!(b.filename(), Some("bar.baz"));
        assert_eq!(c.filename(), Some("baz"));
    }

    #[test]
    fn file_extension() {
        let a = Path::from("foo/bar");
        let b = Path::from("foo/bar.baz");
        let c = Path::from("foo.bar/baz");
        let d = Path::from("foo.bar/baz.qux");

        assert_eq!(a.extension(), None);
        assert_eq!(b.extension(), Some("baz"));
        assert_eq!(c.extension(), None);
        assert_eq!(d.extension(), Some("qux"));
    }
}
