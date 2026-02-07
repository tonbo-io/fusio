use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    manifest::{Op, Record},
    types::{Error, Result},
};

pub(crate) const RUN_DATA_CONTENT_TYPE: &str = "application/vnd.fusio-manifest.run-v3+bin";
pub(crate) const RUN_INDEX_CONTENT_TYPE: &str = "application/vnd.fusio-manifest.run-index-v3+bin";

const RUN_DATA_MAGIC: &[u8; 4] = b"FRD3";
const RUN_INDEX_MAGIC: &[u8; 4] = b"FRI3";
const RUN_BLOCK_MAGIC: &[u8; 4] = b"FRB3";
const RUN_FORMAT_VERSION: u8 = 1;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_MIN_BITS: usize = 64;

const OP_PUT_TAG: u8 = 1;
const OP_DEL_TAG: u8 = 2;

const RUN_BLOCK_HEADER_LEN: usize = 4 + 1 + 4 + 4 + 4;
const RUN_BLOCK_ENTRY_META_LEN: usize = 4 + 4 + 4 + 4 + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RunBuildOptions {
    pub target_bytes: usize,
    pub max_records: usize,
}

impl RunBuildOptions {
    pub fn new(target_bytes: usize, max_records: usize) -> Self {
        Self {
            target_bytes: target_bytes.max(1),
            max_records: max_records.max(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunBlockMeta {
    pub first_key_json: Vec<u8>,
    pub last_key_json: Vec<u8>,
    pub offset: u64,
    pub len: u32,
    pub record_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunIndexMeta {
    pub key_min_json: Vec<u8>,
    pub key_max_json: Vec<u8>,
    pub record_count: u64,
    pub payload_byte_size: u64,
    pub block_target_bytes: u32,
    pub block_max_records: u32,
    pub bloom_k: u8,
    pub bloom_bits: Vec<u8>,
    pub blocks: Vec<RunBlockMeta>,
}

pub(crate) struct EncodedRun {
    pub payload: Vec<u8>,
    pub index: RunIndexMeta,
}

struct EncodedEntry {
    key_json: Vec<u8>,
    op_tag: u8,
    value_json: Option<Vec<u8>>,
}

struct RunBlockLayout {
    entry_count: usize,
    entries_start: usize,
    key_start: usize,
    key_len: usize,
    value_start: usize,
    value_len: usize,
}

#[derive(Clone, Copy)]
struct RunBlockEntryMeta {
    key_offset: u32,
    key_len: u32,
    value_offset: u32,
    value_len: u32,
    op_tag: u8,
}

pub(crate) fn encode_run<K, V>(
    records: &[Record<K, V>],
    options: RunBuildOptions,
) -> Result<EncodedRun>
where
    K: Serialize,
    V: Serialize,
{
    let normalized = RunBuildOptions::new(options.target_bytes, options.max_records);

    let mut payload = Vec::new();
    payload.extend_from_slice(RUN_DATA_MAGIC);
    payload.push(RUN_FORMAT_VERSION);

    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        let key_json = serde_json::to_vec(&record.key)
            .map_err(|e| Error::Corrupt(format!("run key encode: {e}")))?;
        let (op_tag, value_json) = match record.op {
            Op::Put => {
                let value = record
                    .value
                    .as_ref()
                    .ok_or_else(|| Error::Corrupt("put record missing value".into()))?;
                let value_json = serde_json::to_vec(value)
                    .map_err(|e| Error::Corrupt(format!("run value encode: {e}")))?;
                (OP_PUT_TAG, Some(value_json))
            }
            Op::Del => (OP_DEL_TAG, None),
        };
        entries.push(EncodedEntry {
            key_json,
            op_tag,
            value_json,
        });
    }

    let mut blocks = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = start;
        let mut key_bytes = 0usize;
        let mut value_bytes = 0usize;

        while end < entries.len() {
            let entry = &entries[end];
            let entry_value_len = entry.value_json.as_ref().map_or(0, Vec::len);
            let next_count = end + 1 - start;
            let next_key_bytes = key_bytes.saturating_add(entry.key_json.len());
            let next_value_bytes = value_bytes.saturating_add(entry_value_len);
            let next_size = estimate_run_block_size(next_count, next_key_bytes, next_value_bytes)?;
            if end > start
                && (next_count > normalized.max_records || next_size > normalized.target_bytes)
            {
                break;
            }

            key_bytes = next_key_bytes;
            value_bytes = next_value_bytes;
            end += 1;

            if end - start >= normalized.max_records {
                break;
            }
        }

        let block = encode_run_block(&entries[start..end])?;
        let offset = u64::try_from(payload.len())
            .map_err(|_| Error::Corrupt("run payload too large".into()))?;
        let len =
            u32::try_from(block.len()).map_err(|_| Error::Corrupt("run block too large".into()))?;
        let record_count =
            u32::try_from(end - start).map_err(|_| Error::Corrupt("run block too large".into()))?;

        blocks.push(RunBlockMeta {
            first_key_json: entries[start].key_json.clone(),
            last_key_json: entries[end - 1].key_json.clone(),
            offset,
            len,
            record_count,
        });
        payload.extend_from_slice(&block);
        start = end;
    }

    let (key_min_json, key_max_json) =
        if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
            (first.key_json.clone(), last.key_json.clone())
        } else {
            (Vec::new(), Vec::new())
        };

    let (bloom_k, bloom_bits) = build_bloom(&entries);
    let record_count = u64::try_from(entries.len())
        .map_err(|_| Error::Corrupt("run has too many records".into()))?;
    let payload_byte_size =
        u64::try_from(payload.len()).map_err(|_| Error::Corrupt("run payload too large".into()))?;

    Ok(EncodedRun {
        payload,
        index: RunIndexMeta {
            key_min_json,
            key_max_json,
            record_count,
            payload_byte_size,
            block_target_bytes: u32::try_from(normalized.target_bytes)
                .map_err(|_| Error::Corrupt("run block target_bytes too large".into()))?,
            block_max_records: u32::try_from(normalized.max_records)
                .map_err(|_| Error::Corrupt("run block max_records too large".into()))?,
            bloom_k,
            bloom_bits,
            blocks,
        },
    })
}

pub(crate) fn encode_run_index(index: &RunIndexMeta) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(RUN_INDEX_MAGIC);
    out.push(RUN_FORMAT_VERSION);

    write_bytes(&mut out, &index.key_min_json)?;
    write_bytes(&mut out, &index.key_max_json)?;
    out.extend_from_slice(&index.record_count.to_le_bytes());
    out.extend_from_slice(&index.payload_byte_size.to_le_bytes());
    out.extend_from_slice(&index.block_target_bytes.to_le_bytes());
    out.extend_from_slice(&index.block_max_records.to_le_bytes());
    out.push(index.bloom_k);
    write_bytes(&mut out, &index.bloom_bits)?;

    let block_count = u32::try_from(index.blocks.len())
        .map_err(|_| Error::Corrupt("run index has too many blocks".into()))?;
    out.extend_from_slice(&block_count.to_le_bytes());
    for block in &index.blocks {
        write_bytes(&mut out, &block.first_key_json)?;
        write_bytes(&mut out, &block.last_key_json)?;
        out.extend_from_slice(&block.offset.to_le_bytes());
        out.extend_from_slice(&block.len.to_le_bytes());
        out.extend_from_slice(&block.record_count.to_le_bytes());
    }

    Ok(out)
}

pub(crate) fn decode_run_index(bytes: &[u8]) -> Result<RunIndexMeta> {
    let mut cursor = 0;
    ensure_magic(bytes, &mut cursor, RUN_INDEX_MAGIC, "run index")?;
    let version = read_u8(bytes, &mut cursor, "run index version")?;
    if version != RUN_FORMAT_VERSION {
        return Err(Error::Corrupt(format!(
            "unsupported run index version: {version}"
        )));
    }

    let key_min_json = read_bytes(bytes, &mut cursor, "run index key_min")?;
    let key_max_json = read_bytes(bytes, &mut cursor, "run index key_max")?;
    let record_count = read_u64(bytes, &mut cursor, "run index record_count")?;
    let payload_byte_size = read_u64(bytes, &mut cursor, "run index payload_byte_size")?;
    let block_target_bytes = read_u32(bytes, &mut cursor, "run index block_target_bytes")?;
    let block_max_records = read_u32(bytes, &mut cursor, "run index block_max_records")?;
    let bloom_k = read_u8(bytes, &mut cursor, "run index bloom_k")?;
    let bloom_bits = read_bytes(bytes, &mut cursor, "run index bloom_bits")?;
    let block_count = read_u32(bytes, &mut cursor, "run index block_count")?;

    let mut blocks = Vec::with_capacity(block_count as usize);
    let mut prev_end = 0_u64;
    for _ in 0..block_count {
        let first_key_json = read_bytes(bytes, &mut cursor, "run index block first_key")?;
        let last_key_json = read_bytes(bytes, &mut cursor, "run index block last_key")?;
        let offset = read_u64(bytes, &mut cursor, "run index block offset")?;
        let len = read_u32(bytes, &mut cursor, "run index block len")?;
        let record_count = read_u32(bytes, &mut cursor, "run index block record_count")?;
        if offset < prev_end {
            return Err(Error::Corrupt(
                "run index block offsets are not monotonic".into(),
            ));
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or_else(|| Error::Corrupt("run index block range overflow".into()))?;
        if end > payload_byte_size {
            return Err(Error::Corrupt(
                "run index block range exceeds payload size".into(),
            ));
        }
        prev_end = end;
        blocks.push(RunBlockMeta {
            first_key_json,
            last_key_json,
            offset,
            len,
            record_count,
        });
    }

    if cursor != bytes.len() {
        return Err(Error::Corrupt("run index has trailing bytes".into()));
    }
    if block_target_bytes == 0 {
        return Err(Error::Corrupt(
            "run index block_target_bytes must be > 0".into(),
        ));
    }
    if block_max_records == 0 {
        return Err(Error::Corrupt(
            "run index block_max_records must be > 0".into(),
        ));
    }
    if (record_count == 0) != blocks.is_empty() {
        return Err(Error::Corrupt(
            "run index record_count does not match block list".into(),
        ));
    }
    if record_count > 0 && (key_min_json.is_empty() || key_max_json.is_empty()) {
        return Err(Error::Corrupt(
            "run index key range missing for non-empty run".into(),
        ));
    }

    Ok(RunIndexMeta {
        key_min_json,
        key_max_json,
        record_count,
        payload_byte_size,
        block_target_bytes,
        block_max_records,
        bloom_k,
        bloom_bits,
        blocks,
    })
}

pub(crate) fn decode_run_block<K, V>(bytes: &[u8]) -> Result<Vec<Record<K, V>>>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    let layout = parse_run_block_layout(bytes)?;
    let mut out = Vec::with_capacity(layout.entry_count);

    for idx in 0..layout.entry_count {
        let meta = read_block_entry_meta(bytes, &layout, idx)?;
        let key_json = block_key_slice(bytes, &layout, &meta)?;
        let key = serde_json::from_slice(key_json)
            .map_err(|e| Error::Corrupt(format!("run key decode: {e}")))?;

        match meta.op_tag {
            OP_PUT_TAG => {
                let value_json = block_value_slice(bytes, &layout, &meta)?;
                let value = serde_json::from_slice(value_json)
                    .map_err(|e| Error::Corrupt(format!("run value decode: {e}")))?;
                out.push(Record {
                    key,
                    op: Op::Put,
                    value: Some(value),
                });
            }
            OP_DEL_TAG => out.push(Record {
                key,
                op: Op::Del,
                value: None,
            }),
            _ => {
                return Err(Error::Corrupt(format!(
                    "run block has unknown op tag: {}",
                    meta.op_tag
                )));
            }
        }
    }

    Ok(out)
}

pub(crate) fn lookup_key_in_run_block<K, V>(bytes: &[u8], key: &K) -> Result<Option<Option<V>>>
where
    K: Ord + DeserializeOwned,
    V: DeserializeOwned,
{
    let layout = parse_run_block_layout(bytes)?;
    if layout.entry_count == 0 {
        return Ok(None);
    }

    let mut lo = 0usize;
    let mut hi = layout.entry_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let meta = read_block_entry_meta(bytes, &layout, mid)?;
        let mid_key_json = block_key_slice(bytes, &layout, &meta)?;
        let mid_key: K = serde_json::from_slice(mid_key_json)
            .map_err(|e| Error::Corrupt(format!("run key decode: {e}")))?;
        if &mid_key < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if lo >= layout.entry_count {
        return Ok(None);
    }

    let meta = read_block_entry_meta(bytes, &layout, lo)?;
    let candidate_key_json = block_key_slice(bytes, &layout, &meta)?;
    let candidate_key: K = serde_json::from_slice(candidate_key_json)
        .map_err(|e| Error::Corrupt(format!("run key decode: {e}")))?;
    if &candidate_key != key {
        return Ok(None);
    }

    match meta.op_tag {
        OP_DEL_TAG => Ok(Some(None)),
        OP_PUT_TAG => {
            let value_json = block_value_slice(bytes, &layout, &meta)?;
            let value = serde_json::from_slice(value_json)
                .map_err(|e| Error::Corrupt(format!("run value decode: {e}")))?;
            Ok(Some(Some(value)))
        }
        _ => Err(Error::Corrupt(format!(
            "run block has unknown op tag: {}",
            meta.op_tag
        ))),
    }
}

pub(crate) fn run_block_window_for_key_with_bloom<K>(
    index: &RunIndexMeta,
    key: &K,
    bloom_enabled: bool,
) -> Result<Option<(u64, usize)>>
where
    K: Ord + Serialize + DeserializeOwned,
{
    let might_contain = if bloom_enabled {
        run_might_contain_key(index, key)?
    } else {
        run_might_contain_key_range(index, key)?
    };
    if !might_contain || index.blocks.is_empty() {
        return Ok(None);
    }

    let mut lo = 0usize;
    let mut hi = index.blocks.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_last: K = serde_json::from_slice(&index.blocks[mid].last_key_json)
            .map_err(|e| Error::Corrupt(format!("run block last_key decode: {e}")))?;
        if key <= &mid_last {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }

    if lo >= index.blocks.len() {
        return Ok(None);
    }

    let candidate = &index.blocks[lo];
    let first_key: K = serde_json::from_slice(&candidate.first_key_json)
        .map_err(|e| Error::Corrupt(format!("run block first_key decode: {e}")))?;
    if key < &first_key {
        return Ok(None);
    }

    if candidate.len == 0 {
        return Ok(None);
    }

    Ok(Some((candidate.offset, candidate.len as usize)))
}

pub(crate) fn run_might_contain_key<K>(index: &RunIndexMeta, key: &K) -> Result<bool>
where
    K: Ord + Serialize + DeserializeOwned,
{
    if !run_might_contain_key_range(index, key)? {
        return Ok(false);
    }
    if index.bloom_k == 0 || index.bloom_bits.is_empty() {
        return Ok(true);
    }
    let key_json =
        serde_json::to_vec(key).map_err(|e| Error::Corrupt(format!("run key encode: {e}")))?;
    Ok(bloom_might_contain(
        &index.bloom_bits,
        index.bloom_k,
        &key_json,
    ))
}

pub(crate) fn run_might_contain_key_range<K>(index: &RunIndexMeta, key: &K) -> Result<bool>
where
    K: Ord + DeserializeOwned,
{
    if index.record_count == 0 {
        return Ok(false);
    }
    let key_min: K = serde_json::from_slice(&index.key_min_json)
        .map_err(|e| Error::Corrupt(format!("run key_min decode: {e}")))?;
    if key < &key_min {
        return Ok(false);
    }
    let key_max: K = serde_json::from_slice(&index.key_max_json)
        .map_err(|e| Error::Corrupt(format!("run key_max decode: {e}")))?;
    if key > &key_max {
        return Ok(false);
    }
    Ok(true)
}

fn estimate_run_block_size(
    entry_count: usize,
    key_bytes: usize,
    value_bytes: usize,
) -> Result<usize> {
    let metas = entry_count
        .checked_mul(RUN_BLOCK_ENTRY_META_LEN)
        .ok_or_else(|| Error::Corrupt("run block size overflow".into()))?;
    RUN_BLOCK_HEADER_LEN
        .checked_add(metas)
        .and_then(|v| v.checked_add(key_bytes))
        .and_then(|v| v.checked_add(value_bytes))
        .ok_or_else(|| Error::Corrupt("run block size overflow".into()))
}

fn encode_run_block(entries: &[EncodedEntry]) -> Result<Vec<u8>> {
    let entry_count = entries.len();
    let mut key_bytes = Vec::new();
    let mut value_bytes = Vec::new();
    let mut metas = Vec::with_capacity(entry_count);

    for entry in entries {
        let key_offset = u32::try_from(key_bytes.len())
            .map_err(|_| Error::Corrupt("run block key section too large".into()))?;
        let key_len = u32::try_from(entry.key_json.len())
            .map_err(|_| Error::Corrupt("run block key too large".into()))?;
        key_bytes.extend_from_slice(&entry.key_json);

        let (value_offset, value_len) = if entry.op_tag == OP_PUT_TAG {
            let value_json = entry
                .value_json
                .as_ref()
                .ok_or_else(|| Error::Corrupt("put record missing value".into()))?;
            let value_offset = u32::try_from(value_bytes.len())
                .map_err(|_| Error::Corrupt("run block value section too large".into()))?;
            let value_len = u32::try_from(value_json.len())
                .map_err(|_| Error::Corrupt("run block value too large".into()))?;
            value_bytes.extend_from_slice(value_json);
            (value_offset, value_len)
        } else {
            (0, 0)
        };

        metas.push(RunBlockEntryMeta {
            key_offset,
            key_len,
            value_offset,
            value_len,
            op_tag: entry.op_tag,
        });
    }

    let mut out = Vec::with_capacity(estimate_run_block_size(
        entry_count,
        key_bytes.len(),
        value_bytes.len(),
    )?);
    out.extend_from_slice(RUN_BLOCK_MAGIC);
    out.push(RUN_FORMAT_VERSION);
    out.extend_from_slice(
        &u32::try_from(entry_count)
            .map_err(|_| Error::Corrupt("run block has too many records".into()))?
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &u32::try_from(key_bytes.len())
            .map_err(|_| Error::Corrupt("run block key section too large".into()))?
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &u32::try_from(value_bytes.len())
            .map_err(|_| Error::Corrupt("run block value section too large".into()))?
            .to_le_bytes(),
    );

    for meta in metas {
        out.extend_from_slice(&meta.key_offset.to_le_bytes());
        out.extend_from_slice(&meta.key_len.to_le_bytes());
        out.extend_from_slice(&meta.value_offset.to_le_bytes());
        out.extend_from_slice(&meta.value_len.to_le_bytes());
        out.push(meta.op_tag);
    }
    out.extend_from_slice(&key_bytes);
    out.extend_from_slice(&value_bytes);
    Ok(out)
}

fn parse_run_block_layout(bytes: &[u8]) -> Result<RunBlockLayout> {
    let mut cursor = 0;
    ensure_magic(bytes, &mut cursor, RUN_BLOCK_MAGIC, "run block")?;
    let version = read_u8(bytes, &mut cursor, "run block version")?;
    if version != RUN_FORMAT_VERSION {
        return Err(Error::Corrupt(format!(
            "unsupported run block version: {version}"
        )));
    }

    let entry_count = read_u32(bytes, &mut cursor, "run block record_count")? as usize;
    let key_len = read_u32(bytes, &mut cursor, "run block key_section_len")? as usize;
    let value_len = read_u32(bytes, &mut cursor, "run block value_section_len")? as usize;

    let entries_start = cursor;
    let entries_len = entry_count
        .checked_mul(RUN_BLOCK_ENTRY_META_LEN)
        .ok_or_else(|| Error::Corrupt("run block entry meta overflow".into()))?;
    let key_start = entries_start
        .checked_add(entries_len)
        .ok_or_else(|| Error::Corrupt("run block section overflow".into()))?;
    let value_start = key_start
        .checked_add(key_len)
        .ok_or_else(|| Error::Corrupt("run block section overflow".into()))?;
    let end = value_start
        .checked_add(value_len)
        .ok_or_else(|| Error::Corrupt("run block section overflow".into()))?;

    if end != bytes.len() {
        return Err(Error::Corrupt("run block has trailing bytes".into()));
    }

    Ok(RunBlockLayout {
        entry_count,
        entries_start,
        key_start,
        key_len,
        value_start,
        value_len,
    })
}

fn read_block_entry_meta(
    bytes: &[u8],
    layout: &RunBlockLayout,
    idx: usize,
) -> Result<RunBlockEntryMeta> {
    if idx >= layout.entry_count {
        return Err(Error::Corrupt("run block entry index out of bounds".into()));
    }

    let start = layout
        .entries_start
        .checked_add(
            idx.checked_mul(RUN_BLOCK_ENTRY_META_LEN)
                .ok_or_else(|| Error::Corrupt("run block entry meta overflow".into()))?,
        )
        .ok_or_else(|| Error::Corrupt("run block entry meta overflow".into()))?;

    let key_offset = read_u32_at(bytes, start, "run block key_offset")?;
    let key_len = read_u32_at(bytes, start + 4, "run block key_len")?;
    let value_offset = read_u32_at(bytes, start + 8, "run block value_offset")?;
    let value_len = read_u32_at(bytes, start + 12, "run block value_len")?;
    let op_tag = *bytes
        .get(start + 16)
        .ok_or_else(|| Error::Corrupt("unexpected EOF while decoding run block op".into()))?;

    let key_end = (key_offset as usize)
        .checked_add(key_len as usize)
        .ok_or_else(|| Error::Corrupt("run block key range overflow".into()))?;
    if key_end > layout.key_len {
        return Err(Error::Corrupt("run block key range out of bounds".into()));
    }

    match op_tag {
        OP_PUT_TAG => {
            let value_end = (value_offset as usize)
                .checked_add(value_len as usize)
                .ok_or_else(|| Error::Corrupt("run block value range overflow".into()))?;
            if value_end > layout.value_len {
                return Err(Error::Corrupt("run block value range out of bounds".into()));
            }
        }
        OP_DEL_TAG => {
            if value_len != 0 {
                return Err(Error::Corrupt(
                    "run block tombstone has non-empty value".into(),
                ));
            }
        }
        _ => {
            return Err(Error::Corrupt(format!(
                "run block has unknown op tag: {op_tag}"
            )));
        }
    }

    Ok(RunBlockEntryMeta {
        key_offset,
        key_len,
        value_offset,
        value_len,
        op_tag,
    })
}

fn block_key_slice<'a>(
    bytes: &'a [u8],
    layout: &RunBlockLayout,
    meta: &RunBlockEntryMeta,
) -> Result<&'a [u8]> {
    let start = layout
        .key_start
        .checked_add(meta.key_offset as usize)
        .ok_or_else(|| Error::Corrupt("run block key offset overflow".into()))?;
    let end = start
        .checked_add(meta.key_len as usize)
        .ok_or_else(|| Error::Corrupt("run block key offset overflow".into()))?;
    bytes
        .get(start..end)
        .ok_or_else(|| Error::Corrupt("run block key range out of bounds".into()))
}

fn block_value_slice<'a>(
    bytes: &'a [u8],
    layout: &RunBlockLayout,
    meta: &RunBlockEntryMeta,
) -> Result<&'a [u8]> {
    let start = layout
        .value_start
        .checked_add(meta.value_offset as usize)
        .ok_or_else(|| Error::Corrupt("run block value offset overflow".into()))?;
    let end = start
        .checked_add(meta.value_len as usize)
        .ok_or_else(|| Error::Corrupt("run block value offset overflow".into()))?;
    bytes
        .get(start..end)
        .ok_or_else(|| Error::Corrupt("run block value range out of bounds".into()))
}

fn build_bloom(entries: &[EncodedEntry]) -> (u8, Vec<u8>) {
    if entries.is_empty() {
        return (0, Vec::new());
    }

    let target_bits = entries
        .len()
        .saturating_mul(BLOOM_BITS_PER_KEY)
        .max(BLOOM_MIN_BITS);
    let bytes_len = target_bits.div_ceil(8);
    let mut bits = vec![0_u8; bytes_len];
    let k = ((BLOOM_BITS_PER_KEY as f64 * std::f64::consts::LN_2).round() as u8).clamp(1, 15);

    for entry in entries {
        bloom_insert(&mut bits, k, &entry.key_json);
    }
    (k, bits)
}

fn bloom_insert(bits: &mut [u8], k: u8, key_json: &[u8]) {
    let (h1, h2) = bloom_hashes(key_json);
    let m = (bits.len() * 8) as u64;
    if m == 0 {
        return;
    }
    for i in 0..k {
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        let bit = (hash % m) as usize;
        bits[bit / 8] |= 1 << (bit % 8);
    }
}

fn bloom_might_contain(bits: &[u8], k: u8, key_json: &[u8]) -> bool {
    let m = (bits.len() * 8) as u64;
    if m == 0 {
        return false;
    }
    let (h1, h2) = bloom_hashes(key_json);
    for i in 0..k {
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        let bit = (hash % m) as usize;
        if (bits[bit / 8] & (1 << (bit % 8))) == 0 {
            return false;
        }
    }
    true
}

fn bloom_hashes(key_json: &[u8]) -> (u64, u64) {
    let digest = Sha256::digest(key_json);
    let mut h1_bytes = [0_u8; 8];
    let mut h2_bytes = [0_u8; 8];
    h1_bytes.copy_from_slice(&digest[0..8]);
    h2_bytes.copy_from_slice(&digest[8..16]);
    let h1 = u64::from_le_bytes(h1_bytes);
    let mut h2 = u64::from_le_bytes(h2_bytes) | 1;
    if h2 == 0 {
        h2 = 0x9E37_79B9_7F4A_7C15;
    }
    (h1, h2)
}

fn ensure_magic(bytes: &[u8], cursor: &mut usize, magic: &[u8; 4], what: &str) -> Result<()> {
    let got = take(bytes, cursor, magic.len(), "magic")?;
    if got != magic {
        return Err(Error::Corrupt(format!("invalid {what} magic")));
    }
    Ok(())
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| Error::Corrupt("value too large".into()))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u8> {
    Ok(*take(bytes, cursor, 1, field)?
        .first()
        .ok_or_else(|| Error::Corrupt(format!("missing {field}")))?)
}

fn read_u32(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
    let raw = take(bytes, cursor, 4, field)?;
    let mut buf = [0_u8; 4];
    buf.copy_from_slice(raw);
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
    let raw = take(bytes, cursor, 8, field)?;
    let mut buf = [0_u8; 8];
    buf.copy_from_slice(raw);
    Ok(u64::from_le_bytes(buf))
}

fn read_u32_at(bytes: &[u8], start: usize, field: &str) -> Result<u32> {
    let end = start
        .checked_add(4)
        .ok_or_else(|| Error::Corrupt(format!("{field} overflow")))?;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| Error::Corrupt(format!("unexpected EOF while decoding {field}")))?;
    let mut buf = [0_u8; 4];
    buf.copy_from_slice(raw);
    Ok(u32::from_le_bytes(buf))
}

fn read_bytes(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<Vec<u8>> {
    let len = read_u32(bytes, cursor, field)? as usize;
    let raw = take(bytes, cursor, len, field)?;
    Ok(raw.to_vec())
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize, field: &str) -> Result<&'a [u8]> {
    let start = *cursor;
    let end = start
        .checked_add(len)
        .ok_or_else(|| Error::Corrupt(format!("{field} overflow")))?;
    if end > bytes.len() {
        return Err(Error::Corrupt(format!(
            "unexpected EOF while decoding {field}"
        )));
    }
    *cursor = end;
    Ok(&bytes[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_codec_roundtrip() {
        let records = vec![
            Record {
                key: "a".to_string(),
                op: Op::Put,
                value: Some("1".to_string()),
            },
            Record {
                key: "b".to_string(),
                op: Op::Del,
                value: None,
            },
            Record {
                key: "c".to_string(),
                op: Op::Put,
                value: Some("3".to_string()),
            },
        ];

        let encoded = encode_run(&records, RunBuildOptions::new(128, 2)).expect("encode run");
        let index_payload = encode_run_index(&encoded.index).expect("encode run index");
        let decoded_index = decode_run_index(&index_payload).expect("decode run index");

        assert_eq!(decoded_index.blocks.len(), 2);
        assert_eq!(decoded_index.block_target_bytes, 128);
        assert_eq!(decoded_index.block_max_records, 2);
        assert!(decoded_index.bloom_k > 0);
        assert!(!decoded_index.bloom_bits.is_empty());
        assert!(
            run_might_contain_key(&decoded_index, &"a".to_string()).expect("bloom check for key a")
        );

        let (offset, len) =
            run_block_window_for_key_with_bloom::<String>(&decoded_index, &"b".to_string(), true)
                .expect("window")
                .expect("window exists");
        let block = &encoded.payload[offset as usize..offset as usize + len];
        let decoded_records = decode_run_block::<String, String>(block).expect("decode run block");
        assert_eq!(decoded_records.len(), 2);
        assert_eq!(decoded_records[1].op, Op::Del);

        let looked_up =
            lookup_key_in_run_block::<String, String>(block, &"b".to_string()).expect("lookup");
        assert_eq!(looked_up, Some(None));
    }

    #[test]
    fn run_block_lookup_skips_value_deserialize_for_miss() {
        let records = vec![Record {
            key: "a".to_string(),
            op: Op::Put,
            value: Some("v1".to_string()),
        }];

        let encoded = encode_run(&records, RunBuildOptions::new(64, 4)).expect("encode run");
        let index_payload = encode_run_index(&encoded.index).expect("encode run index");
        let decoded_index = decode_run_index(&index_payload).expect("decode run index");
        let (offset, len) =
            run_block_window_for_key_with_bloom::<String>(&decoded_index, &"a".to_string(), true)
                .expect("window")
                .expect("window exists");

        let mut corrupted = encoded.payload[offset as usize..offset as usize + len].to_vec();
        let last = corrupted.len() - 1;
        corrupted[last] = b'{';

        let miss = lookup_key_in_run_block::<String, String>(&corrupted, &"z".to_string())
            .expect("miss lookup");
        assert_eq!(miss, None);

        let hit_err = lookup_key_in_run_block::<String, String>(&corrupted, &"a".to_string())
            .expect_err("hit should fail due to corrupted value payload");
        assert!(format!("{hit_err}").contains("run value decode"));
    }
}
