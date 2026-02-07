use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    manifest::{Op, Record},
    types::{Error, Result},
};

pub(crate) const RUN_DATA_CONTENT_TYPE: &str = "application/vnd.fusio-manifest.run-v2+bin";
pub(crate) const RUN_INDEX_CONTENT_TYPE: &str = "application/vnd.fusio-manifest.run-index-v2+bin";

const RUN_DATA_MAGIC: &[u8; 4] = b"FRD2";
const RUN_INDEX_MAGIC: &[u8; 4] = b"FRI2";
const RUN_BLOCK_MAGIC: &[u8; 4] = b"FRB2";
const RUN_FORMAT_VERSION: u8 = 1;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_MIN_BITS: usize = 64;

const OP_PUT_TAG: u8 = 1;
const OP_DEL_TAG: u8 = 2;

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
    pub block_record_stride: u32,
    pub bloom_k: u8,
    pub bloom_bits: Vec<u8>,
    pub blocks: Vec<RunBlockMeta>,
}

pub(crate) struct EncodedRun {
    pub payload: Vec<u8>,
    pub index: RunIndexMeta,
}

pub(crate) fn encode_run<K, V>(
    records: &[Record<K, V>],
    block_record_stride: usize,
) -> Result<EncodedRun>
where
    K: Serialize,
    V: Serialize,
{
    let stride = block_record_stride.max(1);
    let mut payload = Vec::new();
    payload.extend_from_slice(RUN_DATA_MAGIC);
    payload.push(RUN_FORMAT_VERSION);

    let mut blocks = Vec::new();
    for chunk in records.chunks(stride) {
        let first_key_json = serde_json::to_vec(&chunk[0].key)
            .map_err(|e| Error::Corrupt(format!("run key encode: {e}")))?;
        let last_key_json = serde_json::to_vec(&chunk[chunk.len() - 1].key)
            .map_err(|e| Error::Corrupt(format!("run key encode: {e}")))?;

        let block = encode_run_block(chunk)?;
        let offset = u64::try_from(payload.len())
            .map_err(|_| Error::Corrupt("run payload too large".into()))?;
        let len =
            u32::try_from(block.len()).map_err(|_| Error::Corrupt("run block too large".into()))?;
        let record_count = u32::try_from(chunk.len())
            .map_err(|_| Error::Corrupt("run block has too many records".into()))?;
        payload.extend_from_slice(&block);
        blocks.push(RunBlockMeta {
            first_key_json,
            last_key_json,
            offset,
            len,
            record_count,
        });
    }

    let (key_min_json, key_max_json) =
        if let (Some(first), Some(last)) = (records.first(), records.last()) {
            (
                serde_json::to_vec(&first.key)
                    .map_err(|e| Error::Corrupt(format!("run key encode: {e}")))?,
                serde_json::to_vec(&last.key)
                    .map_err(|e| Error::Corrupt(format!("run key encode: {e}")))?,
            )
        } else {
            (Vec::new(), Vec::new())
        };

    let record_count = u64::try_from(records.len())
        .map_err(|_| Error::Corrupt("run has too many records".into()))?;
    let payload_byte_size =
        u64::try_from(payload.len()).map_err(|_| Error::Corrupt("run payload too large".into()))?;
    let block_record_stride =
        u32::try_from(stride).map_err(|_| Error::Corrupt("run block stride too large".into()))?;
    let (bloom_k, bloom_bits) = build_bloom(records)?;

    Ok(EncodedRun {
        payload,
        index: RunIndexMeta {
            key_min_json,
            key_max_json,
            record_count,
            payload_byte_size,
            block_record_stride,
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
    out.extend_from_slice(&index.block_record_stride.to_le_bytes());
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
    let block_record_stride = read_u32(bytes, &mut cursor, "run index block_record_stride")?;
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
    if (record_count == 0) != blocks.is_empty() {
        return Err(Error::Corrupt(
            "run index record_count does not match block list".into(),
        ));
    }

    Ok(RunIndexMeta {
        key_min_json,
        key_max_json,
        record_count,
        payload_byte_size,
        block_record_stride,
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
    let mut cursor = 0;
    ensure_magic(bytes, &mut cursor, RUN_BLOCK_MAGIC, "run block")?;
    let version = read_u8(bytes, &mut cursor, "run block version")?;
    if version != RUN_FORMAT_VERSION {
        return Err(Error::Corrupt(format!(
            "unsupported run block version: {version}"
        )));
    }

    let record_count = read_u32(bytes, &mut cursor, "run block record_count")?;
    let mut out = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let key_json = read_bytes(bytes, &mut cursor, "run block key")?;
        let key = serde_json::from_slice(&key_json)
            .map_err(|e| Error::Corrupt(format!("run key decode: {e}")))?;
        let op_tag = read_u8(bytes, &mut cursor, "run block op")?;
        match op_tag {
            OP_PUT_TAG => {
                let value_json = read_bytes(bytes, &mut cursor, "run block value")?;
                let value = serde_json::from_slice(&value_json)
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
                    "run block has unknown op tag: {op_tag}"
                )));
            }
        }
    }

    if cursor != bytes.len() {
        return Err(Error::Corrupt("run block has trailing bytes".into()));
    }
    Ok(out)
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

fn encode_run_block<K, V>(records: &[Record<K, V>]) -> Result<Vec<u8>>
where
    K: Serialize,
    V: Serialize,
{
    let mut out = Vec::new();
    out.extend_from_slice(RUN_BLOCK_MAGIC);
    out.push(RUN_FORMAT_VERSION);
    let record_count = u32::try_from(records.len())
        .map_err(|_| Error::Corrupt("run block has too many records".into()))?;
    out.extend_from_slice(&record_count.to_le_bytes());
    for record in records {
        let key_json = serde_json::to_vec(&record.key)
            .map_err(|e| Error::Corrupt(format!("run key encode: {e}")))?;
        write_bytes(&mut out, &key_json)?;

        match record.op {
            Op::Put => {
                out.push(OP_PUT_TAG);
                let value = record
                    .value
                    .as_ref()
                    .ok_or_else(|| Error::Corrupt("put record missing value".into()))?;
                let value_json = serde_json::to_vec(value)
                    .map_err(|e| Error::Corrupt(format!("run value encode: {e}")))?;
                write_bytes(&mut out, &value_json)?;
            }
            Op::Del => {
                out.push(OP_DEL_TAG);
            }
        }
    }
    Ok(out)
}

fn build_bloom<K, V>(records: &[Record<K, V>]) -> Result<(u8, Vec<u8>)>
where
    K: Serialize,
    V: Serialize,
{
    if records.is_empty() {
        return Ok((0, Vec::new()));
    }

    let target_bits = records
        .len()
        .saturating_mul(BLOOM_BITS_PER_KEY)
        .max(BLOOM_MIN_BITS);
    let bytes_len = target_bits.div_ceil(8);
    let mut bits = vec![0_u8; bytes_len];
    let k = ((BLOOM_BITS_PER_KEY as f64 * std::f64::consts::LN_2).round() as u8).clamp(1, 15);

    for record in records {
        let key_json = serde_json::to_vec(&record.key)
            .map_err(|e| Error::Corrupt(format!("run key encode: {e}")))?;
        bloom_insert(&mut bits, k, &key_json);
    }
    Ok((k, bits))
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

        let encoded = encode_run(&records, 2).expect("encode run");
        let index_payload = encode_run_index(&encoded.index).expect("encode run index");
        let decoded_index = decode_run_index(&index_payload).expect("decode run index");
        assert_eq!(decoded_index.blocks.len(), 2);
        assert!(decoded_index.bloom_k > 0);
        assert!(!decoded_index.bloom_bits.is_empty());
        assert!(
            run_might_contain_key(&decoded_index, &"a".to_string()).expect("bloom check for key a")
        );
        assert!(
            run_might_contain_key(&decoded_index, &"b".to_string()).expect("bloom check for key b")
        );
        assert!(
            run_might_contain_key(&decoded_index, &"c".to_string()).expect("bloom check for key c")
        );

        let (offset, len) =
            run_block_window_for_key_with_bloom::<String>(&decoded_index, &"b".to_string(), true)
                .expect("window")
                .expect("window exists");
        let block = &encoded.payload[offset as usize..offset as usize + len];
        let decoded_records = decode_run_block::<String, String>(block).expect("decode run block");
        assert_eq!(decoded_records.len(), 2);
        assert_eq!(decoded_records[1].op, Op::Del);
    }
}
