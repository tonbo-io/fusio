#![cfg_attr(docsrs, feature(doc_cfg))]

pub(crate) mod checkpoint;
pub mod compactor; // Headless remote compactor façade
pub mod db;
pub(crate) mod gc; // TODO: GC plan (stub)
pub(crate) mod head;
pub(crate) mod lease;
pub mod session;
pub mod snapshot; // Snapshot types and snapshot-bound reader // Unified read/write session API
                  // Re-export lease handle type for public read-lease APIs
pub use lease::LeaseHandle;
pub(crate) mod backoff;
pub mod options;
pub(crate) mod segment;
pub mod types;

// Ergonomic S3 entrypoint (single config wires all stores).
#[cfg(any(feature = "aws-tokio", feature = "aws-wasm"))]
pub mod s3;

// Backend implementations (grouped by provider)
pub(crate) mod impls;

#[cfg(test)]
mod tests {
    use crate::{
        head::{HeadJson, HeadStore},
        impls::mem::head::MemHeadStore,
        types::Error,
    };

    #[test]
    fn head_store_put_if_semantics() {
        use futures_executor::block_on;

        use crate::head::PutCondition;
        let store = MemHeadStore::new();
        let head = HeadJson {
            version: 1,
            snapshot: None,
            last_segment_seq: None,
            last_lsn: 7,
        };
        // First publish should succeed as if_not_exists
        let tag = block_on(store.put(&head, PutCondition::IfNotExists)).unwrap();
        // Second publish with IfNotExists should fail
        assert!(matches!(
            block_on(store.put(&head, PutCondition::IfNotExists)),
            Err(Error::PreconditionFailed)
        ));
        // Publish with IfMatch current tag should succeed
        let _ = block_on(store.put(
            &HeadJson {
                version: 2,
                ..head.clone()
            },
            PutCondition::IfMatch(tag),
        ))
        .unwrap();
    }
}

#[cfg(test)]
mod segment_tests {
    use futures_executor::block_on;

    use crate::segment::SegmentIo;

    #[test]
    fn mem_segment_put_get_list() {
        use crate::impls::mem::segment::MemSegmentStore;

        let store = MemSegmentStore::new();
        block_on(async {
            let id1 = store
                .put_next(1, b"hello", "application/json")
                .await
                .unwrap();
            let id2 = store
                .put_next(3, b"world", "application/json")
                .await
                .unwrap();
            assert_eq!(id1.seq, 1);
            assert_eq!(id2.seq, 3);
            let got = store.get(&id1).await.unwrap();
            assert_eq!(got, b"hello");
            let listed = store.list_from(0, 10).await.unwrap();
            let seqs: Vec<u64> = listed.into_iter().map(|s| s.seq).collect();
            assert_eq!(seqs, vec![1, 3]);
        })
    }
}
