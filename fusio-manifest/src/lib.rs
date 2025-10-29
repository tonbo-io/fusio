#![cfg_attr(docsrs, feature(doc_cfg))]

pub(crate) mod checkpoint;
pub mod compactor; // Headless remote compactor façade
pub(crate) mod gc;
pub(crate) mod head;
pub(crate) mod lease;
pub mod manifest;
pub mod session;
pub mod snapshot; // Snapshot types and snapshot-bound reader // Unified read/write session API
                  // Re-export lease handle type for public read-lease APIs
pub use lease::{keeper::LeaseKeeper, LeaseHandle};

pub use crate::{
    checkpoint::{CheckpointStore, CheckpointStoreImpl},
    head::{HeadStore, HeadStoreImpl},
    lease::{LeaseStore, LeaseStoreImpl},
    segment::{SegmentIo, SegmentStoreImpl},
};
pub(crate) mod backoff;
pub use backoff::BackoffPolicy;
pub mod context;
pub use context::ManifestContext;
pub use fusio::executor::BlockingExecutor;
pub(crate) mod cache;
pub mod retention;
pub(crate) mod segment;
pub(crate) mod store;
pub use cache::CacheLayer;
#[cfg(feature = "cache-moka")]
pub use cache::MemoryBlobCache;
pub mod types;

// Ergonomic S3 entrypoint (single config wires all stores).
pub mod s3;

#[cfg(test)]
pub(crate) mod test_utils;

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::{
        head::{HeadJson, HeadStore},
        test_utils::{in_memory_stores, InMemoryStores},
        types::Error,
    };

    #[rstest]
    fn head_store_put_if_semantics(in_memory_stores: InMemoryStores) {
        use futures_executor::block_on;

        use crate::head::PutCondition;
        let store = in_memory_stores.head;
        let head = HeadJson {
            version: 1,
            checkpoint_id: None,
            last_segment_seq: None,
            last_txn_id: 7,
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
    use rstest::rstest;

    use crate::{
        segment::SegmentIo,
        test_utils::{in_memory_stores, InMemoryStores},
    };

    #[rstest]
    fn mem_segment_put_get_list(in_memory_stores: InMemoryStores) {
        let store = in_memory_stores.segment;
        block_on(async {
            let id1 = store
                .put_next(1, 10, b"hello", "application/json")
                .await
                .unwrap();
            let id2 = store
                .put_next(3, 12, b"world", "application/json")
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
