#![cfg_attr(docsrs, feature(doc_cfg))]

pub(crate) mod checkpoint;
pub mod db;
pub(crate) mod head;
pub(crate) mod lease;
pub mod options;
pub(crate) mod segment;
pub mod types;

#[cfg(test)]
mod tests {
    #[cfg(feature = "mem")]
    use crate::head::MemHeadStore;
    use crate::{
        head::{HeadJson, HeadStore},
        types::Error,
    };

    #[cfg(feature = "mem")]
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

    #[cfg(feature = "mem")]
    #[test]
    fn mem_segment_put_get_list() {
        use crate::segment;

        let store = segment::MemSegmentStore::new();
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
