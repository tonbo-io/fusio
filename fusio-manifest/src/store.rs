use std::sync::Arc;

use crate::options::Options;

/// Shared for head/segment/checkpoint/lease stores plus options.
pub(crate) struct Store<HS, SS, CS, LS> {
    pub(crate) head: HS,
    pub(crate) segment: SS,
    pub(crate) checkpoint: CS,
    pub(crate) leases: LS,
    pub(crate) opts: Options,
}

impl<HS, SS, CS, LS> Store<HS, SS, CS, LS> {
    pub(crate) fn new(head: HS, segment: SS, checkpoint: CS, leases: LS, opts: Options) -> Self {
        Self {
            head,
            segment,
            checkpoint,
            leases,
            opts,
        }
    }
}

pub(crate) type StoreHandle<HS, SS, CS, LS> = Arc<Store<HS, SS, CS, LS>>;
