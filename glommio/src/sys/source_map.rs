use crate::{
    free_list::{FreeList, Idx},
    sys::{
        source::PinnedInnerSource,
        EnqueuedSource,
        EnqueuedStatus,
        InnerSource,
        ReactorQueue,
        Source,
    },
};
use std::cell::RefMut;

pub(super) type SourceMap = FreeList<PinnedInnerSource>;
pub(crate) type SourceId = Idx<PinnedInnerSource>;
pub(super) fn from_user_data(user_data: u64) -> SourceId {
    SourceId::from_raw((user_data - 1) as usize)
}
pub(super) fn to_user_data(id: SourceId) -> u64 {
    id.to_raw() as u64 + 1
}

impl SourceMap {
    pub(super) fn add_source(&mut self, source: &Source, queue: ReactorQueue) -> SourceId {
        let item = source.inner.clone();
        let id = self.alloc(item);
        let status = EnqueuedStatus::Enqueued;
        source
            .inner
            .borrow_mut()
            .enqueued
            .replace(EnqueuedSource { id, queue, status });
        id
    }

    pub(super) fn peek_source_mut<R, Fn: for<'a> FnOnce(RefMut<'a, InnerSource>) -> R>(
        &mut self,
        id: SourceId,
        f: Fn,
    ) -> R {
        f(self[id].borrow_mut())
    }

    pub(super) fn consume_source(&mut self, id: SourceId) -> PinnedInnerSource {
        let source = self.dealloc(id);
        source.borrow_mut().enqueued.take();
        source
    }
}
