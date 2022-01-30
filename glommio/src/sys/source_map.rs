use crate::{
    free_list::{FreeList, Idx},
    sys::{source::PinnedInnerSource, EnqueuedSource, EnqueuedStatus, InnerSource, ReactorQueue},
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
    pub(super) fn add_source(&mut self, src: PinnedInnerSource, queue: ReactorQueue) -> SourceId {
        let id = self.alloc(src);
        self.peek_source_mut(id, |mut src| {
            src.enqueued.replace(EnqueuedSource {
                id,
                queue,
                status: EnqueuedStatus::Enqueued,
            });
        });
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
