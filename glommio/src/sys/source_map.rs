use crate::{
    free_list::{FreeList, Idx},
    sys::{
        io_scheduler::{FIFOScheduler, IOScheduler},
        source::PinnedInnerSource,
        SourceStatus,
    },
};
use alloc::rc::{Rc, Weak};
use std::cell::RefCell;

pub(crate) struct SourceMap {
    map: FreeList<PinnedInnerSource>,
    pub(super) scheduler: Rc<RefCell<FIFOScheduler>>,
}

pub(crate) type SourceId = Idx<PinnedInnerSource>;
pub(super) fn from_user_data(user_data: u64) -> SourceId {
    SourceId::from_raw((user_data - 1) as usize)
}
pub(super) fn to_user_data(id: SourceId) -> u64 {
    id.to_raw() as u64 + 1
}

impl SourceMap {
    pub(super) fn new(sched: &Rc<RefCell<FIFOScheduler>>) -> Self {
        Self {
            map: Default::default(),
            scheduler: sched.clone(),
        }
    }

    pub(super) fn add_source(&mut self, src: PinnedInnerSource) -> SourceId {
        let id = self.map.alloc(src);
        self.map[id]
            .borrow_mut()
            .status
            .replace(SourceStatus::Dispatched(
                Rc::downgrade(&self.scheduler) as Weak<RefCell<dyn IOScheduler>>,
                id,
            ));
        id
    }

    pub(super) fn consume_source(&mut self, id: SourceId) -> PinnedInnerSource {
        let source = self.map.dealloc(id);
        source.borrow_mut().status.take();

        source
    }
}
