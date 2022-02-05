use crate::{
    sys::{
        io_scheduler::internal::IOSchedulerPrivate,
        source::PinnedInnerSource,
        InnerSource,
        SourceType,
    },
    IoRequirements,
};
use std::collections::VecDeque;

/// A queue for operations waiting to be submitted to the kernel
pub(super) trait IOSchedulerQueue {
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn push(&mut self, src: PinnedInnerSource);
    fn pop(&mut self) -> Option<PinnedInnerSource>;
    fn peek(&self) -> Option<&PinnedInnerSource>;
}

/// A simple FIFO queue
#[derive(Debug, Default)]
pub(super) struct FIFOQueue {
    inner: VecDeque<PinnedInnerSource>,
}

impl FIFOQueue {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
        }
    }
}

impl IOSchedulerQueue for FIFOQueue {
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn push(&mut self, src: PinnedInnerSource) {
        self.inner.push_back(src)
    }

    fn pop(&mut self) -> Option<PinnedInnerSource> {
        self.inner.pop_front()
    }

    fn peek(&self) -> Option<&PinnedInnerSource> {
        self.inner.front()
    }
}

mod internal {
    use crate::sys::source::PinnedInnerSource;

    pub(crate) trait IOSchedulerPrivate {
        /// Peek into the next source, if any
        /// Return true is the source is an emergency
        fn peek(&self) -> Option<(&PinnedInnerSource, bool)>;

        /// Pop the next source, if any
        /// Return true is the source is an emergency
        fn pop(&mut self) -> Option<(PinnedInnerSource, bool)>;
    }
}

/// An IO scheduler decides what requests go to the reactor and when
pub(crate) trait IOScheduler: internal::IOSchedulerPrivate {
    /// Whether there are any request queued up in the scheduler
    fn is_empty(&self) -> bool;

    /// The number of requests queued up in the scheduler
    fn len(&self) -> usize;

    /// Schedule a request
    fn schedule(&mut self, src: PinnedInnerSource);

    /// Cancel a request
    fn cancel(&mut self, src: &PinnedInnerSource);

    /// Schedule an emergency request. Emergency requests are guaranteed to be
    /// submitted to the kernel before the reactor returns, even if that means
    /// blocking.
    fn schedule_emergency(&mut self, src: PinnedInnerSource);

    /// Create a scheduler session
    /// A session is needed to pop and peek inside the scheduler.
    fn open_session(&mut self) -> IOSchedulerSession<'_>;
}

/// A FIFO scheduler that doesn't prioritize any requests except for
/// cancellations
#[derive(Debug, Default)]
pub(crate) struct FIFOScheduler {
    emergency: FIFOQueue,
    cancellations: FIFOQueue,
    submissions: FIFOQueue,
}

impl FIFOScheduler {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            emergency: FIFOQueue::with_capacity(8),
            cancellations: FIFOQueue::with_capacity(capacity),
            submissions: FIFOQueue::with_capacity(capacity),
        }
    }
}

impl IOSchedulerPrivate for FIFOScheduler {
    fn peek(&self) -> Option<(&PinnedInnerSource, bool)> {
        if let Some(x) = self.emergency.peek() {
            Some((x, true))
        } else if let Some(x) = self.cancellations.peek() {
            Some((x, false))
        } else {
            self.submissions.peek().map(|x| (x, false))
        }
    }

    fn pop(&mut self) -> Option<(PinnedInnerSource, bool)> {
        if let Some(x) = self.emergency.pop() {
            Some((x, true))
        } else if let Some(x) = self.cancellations.pop() {
            Some((x, false))
        } else {
            self.submissions.pop().map(|x| (x, false))
        }
    }
}

impl IOScheduler for FIFOScheduler {
    fn is_empty(&self) -> bool {
        self.emergency.is_empty() && self.cancellations.is_empty() && self.submissions.is_empty()
    }

    fn len(&self) -> usize {
        self.emergency.len() + self.cancellations.len() + self.submissions.len()
    }

    fn schedule(&mut self, src: PinnedInnerSource) {
        self.submissions.push(src);
    }

    fn cancel(&mut self, src: &PinnedInnerSource) {
        self.cancellations.push(InnerSource::pin(
            IoRequirements::default(),
            SourceType::Cancel(src.clone()),
            None,
            None,
        ));
    }

    fn schedule_emergency(&mut self, src: PinnedInnerSource) {
        self.emergency.push(src);
    }

    fn open_session(&mut self) -> IOSchedulerSession<'_> {
        IOSchedulerSession {
            sched: self,
            should_drain: false,
            sealed: false,
        }
    }
}

pub(super) enum IOSchedulerSessionState {
    /// The reactor should drain all prior sources and confirm they have been
    /// submitted to the kernel successfully, the reactor is allowed to block,
    /// as needed. The reactor must open a new scheduler session immediately
    /// afterwards.
    NeedsDrain,

    /// There are additional sources waiting for submissions in the scheduler.
    /// The reactor may open a new session to pop them.
    WaitingSubmission,

    /// The scheduler is empty and no further sources will be yielded
    Exhausted,
}

/// A scheduler session is used to pull requests from a scheduler.
/// Once it is closed, the session returns a state that indicates to the rector
/// what to do next.
pub(crate) struct IOSchedulerSession<'a> {
    sched: &'a mut dyn IOScheduler,
    should_drain: bool,
    sealed: bool,
}

impl<'a> Drop for IOSchedulerSession<'a> {
    fn drop(&mut self) {
        assert!(self.sealed, "dropping without sealing is illegal");
    }
}

impl<'a> IOSchedulerSession<'a> {
    pub(super) fn peek(&self) -> Option<&PinnedInnerSource> {
        self.sched
            .peek()
            .filter(|(_, emergency)| {
                // the last source yielded was an emergency. The reactor
                // should be drained first!
                *emergency || !self.should_drain
            })
            .map(|(s, _)| s)
    }

    pub(super) fn pop(&mut self) -> Option<PinnedInnerSource> {
        if self.peek().is_some() {
            let (source, should_drain) = self.sched.pop().unwrap();
            self.should_drain = should_drain;
            Some(source)
        } else {
            None
        }
    }

    /// Seal closes the session and returns true if all prior sources yielded
    /// should be flushed to the kernel
    pub(super) fn seal(mut self) -> IOSchedulerSessionState {
        self.sealed = true;
        if self.should_drain {
            IOSchedulerSessionState::NeedsDrain
        } else if !self.sched.is_empty() {
            IOSchedulerSessionState::WaitingSubmission
        } else {
            IOSchedulerSessionState::Exhausted
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        sys::{
            io_scheduler::{FIFOScheduler, IOScheduler, IOSchedulerSessionState},
            source::PinnedInnerSource,
            InnerSource,
            SourceType,
        },
        IoRequirements,
    };

    fn dummy() -> PinnedInnerSource {
        InnerSource::pin(IoRequirements::default(), SourceType::Invalid, None, None)
    }

    #[test]
    fn scheduler_fragmented_submission() {
        let mut sched = FIFOScheduler::with_capacity(10);

        for _ in 0..32 {
            sched.schedule(dummy());
        }

        assert!(matches!(
            sched.open_session().seal(),
            IOSchedulerSessionState::WaitingSubmission
        ));
        assert_eq!(sched.len(), 32);
        assert!(!sched.is_empty());

        {
            let mut session = sched.open_session();
            for _ in 0..16 {
                assert!(matches!(
                    session.peek().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
                assert!(matches!(
                    session.pop().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
            }

            assert!(matches!(
                session.seal(),
                IOSchedulerSessionState::WaitingSubmission
            ));
        }
        assert_eq!(sched.len(), 16);
        assert!(!sched.is_empty());

        {
            let mut session = sched.open_session();
            for _ in 0..16 {
                assert!(matches!(
                    session.peek().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
                assert!(matches!(
                    session.pop().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
            }

            assert!(matches!(session.seal(), IOSchedulerSessionState::Exhausted));
        }
        assert_eq!(sched.len(), 0);
        assert!(sched.is_empty());
    }

    #[test]
    fn scheduler_cancellation_precedence() {
        let mut sched = FIFOScheduler::with_capacity(10);

        for x in 0..32 {
            if x % 2 == 0 {
                sched.schedule(dummy());
            } else {
                sched.cancel(&dummy());
            }
        }

        assert!(matches!(
            sched.open_session().seal(),
            IOSchedulerSessionState::WaitingSubmission
        ));
        assert_eq!(sched.len(), 32);
        assert!(!sched.is_empty());

        {
            let mut session = sched.open_session();
            for _ in 0..16 {
                assert!(matches!(
                    session.peek().unwrap().borrow().source_type,
                    SourceType::Cancel(_)
                ));
                assert!(matches!(
                    session.pop().unwrap().borrow().source_type,
                    SourceType::Cancel(_)
                ));
            }

            assert!(matches!(
                session.seal(),
                IOSchedulerSessionState::WaitingSubmission
            ));
        }
        assert_eq!(sched.len(), 16);
        assert!(!sched.is_empty());

        {
            let mut session = sched.open_session();
            for _ in 0..16 {
                assert!(matches!(
                    session.peek().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
                assert!(matches!(
                    session.pop().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
            }

            assert!(matches!(session.seal(), IOSchedulerSessionState::Exhausted));
        }
        assert_eq!(sched.len(), 0);
        assert!(sched.is_empty());
    }

    #[test]
    fn scheduler_emergency_trigger_drain() {
        let mut sched = FIFOScheduler::with_capacity(10);

        for x in 0..32 {
            if x % 2 == 0 {
                sched.schedule(dummy());
            } else {
                sched.cancel(&dummy());
            }
            sched.schedule_emergency(dummy());
        }

        assert!(matches!(
            sched.open_session().seal(),
            IOSchedulerSessionState::WaitingSubmission
        ));
        assert_eq!(sched.len(), 64);
        assert!(!sched.is_empty());

        {
            let mut session = sched.open_session();
            for _ in 0..16 {
                assert!(matches!(
                    session.peek().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
                assert!(matches!(
                    session.pop().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
            }

            assert!(matches!(
                session.seal(),
                IOSchedulerSessionState::NeedsDrain
            ));
        }
        assert_eq!(sched.len(), 48);
        assert!(!sched.is_empty());

        {
            let mut session = sched.open_session();
            for _ in 0..16 {
                assert!(matches!(
                    session.peek().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
                assert!(matches!(
                    session.pop().unwrap().borrow().source_type,
                    SourceType::Invalid
                ));
            }

            assert!(matches!(
                session.seal(),
                IOSchedulerSessionState::NeedsDrain
            ));
        }
        assert_eq!(sched.len(), 32);
        assert!(!sched.is_empty());

        assert!(matches!(
            sched.open_session().seal(),
            IOSchedulerSessionState::WaitingSubmission
        ));
    }

    #[test]
    #[should_panic]
    fn unsealed_session() {
        let mut sched = FIFOScheduler::with_capacity(10);
        sched.open_session();
    }
}
