// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! An executor for running async tasks.

#![forbid(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations)]

use crate::{
    executor::{maybe_activate, TaskQueue},
    task::{task_impl, JoinHandle},
};
use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    marker::PhantomData,
    panic::{RefUnwindSafe, UnwindSafe},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

/// A runnable future, ready for execution.
///
/// When a future is internally spawned using `task::spawn()` or
/// `task::spawn_local()`, we get back two values:
///
/// 1. an `task::Task<()>`, which we refer to as a `Runnable`
/// 2. an `task::JoinHandle<T, ()>`, which is wrapped inside a `Task<T>`
///
/// Once a `Runnable` is run, it "vanishes" and only reappears when its future
/// is woken. When it's woken up, its schedule function is called, which means
/// the `Runnable` gets pushed into a task queue in an executor.
pub(crate) type Runnable = task_impl::Task;

/// An identity fingerprint of a runnable task; useful to compare two tasks for
/// equality without keeping references
pub(crate) type RunnableId = task_impl::TaskId;

/// A spawned future.
///
/// Tasks are also futures themselves and yield the output of the spawned
/// future.
///
/// When a task is dropped, its gets canceled and won't be polled again. To
/// cancel a task a bit more gracefully and wait until it stops running, use the
/// [`cancel()`][Task::cancel()] method.
///
/// Tasks that panic get immediately canceled. Awaiting a canceled task also
/// causes a panic.
///
/// If a task panics, the panic will be thrown by the [`Ticker::tick()`]
/// invocation that polled it.
///
/// ```
#[must_use = "tasks get canceled when dropped, use `.detach()` to run them in the background"]
#[derive(Debug)]
pub(crate) struct Task<T>(Option<JoinHandle<T>>);

impl<T> Task<T> {
    /// Detaches the task to let it keep running in the background.
    pub(crate) fn detach(mut self) -> JoinHandle<T> {
        self.0.take().unwrap()
    }

    /// Cancels the task and waits for it to stop running.
    pub(crate) async fn cancel(self) -> Option<T> {
        let mut task = self;
        let handle = task.0.take().unwrap();
        handle.cancel();
        handle.await
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.cancel();
        }
    }
}

impl<T> Future for Task<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0.as_mut().unwrap())
            .poll(cx)
            .map(|output| output.expect("task has failed"))
    }
}

#[derive(Debug)]
struct LocalQueue {
    queue: VecDeque<Runnable>,
}

impl LocalQueue {
    fn new() -> Self {
        LocalQueue {
            queue: VecDeque::new(),
        }
    }

    pub(crate) fn push(&mut self, runnable: Runnable) {
        self.queue.push_back(runnable);
    }

    pub(crate) fn pop(&mut self) -> Option<Runnable> {
        self.queue.pop_front()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TaskQueueState {
    /// The queue contains scheduled, active tasks. An active queue should not
    /// be dropped, and its tasks should be executed ASAP.
    Active,
    /// The queue contains scheduled reentrant tasks.  A reentrant queue should
    /// not be dropped. Although the user intends the scheduler to schedule
    /// these tasks _eventually_, it is not a priority.
    FullyReentrant,
    /// This queue contains no scheduled task and should be dropped.
    Exhausted,
}

/// A single-threaded executor.
#[derive(Debug)]
struct LocalExecutorInner {
    /// A local queue of schedulable tasks
    active_queue: LocalQueue,
    /// A local queue of reentrant tasks that yielded control to the executor
    /// before preemption was needed; i.e., they would like to get CPU time
    /// _eventually_. Once a task enters this queue, the executor will not
    /// execute them until its next iteration
    reentrant_queue: LocalQueue,
    /// last_popped is the last runnable return by this executor
    /// It is used to avoid placing the same task in run_next
    last_popped: Option<RunnableId>,
}

/// A single-threaded executor.
#[derive(Debug)]
pub(crate) struct LocalExecutor {
    inner: RefCell<LocalExecutorInner>,

    /// Make sure the type is `!Send` and `!Sync`.
    _marker: PhantomData<Rc<()>>,
}

impl UnwindSafe for LocalExecutor {}

impl RefUnwindSafe for LocalExecutor {}

impl LocalExecutor {
    /// Creates a new single-threaded executor.
    pub(crate) fn new() -> LocalExecutor {
        LocalExecutor {
            inner: RefCell::new(LocalExecutorInner {
                active_queue: LocalQueue::new(),
                reentrant_queue: LocalQueue::new(),
                last_popped: None,
            }),
            _marker: PhantomData,
        }
    }

    /// Spawns a thread-local future onto this executor.
    fn spawn<T>(
        &self,
        executor_id: usize,
        tq: Rc<RefCell<TaskQueue>>,
        future: impl Future<Output = T>,
    ) -> (Runnable, JoinHandle<T>) {
        let tq = Rc::downgrade(&tq);

        // The function that schedules a runnable task when it gets woken up.
        let schedule = move |runnable: Runnable| {
            let tq = tq.upgrade();

            if let Some(tq) = tq {
                {
                    let tq = tq.borrow();
                    tq.ex.push_task(runnable, tq.yielded);
                }

                maybe_activate(tq);
            }
        };

        // Create a task, push it into the queue by scheduling it, and return its `Task`
        // handle.
        task_impl::spawn_local(executor_id, future, schedule)
    }

    /// Creates a [`Task`] for a given future and runs it immediately
    pub(crate) fn spawn_and_run<T>(
        &self,
        executor_id: usize,
        tq: Rc<RefCell<TaskQueue>>,
        future: impl Future<Output = T>,
    ) -> Task<T> {
        let (runnable, handle) = self.spawn(executor_id, tq, future);
        runnable.run_right_away();
        Task(Some(handle))
    }

    /// Creates a [`Task`] for a given future and schedules it to run eventually
    pub(crate) fn spawn_and_schedule<T>(
        &self,
        executor_id: usize,
        tq: Rc<RefCell<TaskQueue>>,
        future: impl Future<Output = T>,
    ) -> Task<T> {
        let (runnable, handle) = self.spawn(executor_id, tq, future);
        runnable.schedule();
        Task(Some(handle))
    }

    /// Gets one task from the queue, if one exists.
    ///
    /// Returns an option rapping the task.
    pub(crate) fn get_task(&self) -> Option<Runnable> {
        let mut this = self.inner.borrow_mut();
        let ret = this.active_queue.pop();
        this.last_popped = ret.as_ref().map(RunnableId::from);
        ret
    }

    /// Reset the state of this [`LocalExecutor`] to be fair wrt scheduling
    /// decisions and return the state of the queue.
    pub(crate) fn make_fair(&self) -> TaskQueueState {
        let mut this = self.inner.borrow_mut();

        let any_active = !this.active_queue.queue.is_empty();
        let any_reentrant = !this.reentrant_queue.queue.is_empty();
        let ret = match (any_active, any_reentrant) {
            (true, _) => TaskQueueState::Active,
            (false, true) => TaskQueueState::FullyReentrant,
            (false, false) => TaskQueueState::Exhausted,
        };

        // Put all the reentrant tasks at the back of the active queue such that we get
        // to them next time the queue is polled.
        let reentrant = std::mem::take(&mut this.reentrant_queue.queue);
        this.active_queue.queue.extend(reentrant);

        ret
    }

    /// Schedule a given task for execution on this LocalExecutor
    /// If next is false, runnable is added to the tail of the runnable queue.
    /// If next is true, runnable is set as the immediate next task to run.
    fn push_task(&self, runnable: Runnable, yielded: bool) {
        let mut this = self.inner.borrow_mut();

        // Check whether the task we try to schedule is the same that's currently
        // running, if any. If so then the task is yielding voluntarily so we
        // put it at the tail of the queue
        let mut reentrant = this
            .last_popped
            .as_ref()
            .map_or(false, |last| *last == RunnableId::from(&runnable));

        // If the task scheduled itself back because of preemption, we don't consider
        // it reentrant (it likely has more work to do, and we want to get to it during
        // this executor tick).
        reentrant &= !yielded;

        if reentrant {
            this.reentrant_queue.push(runnable)
        } else {
            this.active_queue.push(runnable)
        }
    }
}
