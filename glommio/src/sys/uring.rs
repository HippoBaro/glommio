// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use log::warn;
use nix::poll::PollFlags;
use rlimit::Resource;
use std::{
    cell::{Cell, RefCell},
    io,
    os::{raw::c_int, unix::io::RawFd},
    panic,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    iou,
    iou::{
        sqe::{FsyncFlags, StatxFlags, StatxMode, TimeoutFlags},
        SQEs,
    },
    sys::{
        self,
        blocking::{BlockingThreadOp, BlockingThreadPool},
        buffer::UringBufferAllocator,
        dma_buffer::DmaBuffer,
        io_scheduler::{FIFOScheduler, IOScheduler, IOSchedulerSession, IOSchedulerSessionState},
        source::PinnedInnerSource,
        source_map::{from_user_data, to_user_data, SourceMap},
        uring::internal::Uring,
        DirectIo,
        InnerSource,
        IoBuffer,
        PollableStatus,
        Source,
        SourceStatus,
        SourceType,
        TimeSpec64,
    },
    uring_sys::{self, IoRingOp},
    GlommioError,
    IoRequirements,
    IoStats,
    PoolPlacement,
    ReactorErrorKind,
    RingIoStats,
    TaskQueueHandle,
};
use ahash::AHashMap;
use nix::sys::socket::{MsgFlags, SockFlag};

const MSG_ZEROCOPY: i32 = 0x4000000;
const EVENTFD_WAKEUP: u64 = 1u64;

fn check_supported_operations(ops: &[uring_sys::IoRingOp]) -> bool {
    unsafe {
        let probe = uring_sys::io_uring_get_probe();
        if probe.is_null() {
            panic!(
                "Failed to register a probe. The most likely reason is that your kernel witnessed \
                 Romulus killing Remus (too old!! kernel should be at least 5.8)"
            );
        }

        let mut ret = true;
        for op in ops {
            let opint = *{ op as *const uring_sys::IoRingOp as *const libc::c_int };
            let sup = uring_sys::io_uring_opcode_supported(probe, opint) > 0;
            ret &= sup;
            if !sup {
                println!(
                    "Yo kernel is so old it was with Hannibal when he crossed the Alps! Missing \
                     {:?}",
                    op
                );
            }
        }
        uring_sys::io_uring_free_probe(probe);
        if !ret {
            eprintln!("Your kernel is older than Caesar. Bye");
            std::process::exit(1);
        }
        ret
    }
}

static GLOMMIO_URING_OPS: &[IoRingOp] = &[
    IoRingOp::IORING_OP_NOP,
    IoRingOp::IORING_OP_READV,
    IoRingOp::IORING_OP_WRITEV,
    IoRingOp::IORING_OP_FSYNC,
    IoRingOp::IORING_OP_READ_FIXED,
    IoRingOp::IORING_OP_WRITE_FIXED,
    IoRingOp::IORING_OP_POLL_ADD,
    IoRingOp::IORING_OP_POLL_REMOVE,
    IoRingOp::IORING_OP_SENDMSG,
    IoRingOp::IORING_OP_RECVMSG,
    IoRingOp::IORING_OP_TIMEOUT,
    IoRingOp::IORING_OP_TIMEOUT_REMOVE,
    IoRingOp::IORING_OP_ACCEPT,
    IoRingOp::IORING_OP_LINK_TIMEOUT,
    IoRingOp::IORING_OP_CONNECT,
    IoRingOp::IORING_OP_FALLOCATE,
    IoRingOp::IORING_OP_OPENAT,
    IoRingOp::IORING_OP_CLOSE,
    IoRingOp::IORING_OP_STATX,
    IoRingOp::IORING_OP_READ,
    IoRingOp::IORING_OP_WRITE,
    IoRingOp::IORING_OP_SEND,
    IoRingOp::IORING_OP_RECV,
];

lazy_static! {
    static ref IO_URING_RECENT_ENOUGH: bool = check_supported_operations(GLOMMIO_URING_OPS);
}

fn sqe_required(src: &PinnedInnerSource) -> usize {
    let src = src.borrow();
    let ret = match src.source_type {
        SourceType::Rename(_, _)
        | SourceType::Truncate(_, _)
        | SourceType::CreateDir(_, _)
        | SourceType::Remove(_)
        | SourceType::BlockingFn
        | SourceType::Invalid => unreachable!(),
        SourceType::ThroughputPreemption(_, _, _) => 2,
        _ => 1,
    };

    ret + src.timeout.map(|_| 1).unwrap_or_default()
}

fn prepare_one_source<F>(src: PinnedInnerSource, mut sqes: SQEs<'_>, buffer_allocation: F)
where
    F: FnOnce(usize) -> Option<DmaBuffer>,
{
    let mut sqes = sqes.soft_linked();
    let mut src = src.borrow_mut();
    let mut sqe = sqes.next().unwrap();
    match &mut src.source_type {
        SourceType::Write(fd, pos, pollable, buffer) => unsafe {
            match (pollable, buffer.uring_buffer_id()) {
                (
                    PollableStatus::NonPollable(DirectIo::Enabled) | PollableStatus::Pollable,
                    Some(buffer_id),
                ) => {
                    sqe.prep_write_fixed(*fd, buffer, *pos, buffer_id as usize);
                }
                _ => {
                    sqe.prep_write(*fd, buffer as &[u8], *pos);
                }
            }
        },
        SourceType::Read(fd, pos, size, pollable, buffer) => unsafe {
            // If you have a buffer here, that very likely means you are reusing the
            // source. The kernel knows about that buffer already, and will write to
            // it. So this can only be called if there is no buffer attached to it.
            assert!(
                buffer
                    .replace(IoBuffer::Dma(
                        buffer_allocation(*size).expect("Buffer allocation failed")
                    ))
                    .is_none()
            );
            let buffer = buffer.as_mut().unwrap();
            match (pollable, buffer.uring_buffer_id()) {
                (
                    PollableStatus::NonPollable(DirectIo::Enabled) | PollableStatus::Pollable,
                    Some(buffer_id),
                ) => {
                    sqe.prep_read_fixed(*fd, buffer as &mut [u8], *pos, buffer_id);
                }
                _ => {
                    sqe.prep_read(*fd, buffer as &mut [u8], *pos);
                }
            }
        },
        SourceType::PollAdd(fd, flags) => unsafe {
            sqe.prep_poll_add(*fd, *flags);
        },
        SourceType::SockSend(fd, buffer, flags) => unsafe {
            sqe.prep_send(
                *fd,
                buffer.as_bytes(),
                MsgFlags::from_bits_unchecked(flags.bits() | MSG_ZEROCOPY),
            );
        },
        SourceType::SockRecv(fd, size, buffer, flags) => unsafe {
            // If you have a buffer here, that very likely means you are reusing the
            // source. The kernel knows about that buffer already, and will write to
            // it. So this can only be called if there is no buffer attached to it.
            assert!(
                buffer
                    .replace(DmaBuffer::new(*size).expect("failed to allocate buffer"))
                    .is_none()
            );
            let buffer = buffer.as_mut().unwrap();
            sqe.prep_recv(*fd, buffer.as_bytes_mut(), *flags);
        },
        SourceType::SockRecvMsg(fd, size, buffer, flags, iov, hdr, msg_name) => unsafe {
            // If you have a buffer here, that very likely means you are reusing the
            // source. The kernel knows about that buffer already, and will write to
            // it. So this can only be called if there is no buffer attached to it.
            assert!(
                buffer
                    .replace(DmaBuffer::new(*size).expect("failed to allocate buffer"))
                    .is_none()
            );
            let buffer = buffer.as_mut().unwrap();
            iov.iov_base = buffer.as_mut_ptr() as *mut libc::c_void;
            iov.iov_len = buffer.len();
            hdr.msg_name = msg_name.as_mut_ptr() as *mut libc::c_void;
            hdr.msg_namelen = std::mem::size_of::<nix::sys::socket::sockaddr_storage>() as _;
            hdr.msg_iov = iov as *mut libc::iovec;
            hdr.msg_iovlen = 1;
            sqe.prep_recvmsg(*fd, hdr as *mut libc::msghdr, *flags);
        },
        SourceType::SockSendMsg(fd, buffer, flags, iov, hdr, addr) => unsafe {
            let (msg_name, msg_namelen) = addr.as_ffi_pair();
            let msg_name = msg_name as *const nix::sys::socket::sockaddr as *mut libc::c_void;

            *iov = libc::iovec {
                iov_base: buffer.as_ptr() as *mut libc::c_void,
                iov_len: buffer.len(),
            };

            hdr.msg_iov = iov as *mut libc::iovec;
            hdr.msg_iovlen = 1;
            hdr.msg_name = msg_name;
            hdr.msg_namelen = msg_namelen;

            sqe.prep_sendmsg(
                *fd,
                hdr,
                MsgFlags::from_bits_unchecked(flags.bits() | MSG_ZEROCOPY),
            );
        },
        SourceType::Open(fd, path, flags, mode) => unsafe {
            sqe.prep_openat(*fd, path, *flags, *mode);
        },
        SourceType::FdataSync(fd) => unsafe {
            sqe.prep_fsync(*fd, FsyncFlags::FSYNC_DATASYNC);
        },
        SourceType::Fallocate(fd, pos, size, flags) => unsafe {
            sqe.prep_fallocate(*fd, *pos, *size as u64, *flags);
        },
        SourceType::Close(fd) => unsafe {
            sqe.prep_close(*fd);
        },
        SourceType::Statx(fd, path, buf) => unsafe {
            sqe.prep_statx(
                *fd,
                path,
                StatxFlags::AT_STATX_SYNC_AS_STAT | StatxFlags::AT_NO_AUTOMOUNT,
                StatxMode::from_bits_truncate(0x7ff),
                buf.get_mut(),
            );
        },
        SourceType::Timeout(ts, events) | SourceType::LatencyPreemption(ts, events) => unsafe {
            sqe.prep_timeout(&ts.raw, *events, TimeoutFlags::empty());
        },
        SourceType::Connect(fd, addr) => unsafe {
            sqe.prep_connect(*fd, addr);
        },
        SourceType::Accept(fd, addr) => unsafe {
            sqe.prep_accept(*fd, Some(&mut *addr), SockFlag::SOCK_CLOEXEC);
        },
        SourceType::ForeignNotifier(fd, result, installed) => unsafe {
            *installed = true;
            sqe.prep_read(*fd, result, 0);
        },
        SourceType::ThroughputPreemption(fd, ts, events) => unsafe {
            sqe.prep_timeout(&ts.raw, *events, TimeoutFlags::empty());
            sqes.next().unwrap().prep_write(*fd, &EVENTFD_WAKEUP, 0);
        },
        SourceType::Cancel(other) => unsafe {
            if let Some(SourceStatus::Cancelling(id)) = other.borrow().status {
                sqe.prep_cancel(to_user_data(id), 0)
            }
        },
        #[cfg(feature = "bench")]
        SourceType::Noop => unsafe {
            sqe.prep_nop();
        },
        _ => unreachable!(),
    }

    match src
        .status
        .as_ref()
        .expect("source isn't registered in the source map")
    {
        SourceStatus::Dispatched(_, id) => unsafe {
            sqe.set_user_data(to_user_data(*id));
        },
        _ => unreachable!("source was cancelled"),
    }

    // set the timout, if any
    if let Some(timeout) = &src.timeout {
        unsafe {
            sqes.next().unwrap().prep_link_timeout(&timeout.raw);
        }
    }
    debug_assert!(sqes.next().is_none(), "the SQE chain provided is too long!")
}

fn transmute_error(res: io::Result<u32>) -> io::Result<usize> {
    res.map(|x| x as usize) // iou standardized on u32, which is good for low level but for higher layers usize is
        // better
        .map_err(|x| {
            // Convert CANCELED to TimedOut. This will be the case for linked `sqe`s with a
            // timeout, and if we wanted to be really strict we'd check. But if
            // the operation is truly cancelled no one will check the result,
            // and we have no other use case for cancel at the moment so keep it simple
            if let Some(libc::ECANCELED) = x.raw_os_error() {
                io::Error::from_raw_os_error(libc::ETIMEDOUT)
            } else {
                x
            }
        })
}

fn record_stats<R: Ring>(ring: &mut R, src: &mut InnerSource, res: &io::Result<usize>) {
    src.wakers.fulfilled_at = Some(Instant::now());
    if let Some(fulfilled) = src.stats_collection.and_then(|x| x.fulfilled) {
        fulfilled(res, ring.io_stats_mut(), 1);
        if let Some(handle) = src.task_queue {
            fulfilled(res, ring.io_stats_for_task_queue_mut(handle), 1);
        }
    }

    let waiters = usize::saturating_sub(src.wakers.waiters.len(), 1);
    if waiters > 0 {
        if let Some(reused) = src.stats_collection.and_then(|x| x.reused) {
            reused(res, ring.io_stats_mut(), waiters as u64);
            if let Some(handle) = src.task_queue {
                reused(
                    res,
                    ring.io_stats_for_task_queue_mut(handle),
                    waiters as u64,
                );
            }
        }
    }
}

fn process_one_event<R>(
    cqe: Option<iou::CQE>,
    post_process: R,
    source_map: &mut SourceMap,
) -> Option<bool>
where
    R: FnOnce(&'_ mut InnerSource, io::Result<usize>) -> io::Result<usize>,
{
    if let Some(value) = cqe {
        if value.user_data() == 0 {
            return Some(false);
        }

        let src = source_map.consume_source(from_user_data(value.user_data()));
        source_map.scheduler.borrow_mut().mark_completed(&src);

        let mut inner_source = src.borrow_mut();
        inner_source.wakers.result = Some(post_process(
            &mut *inner_source,
            transmute_error(value.result()),
        ));
        return Some(inner_source.wakers.wake_waiters());
    }
    None
}

mod internal {
    use crate::{
        iou,
        sys::{io_scheduler::IOSchedulerSession, source_map::SourceMap, Reactor},
    };
    use log::warn;
    use std::io;

    pub(crate) trait Uring {
        /// None if it wasn't possible to acquire an `sqe`. `Some(true)` if it
        /// was possible and there was something to dispatch.
        /// `Some(false)` if there was nothing to dispatch
        fn submit_one_source(
            &mut self,
            source_map: &mut SourceMap,
            session: &mut IOSchedulerSession<'_>,
        ) -> Option<bool>;

        fn submit_sqes(&mut self) -> io::Result<usize>;

        fn force_submit_sqes(
            &mut self,
            source_map: &mut SourceMap,
            woke: &mut usize,
        ) -> io::Result<usize> {
            let to_submit = self.waiting_kernel_submission();
            if to_submit == 0 {
                return Ok(0);
            }

            // if we fail to submit we need to make sure we collect CQEs before sleeping
            let mut consumed = false;
            let mut submitted = 0;
            loop {
                submitted += self
                    .submit_sqes()
                    .or_else(Reactor::busy_ok)
                    .or_else(Reactor::again_ok)
                    .or_else(Reactor::intr_ok)?;

                if submitted < to_submit && !consumed {
                    consumed = true;
                    self.consume_completion_queue(source_map, woke);
                } else if submitted < to_submit {
                    warn!(
                        "failed to flush the required minimum of events ({}/{}); waiting for CQEs",
                        submitted, to_submit
                    );
                    self.wait_for_events(1)
                        .expect("failed to wait for CQE. Game over");
                    self.consume_completion_queue(source_map, woke);
                } else {
                    break;
                }
            }

            Ok(submitted)
        }

        fn submit_wait_sqes(&mut self, wait: u32) -> io::Result<usize>;
        fn waiting_kernel_submission(&self) -> usize;
        fn in_kernel(&self) -> usize;
        fn waiting_kernel_collection(&self) -> usize;
        fn needs_kernel_enter(&self) -> bool;
        fn can_sleep(&self) -> bool;
        /// Return `None` if no event is completed, `Some(true)` for a task is
        /// woken up and `Some(false)` for not.
        fn consume_one_event(&mut self, source_map: &mut SourceMap) -> Option<bool>;
        fn wait_for_events(&mut self, events: u32) -> io::Result<()>;
        fn name(&self) -> &'static str;
        fn registrar(&self) -> iou::Registrar<'_>;
        fn may_rush(&self) -> bool {
            true
        }

        fn consume_completion_queue(
            &mut self,
            source_map: &mut SourceMap,
            woke: &mut usize,
        ) -> usize {
            let mut completed = 0;
            loop {
                match self.consume_one_event(source_map) {
                    None => break,
                    Some(false) => completed += 1,
                    Some(true) => {
                        completed += 1;
                        *woke += 1;
                    }
                }
            }
            completed
        }
    }
}

pub(crate) trait Ring: internal::Uring {
    fn io_stats_mut(&mut self) -> &mut RingIoStats;
    fn io_stats_for_task_queue_mut(&mut self, handle: TaskQueueHandle) -> &mut RingIoStats;
}

struct PollRing {
    ring: iou::IoUring,
    _size: usize,
    allocator: Rc<UringBufferAllocator>,
    stats: RingIoStats,
    task_queue_stats: AHashMap<TaskQueueHandle, RingIoStats>,
    in_kernel: usize,
}

impl PollRing {
    fn new(size: usize, allocator: Rc<UringBufferAllocator>) -> io::Result<Self> {
        let ring = iou::IoUring::new_with_flags(
            size as _,
            iou::SetupFlags::IOPOLL,
            iou::SetupFeatures::empty(),
        )?;
        Ok(PollRing {
            _size: size,
            ring,
            allocator,
            stats: RingIoStats::default(),
            task_queue_stats: AHashMap::new(),
            in_kernel: 0,
        })
    }

    pub(crate) fn alloc_dma_buffer(&mut self, size: usize) -> DmaBuffer {
        self.allocator.new_buffer(size).unwrap()
    }
}

impl Ring for PollRing {
    fn io_stats_mut(&mut self) -> &mut RingIoStats {
        &mut self.stats
    }

    fn io_stats_for_task_queue_mut(&mut self, handle: TaskQueueHandle) -> &mut RingIoStats {
        self.task_queue_stats.entry(handle).or_default()
    }
}

impl internal::Uring for PollRing {
    fn submit_one_source(
        &mut self,
        source_map: &mut SourceMap,
        session: &mut IOSchedulerSession<'_>,
    ) -> Option<bool> {
        let mut sq = self.ring.sq();

        let sqes = session
            .peek()
            .map(|src| sq.prepare_sqes(sqe_required(src) as u32));

        match sqes {
            Some(Some(sqes)) => {
                let src = session.pop().unwrap();
                let mut x = src.borrow_mut();
                x.wakers.submitted_at = Some(Instant::now());
                if let Some(SourceStatus::Canceled) = x.status {
                    return Some(true);
                }
                drop(x);

                // we are golden
                source_map.add_source(src.clone());
                prepare_one_source(src, sqes, |size| self.allocator.new_buffer(size));
                Some(true)
            }
            Some(None) => None,  // No SQEs available
            None => Some(false), // No source to prepare
        }
    }

    fn name(&self) -> &'static str {
        "poll"
    }

    fn registrar(&self) -> iou::Registrar<'_> {
        self.ring.registrar()
    }

    fn needs_kernel_enter(&self) -> bool {
        // We need to enter the kernel to submit and collect CQEs so if the number of
        // submitted requests doesn't match the number of request we collected, we need
        // to poll.
        self.in_kernel > 0 || self.waiting_kernel_submission() > 0
    }

    fn can_sleep(&self) -> bool {
        !self.needs_kernel_enter()
    }

    fn waiting_kernel_submission(&self) -> usize {
        self.ring.sq().ready() as usize
    }

    fn in_kernel(&self) -> usize {
        self.in_kernel
    }

    fn waiting_kernel_collection(&self) -> usize {
        self.ring.cq().ready() as usize
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        if self.needs_kernel_enter() {
            let x = self.ring.submit_sqes()? as usize;
            self.in_kernel += x;
            Ok(x)
        } else {
            Ok(0)
        }
    }

    fn submit_wait_sqes(&mut self, _: u32) -> io::Result<usize> {
        unreachable!("waiting on the poll ring is impossible")
    }

    fn consume_one_event(&mut self, source_map: &mut SourceMap) -> Option<bool> {
        process_one_event(
            self.ring.peek_for_cqe(),
            |src, res| {
                record_stats(self, src, &res);
                res
            },
            source_map,
        )
        .map(|x| {
            self.in_kernel -= 1;
            x
        })
    }

    fn wait_for_events(&mut self, events: u32) -> io::Result<()> {
        self.ring.cq().wait(events)
    }
}

struct SleepableRing {
    ring: iou::IoUring,
    _size: usize,
    name: &'static str,
    allocator: Rc<UringBufferAllocator>,
    stats: RingIoStats,
    task_queue_stats: AHashMap<TaskQueueHandle, RingIoStats>,
    in_kernel: usize,
}

impl SleepableRing {
    fn new(
        size: usize,
        name: &'static str,
        allocator: Rc<UringBufferAllocator>,
    ) -> io::Result<Self> {
        assert!(*IO_URING_RECENT_ENOUGH);
        Ok(SleepableRing {
            ring: iou::IoUring::new(size as _)?,
            _size: size,
            name,
            allocator,
            stats: RingIoStats::default(),
            task_queue_stats: AHashMap::new(),
            in_kernel: 0,
        })
    }

    fn ring_fd(&self) -> RawFd {
        self.ring.raw().ring_fd
    }
}

impl Ring for SleepableRing {
    fn io_stats_mut(&mut self) -> &mut RingIoStats {
        &mut self.stats
    }

    fn io_stats_for_task_queue_mut(&mut self, handle: TaskQueueHandle) -> &mut RingIoStats {
        self.task_queue_stats.entry(handle).or_default()
    }
}

impl internal::Uring for SleepableRing {
    fn submit_one_source(
        &mut self,
        source_map: &mut SourceMap,
        session: &mut IOSchedulerSession<'_>,
    ) -> Option<bool> {
        let mut sq = self.ring.sq();

        let sqes = session
            .peek()
            .map(|src| sq.prepare_sqes(sqe_required(src) as u32));

        match sqes {
            Some(Some(sqes)) => {
                let src = session.pop().unwrap();
                let mut x = src.borrow_mut();
                x.wakers.submitted_at = Some(Instant::now());
                if let Some(SourceStatus::Canceled) = x.status {
                    return Some(true);
                }
                drop(x);

                // we are golden
                source_map.add_source(src.clone());
                prepare_one_source(src, sqes, |size| self.allocator.new_buffer(size));
                Some(true)
            }
            Some(None) => None,  // No SQEs available
            None => Some(false), // No source to prepare
        }
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn registrar(&self) -> iou::Registrar<'_> {
        self.ring.registrar()
    }

    fn may_rush(&self) -> bool {
        false
    }

    fn needs_kernel_enter(&self) -> bool {
        // We only need to enter the kernel to submit SQEs, not to collect CQEs (the
        // kernel posts the CQEs asynchronously for us)
        self.waiting_kernel_submission() > 0
    }

    fn can_sleep(&self) -> bool {
        self.waiting_kernel_submission() == 0 && self.waiting_kernel_collection() == 0
    }

    fn waiting_kernel_submission(&self) -> usize {
        self.ring.sq().ready() as usize
    }

    fn in_kernel(&self) -> usize {
        self.in_kernel
    }

    fn waiting_kernel_collection(&self) -> usize {
        self.ring.cq().ready() as usize
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        if self.needs_kernel_enter() {
            let x = self.ring.submit_sqes()? as usize;
            self.in_kernel += x;
            Ok(x)
        } else {
            Ok(0)
        }
    }

    fn submit_wait_sqes(&mut self, wait: u32) -> io::Result<usize> {
        let x = self.ring.submit_sqes_and_wait(wait)? as usize;
        self.in_kernel += x;
        Ok(x)
    }

    fn consume_one_event(&mut self, source_map: &mut SourceMap) -> Option<bool> {
        process_one_event(
            self.ring.peek_for_cqe(),
            |src, res| {
                record_stats(self, src, &res);
                if let SourceType::ForeignNotifier(_, _, installed) = &mut src.source_type {
                    *installed = false;
                }
                res
            },
            source_map,
        )
        .map(|x| {
            self.in_kernel -= 1;
            x
        })
    }

    fn wait_for_events(&mut self, events: u32) -> io::Result<()> {
        self.ring.cq().wait(events)
    }
}

pub(crate) struct Reactor {
    scheduler: Rc<RefCell<FIFOScheduler>>,

    // FIXME: it is starting to feel we should clean this up to a Inner pattern
    main_ring: SleepableRing,
    latency_ring: SleepableRing,
    poll_ring: PollRing,

    latency_preemption_timeout_src: Cell<Option<Source>>,
    throughput_preemption_timeout_src: Cell<Option<Source>>,

    link_fd: RawFd,

    // This keeps the `eventfd` alive. Drop will close it when we're done
    notifier: Arc<sys::SleepNotifier>,

    // This is the source used to handle the notifications into the ring.
    // It is reused, unlike the timeout src, because it is possible and likely
    // that it will be in the ring through many calls to the reactor loop. It only ever gets
    // completed if this reactor is woken up from another one
    foreign_notifier_src: Source,
    source_map: SourceMap,

    blocking_thread: BlockingThreadPool,

    rings_depth: usize,
}

pub(crate) fn common_flags() -> PollFlags {
    PollFlags::POLLERR | PollFlags::POLLHUP | PollFlags::POLLNVAL
}

/// Epoll flags for all possible readability events.
pub(crate) fn read_flags() -> PollFlags {
    PollFlags::POLLIN | PollFlags::POLLPRI
}

/// Epoll flags for all possible writability events.
pub(crate) fn write_flags() -> PollFlags {
    PollFlags::POLLOUT
}

macro_rules! consume_rings {
    (into $woke:expr; $source_map:expr, $( $ring:expr ),+ ) => {{
        let mut consumed = 0;
        $(
            consumed += $ring.consume_completion_queue($source_map, $woke);
        )*
        consumed
    }}
}

macro_rules! force_flush_rings {
    (into $woke:expr; $source_map:expr, $( $ring:expr ),+ ) => {{
        let mut ret = 0;
        $(
            ret += $ring.force_submit_sqes($source_map, $woke)
                .or_else(Reactor::busy_ok)
                .or_else(Reactor::again_ok)
                .or_else(Reactor::intr_ok)?;
        )*
        io::Result::Ok(ret)
    }}
}

macro_rules! flush_rings {
    ($( $ring:expr ),+ ) => {{
        let mut ret = 0;
        $(
            ret += $ring.submit_sqes()
                .or_else(Reactor::busy_ok)
                .or_else(Reactor::again_ok)
                .or_else(Reactor::intr_ok)?;
        )*
        io::Result::Ok(ret)
    }}
}

fn align_up(v: usize, align: usize) -> usize {
    (v + align - 1) & !(align - 1)
}

impl Reactor {
    pub(crate) fn new(
        notifier: Arc<sys::SleepNotifier>,
        mut io_memory: usize,
        ring_depth: usize,
        thread_pool_placement: PoolPlacement,
    ) -> crate::Result<Reactor, ()> {
        const MIN_MEMLOCK_LIMIT: u64 = 512 * 1024;
        let (memlock_limit, _) = Resource::MEMLOCK.get()?;
        if memlock_limit < MIN_MEMLOCK_LIMIT {
            return Err(GlommioError::ReactorError(ReactorErrorKind::MemLockLimit(
                memlock_limit,
                MIN_MEMLOCK_LIMIT,
            )));
        }

        // always have at least some small amount of memory for the slab
        io_memory = std::cmp::max(align_up(io_memory, 4096), 65536);
        let allocator = Rc::new(UringBufferAllocator::new(io_memory));
        let registry = vec![allocator.as_bytes()];

        let main_ring = SleepableRing::new(ring_depth, "main", allocator.clone())?;
        let poll_ring = PollRing::new(ring_depth, allocator.clone())?;
        let latency_ring = SleepableRing::new(ring_depth, "latency", allocator.clone())?;

        match main_ring.registrar().register_buffers_by_ref(&registry) {
            Err(x) => warn!(
                "Error: registering buffers in the main ring. Skipping{:#?}",
                x
            ),
            Ok(_) => match poll_ring.registrar().register_buffers_by_ref(&registry) {
                Err(x) => {
                    warn!(
                        "Error: registering buffers in the poll ring. Skipping{:#?}",
                        x
                    );
                    main_ring.registrar().unregister_buffers().unwrap();
                }
                Ok(_) => {
                    match latency_ring.registrar().register_buffers_by_ref(&registry) {
                        Err(x) => {
                            warn!(
                                "Error: registering buffers in the poll ring. Skipping{:#?}",
                                x
                            );
                            poll_ring.registrar().unregister_buffers().unwrap();
                            main_ring.registrar().unregister_buffers().unwrap();
                        }
                        Ok(_) => {
                            allocator.activate_registered_buffers(0);
                        }
                    };
                }
            },
        }

        let link_fd = latency_ring.ring_fd();

        let foreign_notifier_src = Source::new(
            IoRequirements::default(),
            SourceType::ForeignNotifier(notifier.eventfd_fd(), 0, false),
            None,
            None,
        );

        let scheduler = Rc::new(RefCell::new(FIFOScheduler::with_capacity(ring_depth * 4)));
        let source_map = SourceMap::new(&scheduler);
        Ok(Reactor {
            scheduler,
            main_ring,
            latency_ring,
            poll_ring,
            latency_preemption_timeout_src: Cell::new(None),
            throughput_preemption_timeout_src: Cell::new(None),
            blocking_thread: BlockingThreadPool::new(thread_pool_placement, notifier.clone())?,
            link_fd,
            notifier,
            foreign_notifier_src,
            source_map,
            rings_depth: ring_depth,
        })
    }

    pub(crate) fn id(&self) -> usize {
        self.notifier.id()
    }

    pub(crate) fn ring_depth(&self) -> usize {
        self.rings_depth
    }

    /// This function prepares a timer that fires unconditionally after a
    /// certain duration. The timer is added at the front of the queue such
    /// that it will be the first SQE submitted the next time we enter the
    /// latency ring
    fn prepare_latency_preemption_timer(&self, d: Duration) -> Source {
        let source = Source::new(
            IoRequirements::default(),
            SourceType::LatencyPreemption(TimeSpec64::from(d), 0),
            None,
            None,
        );

        self.scheduler
            .borrow_mut()
            .schedule_emergency(source.inner.clone());

        source
    }

    /// This function prepares a timer that fires only after a given number of
    /// CQEs are available on the main ring. This timer is linked with a write
    /// to the latency ring's event fd such that this timer triggers a
    /// preemption via the latency ring.
    fn prepare_throughput_preemption_timer(&self) -> Source {
        let source = Source::new(
            IoRequirements::default(),
            SourceType::ThroughputPreemption(
                self.foreign_notifier_src.raw(),
                TimeSpec64::from(Duration::MAX),
                16,
            ),
            None,
            None,
        );

        self.scheduler
            .borrow_mut()
            .schedule_emergency(source.inner.clone());
        source
    }

    fn prepare_foreign_notifier(&self, foreign_notifier_src: &Source) {
        if !foreign_notifier_src.is_installed().unwrap() {
            self.scheduler
                .borrow_mut()
                .schedule_emergency(foreign_notifier_src.inner.clone());
        }
    }

    fn consume_scheduler(&mut self, woke: &mut usize) -> io::Result<usize> {
        let mut submitted = 0;
        let scheduler = self.scheduler.clone();

        loop {
            let status = {
                let mut scheduler = scheduler.borrow_mut();
                let mut session = scheduler.open_session();

                while let Some(src) = session.peek() {
                    // Escape the borrow-checker
                    // The lifetime of the source_map and the rings are the same
                    let (ring, source_map) = unsafe {
                        let ring = (self.ring_for_source(src) as *const _) as *mut dyn Ring;
                        let source_map = (&self.source_map as *const _) as *mut SourceMap;
                        (&mut *ring, &mut *source_map)
                    };

                    if !ring
                        .submit_one_source(source_map, &mut session)
                        .unwrap_or_default()
                    {
                        break;
                    }
                }
                session.seal()
            };

            match status {
                IOSchedulerSessionState::NeedsDrain => {
                    // We need to make sure the previous sources are dispatched immediately
                    submitted += force_flush_rings!(
                        into woke;
                        &mut self.source_map,
                        self.main_ring,
                        self.poll_ring,
                        self.latency_ring
                    )?;
                }
                _ => {
                    submitted += flush_rings!(self.main_ring, self.poll_ring, self.latency_ring)?;
                    break;
                }
            }
        }
        Ok(submitted)
    }

    pub(crate) fn process_foreign_wakes(&self) -> usize {
        self.notifier.process_foreign_wakes()
    }

    pub(crate) fn alloc_dma_buffer(&mut self, size: usize) -> DmaBuffer {
        self.poll_ring.alloc_dma_buffer(size)
    }

    pub(crate) fn write_dma(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn write_buffered(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn read_dma(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn read_buffered(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn poll_ready(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn send(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn sendmsg(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn recv(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn recvmsg(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn connect(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn accept(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn fdatasync(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn fallocate(&self, source: &Source) {
        self.schedule_source(source);
    }

    fn enqueue_blocking_request(&self, source: &Source, op: BlockingThreadOp) {
        self.blocking_thread
            .push(op, source.inner.clone())
            .expect("failed to spawn blocking request");
    }

    pub(crate) fn truncate(&self, source: &Source, size: u64) {
        let op = BlockingThreadOp::Truncate(source.raw(), size as _);
        self.enqueue_blocking_request(source, op);
    }

    pub(crate) fn rename(&self, source: &Source) {
        let (old_path, new_path) = match &*source.source_type() {
            SourceType::Rename(o, n) => (o.clone(), n.clone()),
            _ => panic!("Unexpected source for rename operation"),
        };

        let op = BlockingThreadOp::Rename(old_path, new_path);
        self.enqueue_blocking_request(source, op);
    }

    pub(crate) fn create_dir(&self, source: &Source) {
        let (path, mode) = match &*source.source_type() {
            SourceType::CreateDir(p, m) => (p.clone(), *m),
            _ => panic!("Unexpected source for rename operation"),
        };

        let op = BlockingThreadOp::CreateDir(path, mode.bits() as c_int);
        self.enqueue_blocking_request(source, op);
    }

    pub(crate) fn remove_file(&self, source: &Source) {
        let path = match &*source.source_type() {
            SourceType::Remove(path) => path.clone(),
            _ => panic!("Unexpected source for remove operation"),
        };

        let op = BlockingThreadOp::Remove(path);
        self.enqueue_blocking_request(source, op);
    }

    pub(crate) fn run_blocking(&self, source: &Source, f: Box<dyn FnOnce() + Send + 'static>) {
        assert!(matches!(&*source.source_type(), SourceType::BlockingFn));

        let op = BlockingThreadOp::Fn(f);
        self.enqueue_blocking_request(source, op);
    }

    pub(crate) fn close(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn statx(&self, source: &Source) {
        self.schedule_source(source);
    }

    pub(crate) fn open_at(&self, source: &Source) {
        self.schedule_source(source);
    }

    #[cfg(feature = "bench")]
    pub(crate) fn nop(&self, source: &Source) {
        self.schedule_source(source);
    }

    /// io_uring can return `EBUSY` when submitting more requests would
    /// over-commit the system. This is fine: we just need to make sure that we
    /// don't sleep and that we don't failed rushed polls. So we just ignore
    /// this error
    fn busy_ok<T: Default>(x: std::io::Error) -> io::Result<T> {
        match x.raw_os_error() {
            Some(libc::EBUSY) => Ok(Default::default()),
            Some(_) => Err(x),
            None => Err(x),
        }
    }

    /// io_uring can return `EAGAIN` when the CQE queue is full, and we try to
    /// push more requests. This is fine: we just need to make sure that we
    /// don't sleep and that we don't failed rushed polls. So we just ignore
    /// this error
    fn again_ok<T: Default>(x: std::io::Error) -> io::Result<T> {
        match x.raw_os_error() {
            Some(libc::EAGAIN) => Ok(Default::default()),
            Some(_) => Err(x),
            None => Err(x),
        }
    }

    /// io_uring can return `EINTR` if the syscall was interrupted by a signal
    /// delivery. This is fine: we just need to make sure that we
    /// don't sleep and that we don't failed rushed polls. So we just ignore
    /// this error
    fn intr_ok<T: Default>(x: std::io::Error) -> io::Result<T> {
        match x.raw_os_error() {
            Some(libc::EINTR) => Ok(Default::default()),
            Some(_) => Err(x),
            None => Err(x),
        }
    }

    /// We want to go to sleep, but we can only go to sleep in one of the rings,
    /// as we only have one thread. There are more than one sleepable rings, so
    /// what we do is we take advantage of the fact that the ring's `ring_fd` is
    /// pollable and register a `POLL_ADD` event into the ring we will wait on.
    ///
    /// We may not be able to register an `SQE` at this point, so we return an
    /// Error and will just not sleep.
    fn link_rings_and_sleep(&mut self) -> io::Result<()> {
        assert_eq!(
            self.main_ring.waiting_kernel_submission(),
            0,
            "sleeping with pending SQEs"
        );

        if let Some(sqes) = self.main_ring.ring.sq().prepare_sqes(1) {
            let src = InnerSource::pin(
                IoRequirements::default(),
                SourceType::PollAdd(self.link_fd, common_flags() | read_flags()),
                None,
                None,
            );
            self.source_map.add_source(src.clone());
            prepare_one_source(src, sqes, |_| unreachable!());
            self.main_ring
                .submit_wait_sqes(1)
                .or_else(Reactor::busy_ok)
                .or_else(Reactor::again_ok)
                .or_else(Reactor::intr_ok)?;
            Ok(())
        } else {
            Ok(()) // can't sleep but that's fine
        }
    }

    pub(crate) fn poll_io(&mut self, woke: &mut usize) -> io::Result<()> {
        consume_rings!(into woke; &mut self.source_map, self.latency_ring, self.poll_ring, self.main_ring);
        self.consume_scheduler(woke)?;
        *woke += self.flush_syscall_thread();
        *woke += self.process_foreign_wakes();
        Ok(())
    }

    pub(crate) fn rush_dispatch(&self, _: &Source) -> io::Result<()> {
        Ok(())
    }

    /// This function can be passed two timers. Because they play different
    /// roles we keep them separate instead of overloading the same
    /// parameter.
    ///
    /// * The first is the preempt timer. It is designed to take the current
    ///   task queue out of the cpu. If nothing else fires in the latency ring
    ///   the preempt timer will, making need_preempt return true. Currently, we
    ///   always install a preempt timer in the upper layers but from the point
    ///   of view of the io_uring implementation it is optional: it is perfectly
    ///   valid not to have one. Preempt timers are installed by Glommio
    ///   executor runtime.
    ///
    /// * The second is the user timer. It is installed per a user request when
    ///   the user creates a `Timer` (or `TimerAction`).
    ///
    /// At some level, those are both just timers and can be coalesced. And they
    /// certainly are: if there is a user timer that needs to fire in 1ms, and
    /// we want the preempt_timer to also fire around 1ms, there is no need
    /// to register two timers. At the end of the day, all that matters is
    /// that the latency ring flares and that we leave the CPU. That is
    /// because unlike I/O, we don't have one Source per timer, and
    /// parking.rs just keeps them on a wheel and just tell us about what is
    /// the next expiration.
    ///
    /// However, they are also different. The main source of difference is sleep
    /// and wake behavior:
    ///
    /// * When there is no more work to do, and we go to sleep, we do not want
    ///   to register the preempt timer: it is designed to fire periodically to
    ///   take us out of the CPU and if there is no task queue running, we don't
    ///   want to wake up and spend power just for that. However, if there is a
    ///   user timer that needs to fire in the future we must register it.
    ///   Otherwise, we will sleep and never wake up.
    ///
    /// * The user timer point of expiration never changes. So once we register
    ///   it we don't need to rearm it until it fires. But the preempt timer has
    ///   to be rearmed every time. Moreover, it needs to give every task queue
    ///   a fair shot at running. So it needs to be rearmed as close as possible
    ///   to the point where we *leave* this method. For instance: if we spin
    ///   here for 3ms and the preempt timer is 10ms that would leave the next
    ///   task queue just 7ms to run.
    pub(crate) fn wait<Preempt, F>(
        &mut self,
        preempt_timer: Preempt,
        user_timer: Option<Duration>,
        mut woke: usize,
        process_remote_channels: F,
    ) -> io::Result<bool>
    where
        Preempt: Fn() -> Option<Duration>,
        F: Fn() -> usize,
    {
        woke += self.flush_syscall_thread();

        // consume all events from the rings
        consume_rings!(into &mut woke; &mut self.source_map, self.latency_ring, self.poll_ring, self.main_ring);

        // Cancel the old timer regardless of whether we can sleep:
        // if we won't sleep, we will register the new timer with its new
        // value.
        //
        // But if we will sleep, there might be a timer registered that needs
        // to be removed otherwise we'll wake up when it expires.
        drop(self.latency_preemption_timeout_src.take());

        // Schedule the throughput-based timeout immediately: it won't matter if we end
        // up sleeping.
        self.throughput_preemption_timeout_src
            .replace(Some(self.prepare_throughput_preemption_timer()));

        // Prepare the foreign notifier
        self.prepare_foreign_notifier(&self.foreign_notifier_src);

        self.consume_scheduler(&mut woke)?;
        consume_rings!(into &mut woke; &mut self.source_map, self.latency_ring, self.poll_ring, self.main_ring);

        // If we generated any event so far, we can't sleep. Need to handle them.
        let should_sleep = preempt_timer().is_none()
            && (woke == 0)
            && self.scheduler.borrow().is_empty()
            && self.poll_ring.can_sleep()
            && self.main_ring.can_sleep()
            && self.latency_ring.can_sleep();

        if should_sleep {
            self.latency_preemption_timeout_src
                .set(Some(self.prepare_latency_preemption_timer(
                    user_timer.unwrap_or_else(|| Duration::from_micros(500)),
                )));
            assert!(self.consume_scheduler(&mut 0)? > 0);

            // From this moment on the remote executors are aware that we are sleeping
            // We have to sweep the remote channels function once more because since
            // last time until now it could be that something happened in a remote executor
            // that opened up room. If if did we bail on sleep and go process it.
            self.notifier.prepare_to_sleep();
            // See https://www.scylladb.com/2018/02/15/memory-barriers-seastar-linux/ for
            // details. This translates to `sys_membarrier()` /
            // `MEMBARRIER_CMD_PRIVATE_EXPEDITED`
            membarrier::heavy();
            let events = process_remote_channels()
                + self.process_foreign_wakes()
                + self.flush_syscall_thread();
            if events == 0 {
                if self.foreign_notifier_src.is_installed().unwrap() {
                    self.link_rings_and_sleep().expect("some error");
                    // May have new cancellations related to the link ring fd.
                    consume_rings!(into &mut 0; &mut self.source_map, self.latency_ring, self.poll_ring, self.main_ring);
                }
                // Woke up, so no need to notify us anymore.
                self.notifier.wake_up();
            }
        }

        if let Some(preempt) = preempt_timer() {
            self.prepare_foreign_notifier(&self.foreign_notifier_src);
            self.latency_preemption_timeout_src
                .set(Some(self.prepare_latency_preemption_timer(preempt)));
            assert!(self.consume_scheduler(&mut woke)? > 0);
        }

        // A Note about `need_preempt`:
        //
        // If in the last call to consume_rings! some events completed, the tail and
        // head would have moved to match. So it does not matter that events were
        // generated after we registered the timer: since we consumed them here,
        // need_preempt() should be false at this point. As soon as the next event
        // in the preempt ring completes, though, then it will be true.
        Ok(should_sleep)
    }

    pub(crate) fn flush_syscall_thread(&self) -> usize {
        self.blocking_thread.flush()
    }

    pub(crate) fn preempt_pointers(&self) -> (*const u32, *const u32) {
        let cq = &self.latency_ring.ring.raw().cq;
        (cq.khead, cq.ktail)
    }

    /// RAII-truncate asynchronously files that required it, e.g. because of
    /// padded writes, but were not closed explicitly.
    pub(crate) fn async_truncate(&self, fd: RawFd, size: u64) {
        // actually synchronous for now!
        let _ = sys::truncate_file(fd, size);
    }

    /// RAII-close asynchronously files that were not closed explicitly.
    pub(crate) fn async_close(&self, fd: RawFd) {
        self.scheduler.borrow_mut().schedule(InnerSource::pin(
            IoRequirements::default(),
            SourceType::Close(fd),
            None,
            None,
        ));
    }

    pub(crate) fn ring_for_source(&mut self, source: &PinnedInnerSource) -> &mut dyn Ring {
        // Dispatch requests according to the following rules:
        // * Disk reads/writes go to the poll ring if possible, or the main ring
        //   otherwise;
        // * Network Rx and connect/accept go the latency ring;
        // * Every other request are dispatched to the main ring;
        // We avoid putting requests that come in high numbers on the latency ring
        // because the more request we issue there, the less effective it becomes.

        match &source.borrow().source_type {
            SourceType::Cancel(other) => self.ring_for_source(other),
            SourceType::Read(_, _, _, p, _) | SourceType::Write(_, _, p, _) => match p {
                PollableStatus::Pollable => &mut self.poll_ring,
                PollableStatus::NonPollable(_) => &mut self.main_ring,
            },
            SourceType::LatencyPreemption(_, _)
            | SourceType::ForeignNotifier(_, _, _)
            | SourceType::SockRecv(_, _, _, _)
            | SourceType::SockRecvMsg(_, _, _, _, _, _, _)
            | SourceType::Accept(_, _)
            | SourceType::Connect(_, _) => &mut self.latency_ring,
            SourceType::Invalid => {
                unreachable!("called ring_for_source on invalid source")
            }
            _ => &mut self.main_ring,
        }
    }

    pub fn io_stats(&mut self) -> IoStats {
        IoStats::new(
            std::mem::take(&mut self.main_ring.stats),
            std::mem::take(&mut self.latency_ring.stats),
            std::mem::take(&mut self.poll_ring.stats),
        )
    }

    pub(crate) fn task_queue_io_stats(&mut self, h: &TaskQueueHandle) -> Option<IoStats> {
        let main = self
            .main_ring
            .task_queue_stats
            .get_mut(h)
            .map(std::mem::take);
        let lat = self
            .latency_ring
            .task_queue_stats
            .get_mut(h)
            .map(std::mem::take);
        let poll = self
            .poll_ring
            .task_queue_stats
            .get_mut(h)
            .map(std::mem::take);

        if let (None, None, None) = (&main, &lat, &poll) {
            None
        } else {
            Some(IoStats::new(
                main.unwrap_or_default(),
                lat.unwrap_or_default(),
                poll.unwrap_or_default(),
            ))
        }
    }

    fn schedule_source(&self, source: &Source) {
        source.inner.borrow_mut().wakers.queued_at = Some(Instant::now());
        let mut queue = self.scheduler.borrow_mut();
        queue.schedule(source.inner.clone());
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[test]
    fn timeout_smoke_test() {
        let notifier = sys::new_sleep_notifier().unwrap();
        let mut reactor = Reactor::new(notifier, 0, 128, PoolPlacement::Unbound(1)).unwrap();

        fn timeout_source(millis: u64) -> Source {
            Source::new(
                IoRequirements::default(),
                SourceType::Timeout(TimeSpec64::from(Duration::from_millis(millis)), 0),
                None,
                None,
            )
        }

        let fast = timeout_source(10);
        reactor.schedule_source(&fast);

        let slow = timeout_source(25);
        reactor.schedule_source(&slow);

        let lethargic = timeout_source(50);
        reactor.schedule_source(&lethargic);

        let start = Instant::now();
        reactor.wait(|| None, None, 0, || 0).unwrap();
        let elapsed_ms = start.elapsed().as_millis();
        assert!((10..50).contains(&elapsed_ms));

        drop(slow); // Cancel this one.

        reactor.wait(|| None, None, 0, || 0).unwrap();
        let elapsed_ms = start.elapsed().as_millis();
        assert!((50..100).contains(&elapsed_ms), "{}", elapsed_ms);
    }
}
