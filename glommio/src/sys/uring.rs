// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use log::warn;
use nix::{
    fcntl::{FallocateFlags, OFlag},
    poll::PollFlags,
};
use rlimit::Resource;
use std::{
    cell::{Cell, RefCell, RefMut},
    collections::VecDeque,
    ffi::CStr,
    io,
    ops::Range,
    os::unix::io::RawFd,
    panic,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    iou,
    iou::{
        sqe::{FsyncFlags, SockAddrStorage, StatxFlags, StatxMode, SubmissionFlags, TimeoutFlags},
        SQEs,
    },
    sys::{
        self,
        blocking::{BlockingThreadOp, BlockingThreadPool},
        buffer::UringBufferAllocator,
        dma_buffer::DmaBuffer,
        source::PinnedInnerSource,
        source_map::{from_user_data, to_user_data, SourceId, SourceMap},
        DirectIo,
        EnqueuedStatus,
        InnerSource,
        IoBuffer,
        PollableStatus,
        Source,
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
use nix::sys::{
    socket::{MsgFlags, SockAddr, SockFlag},
    stat::Mode as OpenMode,
};
use smallvec::SmallVec;

const MSG_ZEROCOPY: i32 = 0x4000000;
const EVENTFD_WAKEUP: u64 = 1u64;

#[allow(dead_code)]
#[derive(Debug)]
enum UringOpDescriptor {
    PollAdd(PollFlags),
    PollRemove(*const u8),
    Cancel(u64),
    Write(*const u8, usize, u64),
    WriteFixed(*const u8, usize, u64, u32),
    ReadFixed(u64, usize),
    Read(u64, usize),
    Open(*const u8, libc::c_int, u32),
    Close,
    FDataSync,
    Connect(*const SockAddr),
    LinkTimeout(*const uring_sys::__kernel_timespec),
    Accept(*mut SockAddrStorage),
    Fallocate(u64, u64, libc::c_int),
    Statx(*const u8, *mut libc::statx),
    Timeout(*const uring_sys::__kernel_timespec, u32),
    TimeoutRemove(u64),
    SockSend(*const u8, usize, i32),
    SockSendMsg(*mut libc::msghdr, i32),
    SockRecv(usize, i32),
    SockRecvMsg(usize, i32),
    Nop,
}

#[derive(Debug)]
pub(crate) struct UringDescriptor {
    fd: RawFd,
    flags: SubmissionFlags,
    user_data: u64,
    args: UringOpDescriptor,
}

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

fn fill_sqe<F>(
    sqe: &mut iou::SQE<'_>,
    op: &UringDescriptor,
    buffer_allocation: F,
    source_map: &mut SourceMap,
) where
    F: FnOnce(usize) -> Option<DmaBuffer>,
{
    let mut user_data = op.user_data;
    unsafe {
        match op.args {
            UringOpDescriptor::PollAdd(events) => {
                sqe.prep_poll_add(op.fd, events);
            }
            UringOpDescriptor::PollRemove(to_remove) => {
                user_data = 0;
                sqe.prep_poll_remove(to_remove as u64);
            }
            UringOpDescriptor::Cancel(to_remove) => {
                user_data = 0;
                sqe.prep_cancel(to_remove, 0);
            }
            UringOpDescriptor::Write(ptr, len, pos) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                sqe.prep_write(op.fd, buf, pos);
            }
            UringOpDescriptor::Read(pos, len) => {
                source_map.peek_source_mut(from_user_data(op.user_data), |mut x| {
                    match &mut x.source_type {
                        SourceType::ForeignNotifier(_, result, _) => {
                            sqe.prep_read(op.fd, result, pos);
                        }
                        SourceType::Read(
                            _,
                            _,
                            _,
                            PollableStatus::NonPollable(DirectIo::Disabled),
                            slot,
                        ) => {
                            let mut buf = buffer_allocation(len).expect("Buffer allocation failed");
                            sqe.prep_read(op.fd, buf.as_bytes_mut(), pos);
                            // If you have a buffer here, that very likely means you are reusing the
                            // source. The kernel knows about that buffer already, and will write to
                            // it. So this can only be called if there is no buffer attached to it.
                            assert!(slot.is_none());
                            *slot = Some(IoBuffer::Dma(buf));
                        }
                        _ => unreachable!("Expected Read source type"),
                    }
                });
            }
            UringOpDescriptor::Open(path, flags, mode) => {
                let path = CStr::from_ptr(path as _);
                sqe.prep_openat(
                    op.fd,
                    path,
                    OFlag::from_bits_truncate(flags),
                    OpenMode::from_bits_truncate(mode),
                );
            }
            UringOpDescriptor::FDataSync => {
                sqe.prep_fsync(op.fd, FsyncFlags::FSYNC_DATASYNC);
            }
            UringOpDescriptor::Connect(addr) => {
                sqe.prep_connect(op.fd, &*addr);
            }

            UringOpDescriptor::LinkTimeout(timespec) => {
                sqe.prep_link_timeout(&*timespec);
            }

            UringOpDescriptor::Accept(addr) => {
                sqe.prep_accept(op.fd, Some(&mut *addr), SockFlag::SOCK_CLOEXEC);
            }

            UringOpDescriptor::Fallocate(offset, size, flags) => {
                let flags = FallocateFlags::from_bits_truncate(flags);
                sqe.prep_fallocate(op.fd, offset, size, flags);
            }
            UringOpDescriptor::Statx(path, statx_buf) => {
                let flags = StatxFlags::AT_STATX_SYNC_AS_STAT | StatxFlags::AT_NO_AUTOMOUNT;
                let mode = StatxMode::from_bits_truncate(0x7ff);

                let path = CStr::from_ptr(path as _);
                sqe.prep_statx(-1, path, flags, mode, &mut *statx_buf);
            }
            UringOpDescriptor::Timeout(timespec, events) => {
                sqe.prep_timeout(&*timespec, events, TimeoutFlags::empty());
            }
            UringOpDescriptor::TimeoutRemove(timer) => {
                sqe.prep_timeout_remove(timer as _);
            }
            UringOpDescriptor::Close => {
                sqe.prep_close(op.fd);
            }
            UringOpDescriptor::ReadFixed(pos, len) => {
                let mut buf = buffer_allocation(len).expect("Buffer allocation failed");
                source_map.peek_source_mut(from_user_data(op.user_data), |mut src| {
                    match &mut src.source_type {
                        SourceType::Read(
                            _,
                            _,
                            _,
                            PollableStatus::NonPollable(DirectIo::Disabled),
                            slot,
                        ) => {
                            sqe.prep_read(op.fd, buf.as_bytes_mut(), pos);
                            *slot = Some(IoBuffer::Dma(buf));
                        }
                        SourceType::Read(_, _, _, _, slot) => {
                            match buf.uring_buffer_id() {
                                None => {
                                    sqe.prep_read(op.fd, buf.as_bytes_mut(), pos);
                                }
                                Some(idx) => {
                                    sqe.prep_read_fixed(op.fd, buf.as_bytes_mut(), pos, idx);
                                }
                            };
                            *slot = Some(IoBuffer::Dma(buf));
                        }
                        _ => unreachable!(),
                    };
                });
            }

            UringOpDescriptor::WriteFixed(ptr, len, pos, buf_index) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                sqe.prep_write_fixed(op.fd, buf, pos, buf_index as _);
            }

            UringOpDescriptor::SockSend(ptr, len, flags) => {
                let buf = std::slice::from_raw_parts(ptr, len);
                sqe.prep_send(
                    op.fd,
                    buf,
                    MsgFlags::from_bits_unchecked(flags | MSG_ZEROCOPY),
                );
            }

            UringOpDescriptor::SockSendMsg(hdr, flags) => {
                sqe.prep_sendmsg(
                    op.fd,
                    hdr,
                    MsgFlags::from_bits_unchecked(flags | MSG_ZEROCOPY),
                );
            }

            UringOpDescriptor::SockRecv(len, flags) => {
                let mut buf = DmaBuffer::new(len).expect("failed to allocate buffer");
                sqe.prep_recv(
                    op.fd,
                    buf.as_bytes_mut(),
                    MsgFlags::from_bits_unchecked(flags),
                );

                source_map.peek_source_mut(from_user_data(op.user_data), |mut src| {
                    match &mut src.source_type {
                        SourceType::SockRecv(_, _, slot, _) => {
                            *slot = Some(buf);
                        }
                        _ => unreachable!(),
                    };
                });
            }

            UringOpDescriptor::SockRecvMsg(len, flags) => {
                let mut buf = DmaBuffer::new(len).expect("failed to allocate buffer");
                source_map.peek_source_mut(from_user_data(op.user_data), |mut src| {
                    match &mut src.source_type {
                        SourceType::SockRecvMsg(_, _, slot, _, iov, hdr, msg_name) => {
                            iov.iov_base = buf.as_mut_ptr() as *mut libc::c_void;
                            iov.iov_len = len;

                            let msg_namelen =
                                std::mem::size_of::<nix::sys::socket::sockaddr_storage>()
                                    as libc::socklen_t;
                            hdr.msg_name = msg_name.as_mut_ptr() as *mut libc::c_void;
                            hdr.msg_namelen = msg_namelen;
                            hdr.msg_iov = iov as *mut libc::iovec;
                            hdr.msg_iovlen = 1;

                            sqe.prep_recvmsg(
                                op.fd,
                                hdr as *mut libc::msghdr,
                                MsgFlags::from_bits_unchecked(flags),
                            );
                            *slot = Some(buf);
                        }
                        _ => unreachable!(),
                    };
                });
            }
            UringOpDescriptor::Nop => sqe.prep_nop(),
        }
        sqe.set_user_data(user_data);
        sqe.set_flags(op.flags);
    }
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
        SourceType::Timeout(ts, events) => unsafe {
            sqe.prep_timeout(&ts.raw, *events, TimeoutFlags::empty());
        },
        SourceType::Connect(fd, addr) => unsafe {
            sqe.prep_connect(*fd, addr);
        },
        SourceType::Accept(fd, addr) => unsafe {
            sqe.prep_accept(*fd, Some(&mut *addr), SockFlag::SOCK_CLOEXEC);
        },
        SourceType::ForeignNotifier(fd, result, _) => unsafe {
            sqe.prep_read(*fd, result, 0);
        },
        SourceType::ThroughputPreemption(fd, ts, events) => unsafe {
            sqe.prep_timeout(&ts.raw, *events, TimeoutFlags::empty());
            sqes.next().unwrap().prep_write(*fd, &EVENTFD_WAKEUP, 0);
        },
        #[cfg(feature = "bench")]
        SourceType::Noop => unsafe {
            sqe.prep_nop();
        },
        _ => unreachable!(),
    }

    if let Some(enqueued) = src.enqueued.as_ref() {
        unsafe {
            sqe.set_user_data(to_user_data(enqueued.id));
        }
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

fn record_stats<Ring: UringCommon>(
    ring: &mut Ring,
    src: &mut InnerSource,
    res: &io::Result<usize>,
) {
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

// Find the next complete chain of events from the queue
// Returns None if the queue is empty.
fn peek_one_chain(queue: &VecDeque<UringDescriptor>, ring_size: usize) -> Option<Range<usize>> {
    if queue.is_empty() {
        return None;
    }
    let chain = queue
        .iter()
        .take(ring_size)
        .position(|sqe| {
            !sqe.flags
                .intersects(SubmissionFlags::IO_LINK | SubmissionFlags::IO_HARDLINK)
        })
        .expect("Unterminated SQE link chain or submission queue overflow");
    Some(0..chain + 1)
}

// Extract a chain of events from the queue.
// The chain be empty if the sources were cancelled
fn extract_one_chain(
    source_map: &mut SourceMap,
    queue: &mut VecDeque<UringDescriptor>,
    chain: Range<usize>,
    now: Instant,
) -> SmallVec<[UringDescriptor; 1]> {
    queue
        .drain(chain)
        .filter(move |op| {
            if op.user_data > 0 {
                let id = from_user_data(op.user_data);
                let status = source_map.peek_source_mut(from_user_data(op.user_data), |mut x| {
                    x.wakers.submitted_at = Some(now);
                    let current = x.enqueued.as_mut().expect("bug");
                    match current.status {
                        EnqueuedStatus::Enqueued => {
                            current.status = EnqueuedStatus::Dispatched;
                            EnqueuedStatus::Dispatched
                        }
                        EnqueuedStatus::Canceled => EnqueuedStatus::Canceled,
                        _ => unreachable!(),
                    }
                });
                if status == EnqueuedStatus::Canceled {
                    source_map.consume_source(id);
                    return false;
                }
            }
            true
        })
        .collect()
}

fn process_one_event<R>(
    cqe: Option<iou::CQE>,
    post_process: R,
    source_map: Rc<RefCell<SourceMap>>,
) -> Option<bool>
where
    R: FnOnce(&'_ mut InnerSource, io::Result<usize>) -> io::Result<usize>,
{
    if let Some(value) = cqe {
        // No user data is `POLL_REMOVE` or `CANCEL`, we won't process.
        if value.user_data() == 0 {
            return Some(false);
        }

        let src = source_map
            .borrow_mut()
            .consume_source(from_user_data(value.user_data()));

        let mut inner_source = src.borrow_mut();
        inner_source.wakers.result = Some(post_process(
            &mut *inner_source,
            transmute_error(value.result()),
        ));
        return Some(inner_source.wakers.wake_waiters());
    }
    None
}

#[derive(Debug)]
pub(crate) struct UringQueueState {
    submissions: VecDeque<PinnedInnerSource>,
    cancellations: VecDeque<UringDescriptor>,
}

pub(crate) type ReactorQueue = Rc<RefCell<UringQueueState>>;

impl UringQueueState {
    fn with_capacity(cap: usize) -> ReactorQueue {
        Rc::new(RefCell::new(UringQueueState {
            submissions: VecDeque::with_capacity(cap),
            cancellations: VecDeque::new(),
        }))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.submissions.is_empty() && self.cancellations.is_empty()
    }

    pub(crate) fn cancel_request(&mut self, id: SourceId) {
        self.cancellations.push_back(UringDescriptor {
            args: UringOpDescriptor::Cancel(to_user_data(id)),
            fd: -1,
            flags: SubmissionFlags::empty(),
            user_data: 0,
        });
    }
}

pub(crate) trait UringCommon {
    fn submission_queue(&mut self) -> ReactorQueue;
    fn submit_sqes(&mut self) -> io::Result<usize>;
    fn submit_wait_sqes(&mut self, wait: u32) -> io::Result<usize>;
    fn waiting_kernel_submission(&self) -> usize;
    fn in_kernel(&self) -> usize;
    fn waiting_kernel_collection(&self) -> usize;
    fn needs_kernel_enter(&self) -> bool;
    fn can_sleep(&self) -> bool;
    /// None if it wasn't possible to acquire an `sqe`. `Some(true)` if it was
    /// possible and there was something to dispatch. `Some(false)` if there
    /// was nothing to dispatch
    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<bool>;
    fn submit_one_event2(&mut self, queue: &mut VecDeque<PinnedInnerSource>) -> Option<bool>;
    /// Return `None` if no event is completed, `Some(true)` for a task is woken
    /// up and `Some(false)` for not.
    fn consume_one_event(&mut self) -> Option<bool>;
    fn wait_for_events(&mut self, events: u32) -> io::Result<()>;
    fn name(&self) -> &'static str;
    fn io_stats_mut(&mut self) -> &mut RingIoStats;
    fn io_stats_for_task_queue_mut(&mut self, handle: TaskQueueHandle) -> &mut RingIoStats;
    fn registrar(&self) -> iou::Registrar<'_>;
    fn may_rush(&self) -> bool {
        true
    }

    fn consume_sqe_queue(
        &mut self,
        queue: &mut VecDeque<UringDescriptor>,
        mut dispatch: bool,
    ) -> io::Result<usize> {
        loop {
            match self.submit_one_event(queue) {
                None => {
                    dispatch = true;
                    break;
                }
                Some(true) => {}
                Some(false) => break,
            }
        }

        if dispatch && self.needs_kernel_enter() {
            self.submit_sqes()
        } else {
            Ok(0)
        }
    }

    fn consume_sqe_queue2(
        &mut self,
        queue: &mut VecDeque<PinnedInnerSource>,
        mut dispatch: bool,
    ) -> io::Result<usize> {
        loop {
            match self.submit_one_event2(queue) {
                None => {
                    dispatch = true;
                    break;
                }
                Some(true) => {}
                Some(false) => break,
            }
        }

        if dispatch && self.needs_kernel_enter() {
            self.submit_sqes()
        } else {
            Ok(0)
        }
    }

    /// We will not dispatch the cancellation queue unless we need to.
    /// Dispatches will come from the submission queue.
    fn consume_cancellation_queue(&mut self) -> io::Result<usize> {
        let q = self.submission_queue();
        let mut queue = q.borrow_mut();
        self.consume_sqe_queue(&mut queue.cancellations, false)
    }

    fn consume_submission_queue(&mut self) -> io::Result<usize> {
        let q = self.submission_queue();
        let mut queue = q.borrow_mut();
        self.consume_sqe_queue2(&mut queue.submissions, true)
    }

    fn consume_completion_queue(&mut self, woke: &mut usize) -> usize {
        let mut completed = 0;
        loop {
            match self.consume_one_event() {
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

    /// Used when we absolutely need to flush at least min_requests requests
    /// from queue. This function blocks if it has to.
    ///
    /// min_requests does not account for SQEs waiting to be sent to the kernel.
    /// If we have 2 items in the queue and 8 SQEs pending submission then
    /// min_requests = 1 will submit all 8 SQEs + the first item in the queue.
    ///
    /// The function returns the number of elements submitted. This can be lower
    /// than min_requests if there were fewer requests in the queue.
    fn force_flush_ring(
        &mut self,
        queue: &mut VecDeque<UringDescriptor>,
        mut min_requests: usize,
        woke: &mut usize,
    ) -> usize {
        // we can't submit more request than what's there to submit + pending SQEs
        min_requests = min_requests.min(queue.len()) + self.waiting_kernel_submission();

        // if we fail to submit we need to make sure we collect CQEs before sleeping
        let mut consumed = false;
        let mut submitted = 0;
        loop {
            match self.consume_sqe_queue(queue, true) {
                Err(err) => {
                    if !consumed {
                        consumed = true;
                    } else {
                        warn!(
                            "failed to flush the required minimum of events ({}/{}): {}; waiting \
                             for {} CQEs",
                            submitted,
                            min_requests,
                            err,
                            min_requests - submitted
                        );
                        self.wait_for_events((min_requests - submitted) as u32)
                            .expect("failed to wait for CQE. Game over");
                        consumed = false;
                    }
                    self.consume_completion_queue(woke);
                }
                Ok(x) => submitted += x,
            }

            if submitted >= min_requests {
                break;
            }
        }

        submitted
    }

    /// Used when we absolutely need to flush at least min_requests requests
    /// from queue. This function blocks if it has to.
    ///
    /// min_requests does not account for SQEs waiting to be sent to the kernel.
    /// If we have 2 items in the queue and 8 SQEs pending submission then
    /// min_requests = 1 will submit all 8 SQEs + the first item in the queue.
    ///
    /// The function returns the number of elements submitted. This can be lower
    /// than min_requests if there were fewer requests in the queue.
    fn force_flush_ring2(
        &mut self,
        queue: &mut VecDeque<PinnedInnerSource>,
        mut min_requests: usize,
        woke: &mut usize,
    ) -> usize {
        // we can't submit more request than what's there to submit + pending SQEs
        min_requests = min_requests.min(queue.len()) + self.waiting_kernel_submission();

        // if we fail to submit we need to make sure we collect CQEs before sleeping
        let mut consumed = false;
        let mut submitted = 0;
        loop {
            match self.consume_sqe_queue2(queue, true) {
                Err(err) => {
                    if !consumed {
                        consumed = true;
                    } else {
                        warn!(
                            "failed to flush the required minimum of events ({}/{}): {}; waiting \
                             for {} CQEs",
                            submitted,
                            min_requests,
                            err,
                            min_requests - submitted
                        );
                        self.wait_for_events((min_requests - submitted) as u32)
                            .expect("failed to wait for CQE. Game over");
                        consumed = false;
                    }
                    self.consume_completion_queue(woke);
                }
                Ok(x) => submitted += x,
            }

            if submitted >= min_requests {
                break;
            }
        }

        submitted
    }

    fn poll(&mut self, woke: &mut usize) -> io::Result<()> {
        self.consume_cancellation_queue()
            .or_else(Reactor::busy_ok)
            .or_else(Reactor::again_ok)
            .or_else(Reactor::intr_ok)?;
        self.consume_submission_queue()
            .or_else(Reactor::busy_ok)
            .or_else(Reactor::again_ok)
            .or_else(Reactor::intr_ok)?;
        self.consume_completion_queue(woke);
        Ok(())
    }
}

struct PollRing {
    ring: iou::IoUring,
    size: usize,
    submission_queue: ReactorQueue,
    allocator: Rc<UringBufferAllocator>,
    stats: RingIoStats,
    task_queue_stats: AHashMap<TaskQueueHandle, RingIoStats>,
    source_map: Rc<RefCell<SourceMap>>,
    in_kernel: usize,
}

impl PollRing {
    fn new(
        size: usize,
        allocator: Rc<UringBufferAllocator>,
        source_map: Rc<RefCell<SourceMap>>,
    ) -> io::Result<Self> {
        let ring = iou::IoUring::new_with_flags(
            size as _,
            iou::SetupFlags::IOPOLL,
            iou::SetupFeatures::empty(),
        )?;
        Ok(PollRing {
            size,
            ring,
            submission_queue: UringQueueState::with_capacity(size * 4),
            allocator,
            stats: RingIoStats::default(),
            task_queue_stats: AHashMap::new(),
            source_map,
            in_kernel: 0,
        })
    }

    pub(crate) fn alloc_dma_buffer(&mut self, size: usize) -> DmaBuffer {
        self.allocator.new_buffer(size).unwrap()
    }
}

impl UringCommon for PollRing {
    fn name(&self) -> &'static str {
        "poll"
    }

    fn io_stats_mut(&mut self) -> &mut RingIoStats {
        &mut self.stats
    }

    fn io_stats_for_task_queue_mut(&mut self, handle: TaskQueueHandle) -> &mut RingIoStats {
        self.task_queue_stats.entry(handle).or_default()
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
        self.submission_queue.borrow().is_empty() && !self.needs_kernel_enter()
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

    fn submission_queue(&mut self) -> ReactorQueue {
        self.submission_queue.clone()
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        let x = self.ring.submit_sqes()? as usize;
        self.in_kernel += x;
        Ok(x)
    }

    fn submit_wait_sqes(&mut self, _: u32) -> io::Result<usize> {
        unreachable!("waiting on the poll ring is impossible")
    }

    fn consume_one_event(&mut self) -> Option<bool> {
        let source_map = self.source_map.clone();
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

    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<bool> {
        let source_map = &mut *self.source_map.borrow_mut();
        let now = Instant::now();

        while let Some(chain) = peek_one_chain(queue, self.size) {
            return if let Some(sqes) = self.ring.sq().prepare_sqes(chain.len() as u32) {
                let ops = extract_one_chain(source_map, queue, chain, now);
                if ops.is_empty() {
                    // all the sources in the ring were cancelled
                    continue;
                }

                for (op, mut sqe) in ops.into_iter().zip(sqes.into_iter()) {
                    let allocator = self.allocator.clone();
                    fill_sqe(
                        &mut sqe,
                        &op,
                        move |size| allocator.new_buffer(size),
                        source_map,
                    );
                }
                Some(true)
            } else {
                None
            };
        }
        Some(false)
    }

    fn submit_one_event2(&mut self, queue: &mut VecDeque<PinnedInnerSource>) -> Option<bool> {
        let source_map = &mut *self.source_map.borrow_mut();
        let mut sq = self.ring.sq();

        let sqes = queue
            .front()
            .map(|src| sq.prepare_sqes(sqe_required(src) as u32));

        match sqes {
            Some(Some(sqes)) => {
                let src = queue.pop_front().unwrap();
                let mut x = src.borrow_mut();
                x.wakers.submitted_at = Some(Instant::now());
                let enqueued = x.enqueued.as_ref().map(|x| (x.status, x.id));
                drop(x);

                if let Some((status, id)) = enqueued {
                    match status {
                        EnqueuedStatus::Enqueued => {
                            src.borrow_mut().enqueued.as_mut().unwrap().status =
                                EnqueuedStatus::Dispatched;
                        }
                        EnqueuedStatus::Canceled => {
                            source_map.consume_source(id);
                            return Some(true);
                        }
                        _ => unreachable!(),
                    }
                }

                // we are golden
                prepare_one_source(src, sqes, |size| self.allocator.new_buffer(size));
                Some(true)
            }
            Some(None) => None,  // No SQEs available
            None => Some(false), // No source to prepare
        }
    }
}

struct SleepableRing {
    ring: iou::IoUring,
    size: usize,
    submission_queue: ReactorQueue,
    name: &'static str,
    allocator: Rc<UringBufferAllocator>,
    stats: RingIoStats,
    task_queue_stats: AHashMap<TaskQueueHandle, RingIoStats>,
    source_map: Rc<RefCell<SourceMap>>,
    in_kernel: usize,
}

impl SleepableRing {
    fn new(
        size: usize,
        name: &'static str,
        allocator: Rc<UringBufferAllocator>,
        source_map: Rc<RefCell<SourceMap>>,
    ) -> io::Result<Self> {
        assert!(*IO_URING_RECENT_ENOUGH);
        Ok(SleepableRing {
            ring: iou::IoUring::new(size as _)?,
            size,
            submission_queue: UringQueueState::with_capacity(size * 4),
            name,
            allocator,
            stats: RingIoStats::default(),
            task_queue_stats: AHashMap::new(),
            source_map,
            in_kernel: 0,
        })
    }

    fn ring_fd(&self) -> RawFd {
        self.ring.raw().ring_fd
    }

    /// This function prepares a timer that fires unconditionally after a
    /// certain duration. The timer is added at the front of the queue such
    /// that it will be the first SQE submitted the next time we enter the
    /// latency ring
    fn prepare_latency_preemption_timer(&mut self, d: Duration) -> Source {
        let source = Source::new(
            IoRequirements::default(),
            SourceType::Timeout(TimeSpec64::from(d), 0),
            None,
            None,
        );

        self.source_map
            .borrow_mut()
            .add_source(source.inner.clone(), self.submission_queue.clone());

        self.submission_queue()
            .borrow_mut()
            .submissions
            .push_front(source.inner.clone());
        source
    }

    /// This function prepares a timer that fires only after a given number of
    /// CQEs are available on the main ring. This timer is linked with a write
    /// to the latency ring's event fd such that this timer triggers a
    /// preemption via the latency ring.
    fn prepare_throughput_preemption_timer(&mut self, min_events: u32, event_fd: RawFd) -> Source {
        assert!(min_events >= 1, "min_events should be at least 1");
        let source = Source::new(
            IoRequirements::default(),
            SourceType::ThroughputPreemption(event_fd, TimeSpec64::from(Duration::MAX), min_events),
            None,
            None,
        );

        self.source_map
            .borrow_mut()
            .add_source(source.inner.clone(), self.submission_queue.clone());

        self.submission_queue
            .borrow_mut()
            .submissions
            .push_front(source.inner.clone());
        source
    }

    fn install_eventfd(&mut self, eventfd_src: &Source) -> bool {
        if let Some(mut sqe) = self.ring.sq().prepare_sqe() {
            // Now must wait on the `eventfd` in case someone wants to wake us up.
            // If we can't then we can't sleep and will just bail immediately
            let op = UringDescriptor {
                fd: eventfd_src.raw(),
                flags: SubmissionFlags::empty(),
                user_data: to_user_data(
                    self.source_map
                        .borrow_mut()
                        .add_source(eventfd_src.inner.clone(), self.submission_queue.clone()),
                ),
                args: UringOpDescriptor::Read(0, 8),
            };

            fill_sqe(
                &mut sqe,
                &op,
                |_| unreachable!(),
                &mut *self.source_map.borrow_mut(),
            );

            match &mut *eventfd_src.source_type_mut() {
                SourceType::ForeignNotifier(_, _, installed) => {
                    *installed = self.submit_sqes().is_ok();
                    *installed
                }
                _ => unreachable!("Expected ForeignNotifier source type"),
            }
        } else {
            false
        }
    }
}

impl UringCommon for SleepableRing {
    fn name(&self) -> &'static str {
        self.name
    }

    fn io_stats_mut(&mut self) -> &mut RingIoStats {
        &mut self.stats
    }

    fn io_stats_for_task_queue_mut(&mut self, handle: TaskQueueHandle) -> &mut RingIoStats {
        self.task_queue_stats.entry(handle).or_default()
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
        self.submission_queue.borrow().is_empty()
            && self.waiting_kernel_submission() == 0
            && self.waiting_kernel_collection() == 0
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

    fn submission_queue(&mut self) -> ReactorQueue {
        self.submission_queue.clone()
    }

    fn submit_sqes(&mut self) -> io::Result<usize> {
        let x = self.ring.submit_sqes()? as usize;
        self.in_kernel += x;
        Ok(x)
    }

    fn submit_wait_sqes(&mut self, wait: u32) -> io::Result<usize> {
        let x = self.ring.submit_sqes_and_wait(wait)? as usize;
        self.in_kernel += x;
        Ok(x)
    }

    fn consume_one_event(&mut self) -> Option<bool> {
        let source_map = self.source_map.clone();
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

    fn submit_one_event(&mut self, queue: &mut VecDeque<UringDescriptor>) -> Option<bool> {
        let source_map = &mut *self.source_map.borrow_mut();
        let now = Instant::now();

        while let Some(chain) = peek_one_chain(queue, self.size) {
            return if let Some(sqes) = self.ring.sq().prepare_sqes(chain.len() as u32) {
                let ops = extract_one_chain(source_map, queue, chain, now);
                if ops.is_empty() {
                    // all the sources in the ring were cancelled
                    continue;
                }

                for (op, mut sqe) in ops.into_iter().zip(sqes.into_iter()) {
                    let allocator = self.allocator.clone();
                    fill_sqe(
                        &mut sqe,
                        &op,
                        move |size| allocator.new_buffer(size),
                        source_map,
                    );
                }
                Some(true)
            } else {
                None
            };
        }
        Some(false)
    }

    fn submit_one_event2(&mut self, queue: &mut VecDeque<PinnedInnerSource>) -> Option<bool> {
        let source_map = &mut *self.source_map.borrow_mut();
        let mut sq = self.ring.sq();

        let sqes = queue
            .front()
            .map(|src| sq.prepare_sqes(sqe_required(src) as u32));

        match sqes {
            Some(Some(sqes)) => {
                let src = queue.pop_front().unwrap();
                let mut x = src.borrow_mut();
                x.wakers.submitted_at = Some(Instant::now());
                let enqueued = x.enqueued.as_ref().map(|x| (x.status, x.id));
                drop(x);

                if let Some((status, id)) = enqueued {
                    match status {
                        EnqueuedStatus::Enqueued => {
                            src.borrow_mut().enqueued.as_mut().unwrap().status =
                                EnqueuedStatus::Dispatched;
                        }
                        EnqueuedStatus::Canceled => {
                            source_map.consume_source(id);
                            return Some(true);
                        }
                        _ => unreachable!(),
                    }
                }

                // we are golden
                prepare_one_source(src, sqes, |size| self.allocator.new_buffer(size));
                Some(true)
            }
            Some(None) => None,  // No SQEs available
            None => Some(false), // No source to prepare
        }
    }
}

pub(crate) struct Reactor {
    // FIXME: it is starting to feel we should clean this up to a Inner pattern
    main_ring: RefCell<SleepableRing>,
    latency_ring: RefCell<SleepableRing>,
    poll_ring: RefCell<PollRing>,

    latency_preemption_timeout_src: Cell<Option<Source>>,
    throughput_preemption_timeout_src: Cell<Option<Source>>,

    link_fd: RawFd,

    // This keeps the `eventfd` alive. Drop will close it when we're done
    notifier: Arc<sys::SleepNotifier>,
    // This is the source used to handle the notifications into the ring.
    // It is reused, unlike the timeout src, because it is possible and likely
    // that it will be in the ring through many calls to the reactor loop. It only ever gets
    // completed if this reactor is woken up from another one
    eventfd_src: Source,
    source_map: Rc<RefCell<SourceMap>>,

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
    (into $woke:expr; $( $ring:expr ),+ ) => {{
        let mut consumed = 0;
        $(
            consumed += $ring.consume_completion_queue($woke);
        )*
        consumed
    }}
}

/// It is important to process cancellations as soon as we see them,
/// which is why they go into a separate queue. The reason is that
/// cancellations can be racy if they are left to their own devices.
///
/// Imagine that you have a write request to fd 3 and wants to cancel it.
/// But before the cancellation is run fd 3 gets closed and another file
/// is opened with the same fd.
macro_rules! flush_cancellations {
    (into $output:expr; $( $ring:expr ),+ ) => {{
        $(
            let q = $ring.submission_queue();
            let mut queue = q.borrow_mut();
            let min = queue.cancellations.len();
            $ring.force_flush_ring(&mut queue.cancellations, min, $output);
        )*
    }}
}

macro_rules! flush_rings {
    ($( $ring:expr ),+ ) => {{
        let mut ret = 0;
        $(
            ret += $ring.consume_submission_queue()
                .or_else(Reactor::busy_ok)
                .or_else(Reactor::again_ok)
                .or_else(Reactor::intr_ok)?;
        )*
        io::Result::Ok(ret)
    }}
}

macro_rules! force_flush_rings {
     (into $output:expr; $min:expr, $( $ring:expr ),+ ) => {{
        let mut ret = 0;
        $(
            let q = $ring.submission_queue();
            let mut queue = q.borrow_mut();
            ret += $ring.force_flush_ring2(&mut queue.submissions, $min, &mut ret);
        )*
        ret
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

        let source_map = Rc::new(RefCell::new(SourceMap::default()));
        // always have at least some small amount of memory for the slab
        io_memory = std::cmp::max(align_up(io_memory, 4096), 65536);

        let allocator = Rc::new(UringBufferAllocator::new(io_memory));
        let registry = vec![allocator.as_bytes()];

        let main_ring =
            SleepableRing::new(ring_depth, "main", allocator.clone(), source_map.clone())?;
        let poll_ring = PollRing::new(ring_depth, allocator.clone(), source_map.clone())?;
        let mut latency_ring =
            SleepableRing::new(ring_depth, "latency", allocator.clone(), source_map.clone())?;

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

        let eventfd_src = Source::new(
            IoRequirements::default(),
            SourceType::ForeignNotifier(notifier.eventfd_fd(), 0, false),
            None,
            None,
        );

        if !eventfd_src.is_installed().unwrap() {
            latency_ring.install_eventfd(&eventfd_src);
        }

        Ok(Reactor {
            main_ring: RefCell::new(main_ring),
            latency_ring: RefCell::new(latency_ring),
            poll_ring: RefCell::new(poll_ring),
            latency_preemption_timeout_src: Cell::new(None),
            throughput_preemption_timeout_src: Cell::new(None),
            blocking_thread: BlockingThreadPool::new(thread_pool_placement, notifier.clone())?,
            link_fd,
            notifier,
            eventfd_src,
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

    pub(crate) fn install_eventfd(&self) {
        if !self.eventfd_src.is_installed().unwrap() {
            self.latency_ring
                .borrow_mut()
                .install_eventfd(&self.eventfd_src);
        }
    }

    pub(crate) fn process_foreign_wakes(&self) -> usize {
        self.notifier.process_foreign_wakes()
    }

    pub(crate) fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer {
        let mut poll_ring = self.poll_ring.borrow_mut();
        poll_ring.alloc_dma_buffer(size)
    }

    pub(crate) fn write_dma(&self, source: &Source, pos: u64) {
        let op = match &*source.source_type() {
            SourceType::Write(
                _,
                _,
                PollableStatus::NonPollable(DirectIo::Disabled),
                IoBuffer::Dma(buf),
            ) => UringOpDescriptor::Write(buf.as_ptr(), buf.len(), pos),
            SourceType::Write(_, _, _, IoBuffer::Dma(buf)) => match buf.uring_buffer_id() {
                Some(id) => UringOpDescriptor::WriteFixed(buf.as_ptr(), buf.len(), pos, id),
                None => UringOpDescriptor::Write(buf.as_ptr(), buf.len(), pos),
            },
            x => panic!("Unexpected source type for write: {:?}", x),
        };
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn write_buffered(&self, source: &Source, pos: u64) {
        let op = match &*source.source_type() {
            SourceType::Write(
                _,
                _,
                PollableStatus::NonPollable(DirectIo::Disabled),
                IoBuffer::Buffered(buf),
            ) => UringOpDescriptor::Write(buf.as_ptr() as *const u8, buf.len(), pos),
            x => panic!("Unexpected source type for write: {:?}", x),
        };
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn read_dma(&self, source: &Source, pos: u64, size: usize) {
        let op = UringOpDescriptor::ReadFixed(pos, size);
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn read_buffered(&self, source: &Source, pos: u64, size: usize) {
        let op = UringOpDescriptor::Read(pos, size);
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn poll_ready(&self, source: &Source, flags: PollFlags) {
        let op = UringOpDescriptor::PollAdd(flags);
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn send(&self, source: &Source, flags: MsgFlags) {
        let op = match &*source.source_type() {
            SourceType::SockSend(_, buf, _) => {
                UringOpDescriptor::SockSend(buf.as_ptr() as *const u8, buf.len(), flags.bits())
            }
            _ => unreachable!(),
        };
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn sendmsg(&self, source: &Source, flags: MsgFlags) {
        let op = match &mut *source.source_type_mut() {
            SourceType::SockSendMsg(_, _, _, iov, hdr, addr) => {
                let (msg_name, msg_namelen) = addr.as_ffi_pair();
                let msg_name = msg_name as *const nix::sys::socket::sockaddr as *mut libc::c_void;

                hdr.msg_iov = iov as *mut libc::iovec;
                hdr.msg_iovlen = 1;
                hdr.msg_name = msg_name;
                hdr.msg_namelen = msg_namelen;

                UringOpDescriptor::SockSendMsg(hdr, flags.bits())
            }
            _ => unreachable!(),
        };
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn recv(&self, source: &Source, len: usize, flags: MsgFlags) {
        let op = UringOpDescriptor::SockRecv(len, flags.bits());
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn recvmsg(&self, source: &Source, len: usize, flags: MsgFlags) {
        let op = UringOpDescriptor::SockRecvMsg(len, flags.bits());
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn connect(&self, source: &Source) {
        let op = match &*source.source_type() {
            SourceType::Connect(_, addr) => UringOpDescriptor::Connect(addr as *const SockAddr),
            x => panic!("Unexpected source type for connect: {:?}", x),
        };
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn accept(&self, source: &Source) {
        let op = match &mut *source.source_type_mut() {
            SourceType::Accept(_, addr) => UringOpDescriptor::Accept(addr as *mut SockAddrStorage),
            x => panic!("Unexpected source type for accept: {:?}", x),
        };
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn fdatasync(&self, source: &Source) {
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            UringOpDescriptor::FDataSync,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn fallocate(&self, source: &Source, offset: u64, size: u64, flags: libc::c_int) {
        let op = UringOpDescriptor::Fallocate(offset, size, flags);
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
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

    pub(crate) fn create_dir(&self, source: &Source, mode: libc::c_int) {
        let path = match &*source.source_type() {
            SourceType::CreateDir(p, _) => p.clone(),
            _ => panic!("Unexpected source for rename operation"),
        };

        let op = BlockingThreadOp::CreateDir(path, mode);
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
        let op = UringOpDescriptor::Close;
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn statx(&self, source: &Source) {
        let op = match &*source.source_type() {
            SourceType::Statx(_, path, buf) => {
                let path = path.as_c_str().as_ptr();
                let buf = buf.as_ptr();
                UringOpDescriptor::Statx(path as _, buf)
            }
            _ => panic!("Unexpected source for statx operation"),
        };
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    pub(crate) fn open_at(&self, source: &Source, flags: libc::c_int, mode: libc::mode_t) {
        let pathptr = match &*source.source_type() {
            SourceType::Open(_, cstring, _, _) => cstring.as_c_str().as_ptr(),
            _ => panic!("Wrong source type!"),
        };
        let op = UringOpDescriptor::Open(pathptr as _, flags, mode as _);
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut *self.source_map.borrow_mut(),
        );
    }

    #[cfg(feature = "bench")]
    pub(crate) fn nop(&self, source: &Source) {
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            UringOpDescriptor::Nop,
            &mut *self.source_map.borrow_mut(),
        );
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
    fn link_rings_and_sleep(&self, ring: &mut SleepableRing) -> io::Result<()> {
        assert_eq!(
            ring.waiting_kernel_submission(),
            0,
            "sleeping with pending SQEs"
        );

        if let Some(sqes) = ring.ring.sq().prepare_sqes(1) {
            prepare_one_source(
                InnerSource::pin(
                    IoRequirements::default(),
                    SourceType::PollAdd(self.link_fd, common_flags() | read_flags()),
                    None,
                    None,
                ),
                sqes,
                |_| unreachable!(),
            );
            ring.submit_wait_sqes(1)
                .or_else(Reactor::busy_ok)
                .or_else(Reactor::again_ok)
                .or_else(Reactor::intr_ok)?;
            Ok(())
        } else {
            Ok(()) // can't sleep but that's fine
        }
    }

    pub(crate) fn poll_io(&self, woke: &mut usize) -> io::Result<()> {
        self.poll_ring.borrow_mut().poll(woke)?;
        self.main_ring.borrow_mut().poll(woke)?;
        self.latency_ring.borrow_mut().poll(woke)?;
        *woke += self.flush_syscall_thread();
        Ok(())
    }

    pub(crate) fn rush_dispatch(&self, src: &Source, woke: &mut usize) -> io::Result<()> {
        let ring = &mut *self.ring_for_source(src);
        if ring.may_rush() {
            ring.poll(woke)
        } else {
            Ok(())
        }
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
        &self,
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

        let mut poll_ring = self.poll_ring.borrow_mut();
        let mut main_ring = self.main_ring.borrow_mut();
        let mut lat_ring = self.latency_ring.borrow_mut();

        // consume all events from the rings
        consume_rings!(into &mut woke; lat_ring, poll_ring, main_ring);

        // Cancel the old timer regardless of whether we can sleep:
        // if we won't sleep, we will register the new timer with its new
        // value.
        //
        // But if we will sleep, there might be a timer registered that needs
        // to be removed otherwise we'll wake up when it expires.
        drop(self.latency_preemption_timeout_src.take());

        // Schedule the throughput-based timeout immediately: it won't matter if we end
        // up sleeping.
        self.throughput_preemption_timeout_src.replace(Some(
            main_ring.prepare_throughput_preemption_timer(
                self.ring_depth() as u32,
                self.eventfd_src.raw(),
            ),
        ));

        flush_cancellations!(into &mut woke; lat_ring, poll_ring, main_ring);
        flush_rings!(lat_ring, poll_ring, main_ring)?;
        // pick up the results of any cancellations
        consume_rings!(into &mut woke; lat_ring, poll_ring, main_ring);

        // If we generated any event so far, we can't sleep. Need to handle them.
        let should_sleep = preempt_timer().is_none()
            && (woke == 0)
            && poll_ring.can_sleep()
            && main_ring.can_sleep()
            && lat_ring.can_sleep();

        if should_sleep {
            // We are about to go to sleep. It's ok to sleep, but if there
            // is a timer set, we need to make sure we wake up to handle it.
            if let Some(dur) = user_timer {
                self.latency_preemption_timeout_src
                    .set(Some(lat_ring.prepare_latency_preemption_timer(dur)));
                assert!(flush_rings!(lat_ring)? > 0);
            }
            // From this moment on the remote executors are aware that we are sleeping
            // We have to sweep the remote channels function once more because since
            // last time until now it could be that something happened in a remote executor
            // that opened up room. If if did we bail on sleep and go process it.
            self.notifier.prepare_to_sleep();
            // See https://www.scylladb.com/2018/02/15/memory-barriers-seastar-linux/ for
            // details. This translates to `sys_membarrier()` /
            // `MEMBARRIER_CMD_PRIVATE_EXPEDITED`
            membarrier::heavy();
            let events = process_remote_channels() + self.flush_syscall_thread();
            if events == 0 {
                if self.eventfd_src.is_installed().unwrap() {
                    self.link_rings_and_sleep(&mut main_ring)
                        .expect("some error");
                    // May have new cancellations related to the link ring fd.
                    flush_cancellations!(into &mut 0; lat_ring, poll_ring, main_ring);
                    flush_rings!(lat_ring, poll_ring, main_ring)?;
                    consume_rings!(into &mut 0; lat_ring, poll_ring, main_ring);
                }
                // Woke up, so no need to notify us anymore.
                self.notifier.wake_up();
            }
        }

        if let Some(preempt) = preempt_timer() {
            self.latency_preemption_timeout_src
                .set(Some(lat_ring.prepare_latency_preemption_timer(preempt)));
            assert!(force_flush_rings!(into &mut 0; 1, lat_ring, main_ring) > 0);
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
        let mut lat_ring = self.latency_ring.borrow_mut();
        let cq = unsafe { &lat_ring.ring.raw_mut().cq };
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
        let q = self.main_ring.borrow_mut().submission_queue();
        let mut queue = q.borrow_mut();
        queue.submissions.push_back(InnerSource::pin(
            IoRequirements::default(),
            SourceType::Close(fd),
            None,
            None,
        ));
    }

    pub(crate) fn ring_for_source(&self, source: &Source) -> RefMut<'_, dyn UringCommon> {
        // Dispatch requests according to the following rules:
        // * Disk reads/writes go to the poll ring if possible, or the main ring
        //   otherwise;
        // * Network Rx and connect/accept go the latency ring;
        // * Every other request are dispatched to the main ring;
        // We avoid putting requests that come in high numbers on the latency ring
        // because the more request we issue there, the less effective it becomes.

        match &*source.source_type() {
            SourceType::Read(_, _, _, p, _) | SourceType::Write(_, _, p, _) => match p {
                PollableStatus::Pollable => self.poll_ring.borrow_mut(),
                PollableStatus::NonPollable(_) => self.main_ring.borrow_mut(),
            },
            SourceType::SockRecv(_, _, _, _)
            | SourceType::SockRecvMsg(_, _, _, _, _, _, _)
            | SourceType::Accept(_, _)
            | SourceType::Connect(_, _) => self.latency_ring.borrow_mut(),
            SourceType::Invalid => {
                unreachable!("called ring_for_source on invalid source")
            }
            _ => self.main_ring.borrow_mut(),
        }
    }

    pub fn io_stats(&self) -> IoStats {
        IoStats::new(
            std::mem::take(&mut self.main_ring.borrow_mut().stats),
            std::mem::take(&mut self.latency_ring.borrow_mut().stats),
            std::mem::take(&mut self.poll_ring.borrow_mut().stats),
        )
    }

    pub(crate) fn task_queue_io_stats(&self, h: &TaskQueueHandle) -> Option<IoStats> {
        let main = self
            .main_ring
            .borrow_mut()
            .task_queue_stats
            .get_mut(h)
            .map(std::mem::take);
        let lat = self
            .latency_ring
            .borrow_mut()
            .task_queue_stats
            .get_mut(h)
            .map(std::mem::take);
        let poll = self
            .poll_ring
            .borrow_mut()
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
}

fn queue_request_into_ring(
    ring: &mut (impl UringCommon + ?Sized),
    source: &Source,
    _descriptor: UringOpDescriptor,
    source_map: &mut SourceMap,
) {
    source.inner.borrow_mut().wakers.queued_at = Some(Instant::now());
    let q = ring.submission_queue();
    source_map.add_source(source.inner.clone(), q.clone());

    let mut queue = q.borrow_mut();
    queue.submissions.push_back(source.inner.clone());
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
        let reactor = Reactor::new(notifier, 0, 128, PoolPlacement::Unbound(1)).unwrap();

        fn timeout_source(millis: u64) -> (Source, UringOpDescriptor) {
            let source = Source::new(
                IoRequirements::default(),
                SourceType::Timeout(TimeSpec64::from(Duration::from_millis(millis)), 0),
                None,
                None,
            );
            let op = match &*source.source_type() {
                SourceType::Timeout(ts, events) => {
                    UringOpDescriptor::Timeout(&ts.raw as *const _, *events)
                }
                _ => unreachable!(),
            };
            (source, op)
        }

        let (fast, op) = timeout_source(50);
        queue_request_into_ring(
            &mut *reactor.ring_for_source(&fast),
            &fast,
            op,
            &mut *reactor.source_map.borrow_mut(),
        );

        let (slow, op) = timeout_source(150);
        queue_request_into_ring(
            &mut *reactor.ring_for_source(&slow),
            &slow,
            op,
            &mut *reactor.source_map.borrow_mut(),
        );

        let (lethargic, op) = timeout_source(300);
        queue_request_into_ring(
            &mut *reactor.ring_for_source(&lethargic),
            &lethargic,
            op,
            &mut *reactor.source_map.borrow_mut(),
        );

        let start = Instant::now();
        reactor.wait(|| None, None, 0, || 0).unwrap();
        let elapsed_ms = start.elapsed().as_millis();
        assert!((50..100).contains(&elapsed_ms));

        drop(slow); // Cancel this one.

        reactor.wait(|| None, None, 0, || 0).unwrap();
        let elapsed_ms = start.elapsed().as_millis();
        assert!((300..350).contains(&elapsed_ms));
    }
}
