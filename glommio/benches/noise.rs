use glommio::{enclose, prelude::*, Task};
use std::{
    cell::RefCell,
    rc::Rc,
    task::{Poll, Waker},
    time::Instant,
};

fn noise_common_task_queue(tasks: usize) {
    let local_ex = LocalExecutorBuilder::new()
        .pin_to_cpu(0)
        .spawn(move || async move {
            let iter: usize = 10_000;
            let t = Instant::now();

            let waker: Rc<RefCell<Option<Waker>>> = Default::default();
            let counter: Rc<RefCell<usize>> = Default::default();

            let _futs: Vec<Task<()>> = (0..tasks)
                .map(|_| {
                    glommio::spawn_local(enclose!( (waker) async move {
                        loop {
                            if let Some(ref waker) = *waker.borrow() {
                                waker.wake_by_ref();
                            }
                            futures_lite::future::yield_now().await;
                        }
                    }))
                })
                .collect();

            let driver_task = futures_lite::future::poll_fn(enclose!(
                (waker, counter) | cx | {
                    if waker.borrow_mut().replace(cx.waker().clone()).is_none() {
                        cx.waker().wake_by_ref();
                    }
                    *counter.borrow_mut() += 1;
                    if *counter.borrow() >= iter {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }
            ));

            driver_task.await;

            let t = t.elapsed();
            println!(
                "time to complete for {} tasks: {:#?} ({:#?} per task)",
                tasks,
                t,
                t / tasks as u32
            );
        })
        .unwrap();
    local_ex.join().unwrap();
}

fn noise_separate_task_queue(tasks: usize) {
    let local_ex = LocalExecutorBuilder::new()
        .pin_to_cpu(0)
        .spawn(move || async move {
            let iter: usize = 10_000;
            let t = Instant::now();

            let waker: Rc<RefCell<Option<Waker>>> = Default::default();
            let counter: Rc<RefCell<usize>> = Default::default();

            let tq = glommio::executor().create_task_queue(
                Shares::Static(1000),
                Latency::NotImportant,
                "foo",
            );

            let _futs: Vec<Task<()>> = (0..tasks)
                .map(|_| {
                    glommio::spawn_local_into(
                        enclose!( (waker) async move {
                            loop {
                                if let Some(ref waker) = *waker.borrow() {
                                    waker.wake_by_ref();
                                }
                                futures_lite::future::yield_now().await;
                            }
                        }),
                        tq,
                    )
                    .unwrap()
                })
                .collect();

            let driver_task = futures_lite::future::poll_fn(enclose!(
                (waker, counter) | cx | {
                    if waker.borrow_mut().replace(cx.waker().clone()).is_none() {
                        cx.waker().wake_by_ref();
                    }
                    *counter.borrow_mut() += 1;
                    if *counter.borrow() >= iter {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }
            ));

            driver_task.await;

            let t = t.elapsed();
            println!(
                "time to complete for {} tasks: {:#?} ({:#?} per task)",
                tasks,
                t,
                t / tasks as u32
            );
        })
        .unwrap();
    local_ex.join().unwrap();
}

fn main() {
    noise_common_task_queue(100);
    noise_common_task_queue(1000);
    noise_common_task_queue(10000);

    noise_separate_task_queue(100);
    noise_separate_task_queue(1000);
    noise_separate_task_queue(10000);
}
