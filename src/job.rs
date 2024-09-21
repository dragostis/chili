use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    mem::ManuallyDrop,
    panic::{self, AssertUnwindSafe},
    ptr::NonNull,
    sync::atomic::{AtomicU8, Ordering},
    thread::{self, Thread},
};

use crate::Scope;

enum Poll {
    Pending,
    Ready,
    Locked,
}

#[derive(Debug, Default)]
pub struct Future<T = ()> {
    state: AtomicU8,
    /// Can only be accessed if `state` is `Poll::Locked`.
    waiting_thread: UnsafeCell<Option<Thread>>,
    /// Can only be written if `state` is `Poll::Locked` and read if `state` is
    /// `Poll::Ready`.
    val: UnsafeCell<Option<Box<thread::Result<T>>>>,
}

impl<T> Future<T> {
    pub fn poll(&self) -> bool {
        self.state.load(Ordering::Acquire) == Poll::Ready as u8
    }

    pub fn wait(&self) -> Option<thread::Result<T>> {
        loop {
            let result = self.state.compare_exchange(
                Poll::Pending as u8,
                Poll::Locked as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );

            match result {
                Ok(_) => {
                    // SAFETY:
                    // Lock is acquired, only we are accessing `self.waiting_thread`.
                    unsafe { *self.waiting_thread.get() = Some(thread::current()) };

                    self.state.store(Poll::Pending as u8, Ordering::Release);

                    thread::park();

                    // Skip yielding after being woken up.
                    continue;
                }
                Err(state) if state == Poll::Ready as u8 => {
                    // SAFETY:
                    // `state` is `Poll::Ready` only after `Self::complete`
                    // releases the lock.
                    //
                    // Calling `Self::complete` when `state` is `Poll::Ready`
                    // cannot mutate `self.val`.
                    break unsafe { (*self.val.get()).take().map(|b| *b) };
                }
                _ => (),
            }

            thread::yield_now();
        }
    }

    pub fn complete(&self, val: thread::Result<T>) {
        let val = Box::new(val);

        loop {
            let result = self.state.compare_exchange(
                Poll::Pending as u8,
                Poll::Locked as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );

            match result {
                Ok(_) => break,
                Err(_) => thread::yield_now(),
            }
        }

        // SAFETY:
        // Lock is acquired, only we are accessing `self.val`.
        unsafe {
            *self.val.get() = Some(val);
        }

        // SAFETY:
        // Lock is acquired, only we are accessing `self.waiting_thread`.
        if let Some(thread) = unsafe { (*self.waiting_thread.get()).take() } {
            thread.unpark();
        }

        self.state.store(Poll::Ready as u8, Ordering::Release);
    }
}

pub struct JobStack<F = ()> {
    /// All code paths should call either `Job::execute` or `Self::unwrap` to
    /// avoid a potential memory leak.
    f: UnsafeCell<ManuallyDrop<F>>,
}

impl<F> JobStack<F> {
    pub fn new(f: F) -> Self {
        Self {
            f: UnsafeCell::new(ManuallyDrop::new(f)),
        }
    }

    /// SAFETY:
    /// It should only be called once.
    pub unsafe fn take_once(&self) -> F {
        // SAFETY:
        // No `Job` has has been executed, therefore `self.f` has not yet been
        // `take`n.
        unsafe { ManuallyDrop::take(&mut *self.f.get()) }
    }
}

/// `Job` is only sent, not shared between threads.
///
/// When popped from the `JobQueue`, it gets copied before sending across
/// thread boundaries.
#[derive(Clone, Debug)]
pub struct Job<T = ()> {
    stack: NonNull<JobStack>,
    harness: unsafe fn(&mut Scope<'_>, NonNull<JobStack>, NonNull<Future>),
    fut: Cell<Option<NonNull<Future<T>>>>,
}

impl<T> Job<T> {
    pub fn new<F>(stack: &JobStack<F>) -> Self
    where
        F: FnOnce(&mut Scope<'_>) -> T + Send,
        T: Send,
    {
        /// SAFETY:
        /// It should only be called while the `stack` is still alive.
        unsafe fn harness<F, T>(
            scope: &mut Scope<'_>,
            stack: NonNull<JobStack>,
            fut: NonNull<Future>,
        ) where
            F: FnOnce(&mut Scope<'_>) -> T + Send,
            T: Send,
        {
            // SAFETY:
            // The `stack` is still alive.
            let stack: &JobStack<F> = unsafe { stack.cast().as_ref() };
            // SAFETY:
            // This is the first call to `take_once` since `Job::execute`
            // (the only place where this harness is called) is called only
            // after the job has been popped.
            let f = unsafe { stack.take_once() };
            // SAFETY:
            // Before being popped, the `JobQueue` allocates and stores a
            // `Future` in `self.fur_or_next` that should get passed here.
            let fut: &Future<T> = unsafe { fut.cast().as_ref() };

            fut.complete(panic::catch_unwind(AssertUnwindSafe(|| f(scope))));
        }

        Self {
            stack: NonNull::from(stack).cast(),
            harness: harness::<F, T>,
            fut: Cell::new(None),
        }
    }

    pub fn is_waiting(&self) -> bool {
        self.fut.get().is_none()
    }

    pub fn eq(&self, other: &Job) -> bool {
        self.stack == other.stack
    }

    /// SAFETY:
    /// It should only be called after being popped from a `JobQueue`.
    pub unsafe fn poll(&self) -> bool {
        self.fut
            .get()
            .map(|fut| {
                // SAFETY:
                // Before being popped, the `JobQueue` allocates and stores a
                // `Future` in `self.fur_or_next` that should get passed here.
                let fut = unsafe { fut.as_ref() };
                fut.poll()
            })
            .unwrap_or_default()
    }

    /// SAFETY:
    /// It should only be called after being popped from a `JobQueue`.
    pub unsafe fn wait(&self) -> Option<thread::Result<T>> {
        self.fut.get().and_then(|fut| {
            // SAFETY:
            // Before being popped, the `JobQueue` allocates and stores a
            // `Future` in `self.fur_or_next` that should get passed here.
            let result = unsafe { fut.as_ref().wait() };
            // SAFETY:
            // We only can drop the `Box` *after* waiting on the `Future`
            // in order to ensure unique access.
            unsafe {
                drop(Box::from_raw(fut.as_ptr()));
            }

            result
        })
    }

    /// SAFETY:
    /// It should only be called in the case where the job has been popped
    /// from the front and will not be `Job::Wait`ed.
    pub unsafe fn drop(&self) {
        if let Some(fut) = self.fut.get() {
            // SAFETY:
            // Before being popped, the `JobQueue` allocates and store a
            // `Future` in `self.fur_or_next` that should get passed here.
            unsafe {
                drop(Box::from_raw(fut.as_ptr()));
            }
        }
    }
}

impl Job {
    /// SAFETY:
    /// It should only be called while the `JobStack` it was created with is
    /// still alive and after being popped from a `JobQueue`.
    pub unsafe fn execute(&self, scope: &mut Scope<'_>) {
        // SAFETY:
        // Before being popped, the `JobQueue` allocates and store a
        // `Future` in `self.fur_or_next` that should get passed here.
        unsafe {
            (self.harness)(scope, self.stack, self.fut.get().unwrap());
        }
    }
}

// SAFETY:
// The job's `stack` will only be accessed after acquiring a lock (in
// `Future`), while `prev` and `fut_or_next` are never accessed after being
// sent across threads.
unsafe impl Send for Job {}

#[derive(Debug, Default)]
pub struct JobQueue(VecDeque<NonNull<Job>>);

impl JobQueue {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// SAFETY:
    /// Any `Job` pushed onto the queue should alive at least until it gets
    /// popped.
    pub unsafe fn push_back<T>(&mut self, job: &Job<T>) {
        let next_tail = unsafe { &*(job as *const Job<T> as *const Job) };
        self.0.push_back(NonNull::from(next_tail).cast());
    }

    pub fn pop_back(&mut self) {
        self.0.pop_back();
    }

    pub fn pop_front(&mut self) -> Option<Job> {
        let val = self.0.pop_front()?;
        // SAFETY:
        // `Job` is still alive as per contract in `push_back`
        let job = unsafe { val.as_ref() };
        job.fut.set(Some(Box::leak(Box::new(Future::default())).into()));
        Some(job.clone())
    }
}
