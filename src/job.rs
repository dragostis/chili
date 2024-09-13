use std::{
    cell::{Cell, UnsafeCell},
    hint,
    mem::ManuallyDrop,
    panic::{self, AssertUnwindSafe},
    ptr::{self, NonNull},
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
                    // Lock is acquired, only we are accessing `self.waiting_thread`.
                    unsafe { *self.waiting_thread.get() = Some(thread::current()) };

                    self.state.store(Poll::Pending as u8, Ordering::Release);

                    thread::park();

                    // Skip spininning after being woken up.
                    continue;
                }
                Err(state) if state == Poll::Ready as u8 => {
                    // `state` is `Poll::Ready` only after after
                    // `Self::complete` releases the lock.
                    //
                    // Calling `Self::complete` when `state` is `Poll::Ready`
                    // cannot mutate `self.val`.
                    break unsafe { (*self.val.get()).take().map(|b| *b) };
                }
                _ => (),
            }

            // Spinning time should be short since the lock is held only to
            // write the `Box<T>` and unpark this thread.
            hint::spin_loop();
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
                // Spinning time should be short since the lock is held only to
                // write the thread to `self.waiting_thread`.
                Err(_) => hint::spin_loop(),
            }
        }

        // Lock is acquired, only we are accessing `self.val`.
        unsafe {
            *self.val.get() = Some(val);
        }

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

    /// It should only be called once.
    pub unsafe fn take_once(self) -> F {
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
    stack: *const JobStack,
    harness: unsafe fn(&mut Scope<'_>, *const JobStack, *const Future),
    prev: Cell<Option<NonNull<Self>>>,
    fut_or_next: Cell<Option<NonNull<Future<T>>>>,
}

impl<T> Job<T> {
    pub fn new<F>(stack: &JobStack<F>) -> Self
    where
        F: FnOnce(&mut Scope<'_>) -> T + Send,
        T: Send,
    {
        /// It should only be called while the `stack` is still alive.
        unsafe fn harness<F, T>(scope: &mut Scope<'_>, stack: *const JobStack, fut: *const Future)
        where
            F: FnOnce(&mut Scope<'_>) -> T + Send,
            T: Send,
        {
            // The `stack` is still alive.
            let stack = unsafe { &*(stack as *const JobStack<F>) };
            // This is the first call to `take` the closure since
            // `Job::execute` is called only after the job has been popped.
            let f = unsafe { ManuallyDrop::take(&mut *stack.f.get()) };
            // Before being popped, the `JobQueue` allocates and store a
            // `Future` in `self.fur_or_next` that should get passed here.
            let fut = unsafe { &*(fut as *const Future<T>) };

            fut.complete(panic::catch_unwind(AssertUnwindSafe(|| f(scope))));
        }

        Self {
            stack: stack as *const JobStack<F> as *const JobStack,
            harness: harness::<F, T>,
            prev: Cell::new(None),
            fut_or_next: Cell::new(None),
        }
    }

    pub fn is_in_queue(&self) -> bool {
        self.prev.get().is_some()
    }

    pub fn eq(&self, other: &Job) -> bool {
        self.stack == other.stack
    }

    /// It should only be called after being popped from a `JobQueue`.
    pub unsafe fn poll(&self) -> bool {
        self.fut_or_next
            .get()
            .map(|fut| {
                // Before being popped, the `JobQueue` allocates and store a
                // `Future` in `self.fur_or_next` that should get passed here.
                let fut = unsafe { fut.as_ref() };
                fut.poll()
            })
            .unwrap_or_default()
    }

    /// It should only be called after being popped from a `JobQueue`.
    pub unsafe fn wait(&self) -> Option<thread::Result<T>> {
        self.fut_or_next.get().and_then(|fut| {
            // Before being popped, the `JobQueue` allocates and stores a
            // `Future` in `self.fur_or_next` that should get passed here.
            let result = unsafe { (*fut.as_ptr()).wait() };
            // We only can drop the `Box` *after* waiting on the `Future`
            // in order to ensure unique access.
            unsafe {
                drop(Box::from_raw(fut.as_ptr()));
            }

            result
        })
    }

    /// It should only be called in the case where the job has been popped
    /// from the front and will not be `Job::Wait`ed.
    pub unsafe fn drop(&self) {
        if let Some(fut) = self.fut_or_next.get() {
            // Before being popped, the `JobQueue` allocates and store a
            // `Future` in `self.fur_or_next` that should get passed here.
            unsafe {
                drop(Box::from_raw(fut.as_ptr()));
            }
        }
    }
}

impl Job {
    /// It should only be called while the `JobStack` it was created with is
    /// still alive and after being popped from a `JobQueue`.
    pub unsafe fn execute(&self, scope: &mut Scope<'_>) {
        // Before being popped, the `JobQueue` allocates and store a
        // `Future` in `self.fur_or_next` that should get passed here.
        unsafe {
            (self.harness)(scope, self.stack, self.fut_or_next.get().unwrap().as_ptr());
        }
    }
}

unsafe impl Send for Job {}

#[derive(Debug)]
pub struct JobQueue {
    sentinel: NonNull<Job>,
    tail: NonNull<Job>,
}

impl Default for JobQueue {
    fn default() -> Self {
        let root = Box::leak(Box::new(Job {
            stack: ptr::null(),
            harness: |_, _, _| (),
            prev: Cell::new(None),
            fut_or_next: Cell::new(None),
        }))
        .into();

        Self {
            sentinel: root,
            tail: root,
        }
    }
}

impl Drop for JobQueue {
    fn drop(&mut self) {
        // `self.sentinel` never gets written over, so it contains the original
        // `leak`ed `Box` that gets allocated in `JobQueue::default`.
        unsafe {
            drop(Box::from_raw(self.sentinel.as_ptr()));
        }
    }
}

impl JobQueue {
    /// Any `Job` pushed onto the queue should alive at least until it gets
    /// popped.
    pub unsafe fn push_back<T>(&mut self, job: &Job<T>) {
        // The tail can either be the root `Box::leak`ed in the default
        // constructor or a `Job` that has been pushed previously and which is
        // still alive.
        let current_tail = unsafe { self.tail.as_ref() };
        // This effectively casts the `Job`'s `fut_or_next` from `Future<T>` to
        // `Future<()>` which casts the `Future`'s `Box<T>` to a `Box<()>`.
        //
        // This box will not be access until the pointer gets passed in the
        // `harness` where it gets cast back to `T`.
        let next_tail = unsafe { &*(job as *const Job<T> as *const Job) };

        current_tail
            .fut_or_next
            .set(Some(NonNull::from(next_tail).cast()));
        next_tail.prev.set(Some(current_tail.into()));

        self.tail = next_tail.into();
    }

    /// The last `Job` in the queue must still be alive.
    pub unsafe fn pop_back(&mut self) {
        // The tail can either be the root `Box::leak`ed in the default
        // constructor or a `Job` that has been pushed previously and which is
        // still alive.
        let current_tail = unsafe { self.tail.as_ref() };
        if let Some(prev_tail) = current_tail.prev.get() {
            // `Job`'s `prev` pointer can only be set by `JobQueue::push_back`
            // to the previous tail which should still be alive or by
            // `JobQueue::pop_front` when it's set to `self.sentinel` which is
            // alive for the entirety of `self`.
            let prev_tail = unsafe { prev_tail.as_ref() };

            current_tail.prev.set(None);
            prev_tail.fut_or_next.set(None);

            self.tail = prev_tail.into();
        }
    }

    /// The first `Job` in the queue must still be alive.
    pub unsafe fn pop_front(&mut self) -> Option<Job> {
        // `self.sentinel` is alive for the entirety of `self`.
        let sentinel = unsafe { self.sentinel.as_ref() };

        sentinel.fut_or_next.get().map(|next| {
            // `self.sentinel`'s `fut_or_next` pointer can only be set by
            // `JobQueue::push_back` or by `JobQueue::pop_front` when it's set
            // to a job that was previous set by `JobQueue::push_back` and
            // should still be alive.
            let head: &Job = unsafe { next.cast().as_ref() };

            if let Some(next) = head.fut_or_next.get() {
                sentinel.fut_or_next.set(Some(next.cast()));

                // `Job`'s `fut_or_next` pointer can only be set by
                // `JobQueue::push_back` or by `JobQueue::pop_front` when it's set
                // to a job that was previous set by `JobQueue::push_back` and
                // should still be alive.
                //
                // It can also be set to a `Future`, but that can only happen after
                // the job was removed from the queue.
                let next: &Job = unsafe { next.cast().as_ref() };
                next.prev.set(Some(sentinel.into()));
            } else {
                sentinel.fut_or_next.set(None);
                self.tail = sentinel.into();
            }

            // `self.sentinel`'s `fut_or_next` pointer can only be set by
            // `JobQueue::push_back` or by `JobQueue::pop_front` when it's set
            // to a job that was previous set by `JobQueue::push_back` and
            // should still be alive.
            let head: &Job<Future> = unsafe { next.cast().as_ref() };

            head.prev.set(None);
            head.fut_or_next
                .set(Some(Box::leak(Box::new(Future::default())).into()));

            // `self.sentinel`'s `fut_or_next` pointer can only be set by
            // `JobQueue::push_back` or by `JobQueue::pop_front` when it's set
            // to a job that was previous set by `JobQueue::push_back` and
            // should still be alive.
            unsafe { next.cast::<Job>().as_ref().clone() }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Job {
        pub fn from_usize(val: &'static usize) -> Self {
            Self {
                stack: val as *const usize as *mut JobStack as *const JobStack,
                harness: |_, _, _| (),
                prev: Cell::new(None),
                fut_or_next: Cell::new(None),
            }
        }

        pub fn as_usize(&self) -> usize {
            unsafe { *(self.stack as *const usize) }
        }
    }

    #[test]
    fn push_pop_back() {
        let mut queue = JobQueue::default();

        assert_eq!(queue.sentinel, queue.tail);

        let job1 = Job::from_usize(&1);

        unsafe {
            queue.push_back(&job1);
        }
        assert_eq!(unsafe { queue.tail.as_ref().as_usize() }, 1);

        unsafe {
            queue.pop_back();
        }
        assert_eq!(queue.sentinel, queue.tail);
    }

    #[test]
    fn push2_pop2_back() {
        let mut queue = JobQueue::default();

        assert_eq!(queue.sentinel, queue.tail);

        let job1 = Job::from_usize(&1);
        let job2 = Job::from_usize(&2);

        unsafe {
            queue.push_back(&job1);
        }
        assert_eq!(unsafe { queue.tail.as_ref().as_usize() }, 1);

        unsafe {
            queue.push_back(&job2);
        }
        assert_eq!(unsafe { queue.tail.as_ref().as_usize() }, 2);

        unsafe {
            queue.pop_back();
        }
        assert_eq!(unsafe { queue.tail.as_ref().as_usize() }, 1);

        unsafe {
            queue.pop_back();
        }
        assert_eq!(queue.sentinel, queue.tail);
    }

    #[test]
    fn push_pop_front() {
        let mut queue = JobQueue::default();

        assert_eq!(queue.sentinel, queue.tail);

        let job1 = Job::from_usize(&1);

        unsafe {
            queue.push_back(&job1);
        }
        assert_eq!(unsafe { queue.tail.as_ref().as_usize() }, 1);

        let job = unsafe { queue.pop_front().unwrap() };
        assert_eq!(job.as_usize(), 1);
        assert!(job.prev.get().is_none());
        assert!(job.fut_or_next.get().is_some());

        unsafe {
            job.drop();
        }

        assert_eq!(queue.sentinel, queue.tail);
    }

    #[test]
    fn push2_pop2_front() {
        let mut queue = JobQueue::default();

        assert_eq!(queue.sentinel, queue.tail);

        let job1 = Job::from_usize(&1);
        let job2 = Job::from_usize(&2);

        unsafe {
            queue.push_back(&job1);
        }
        assert_eq!(unsafe { queue.tail.as_ref().as_usize() }, 1);

        unsafe {
            queue.push_back(&job2);
        }
        assert_eq!(unsafe { queue.tail.as_ref().as_usize() }, 2);

        let job = unsafe { queue.pop_front().unwrap() };
        assert_eq!(job.as_usize(), 1);
        assert!(job.prev.get().is_none());
        assert!(job.fut_or_next.get().is_some());

        unsafe {
            job.drop();
        }

        let job = unsafe { queue.pop_front().unwrap() };
        assert_eq!(job.as_usize(), 2);
        assert!(job.prev.get().is_none());
        assert!(job.fut_or_next.get().is_some());

        unsafe {
            job.drop();
        }

        assert_eq!(queue.sentinel, queue.tail);
    }
}
