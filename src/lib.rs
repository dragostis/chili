#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

//! A crate for very low-overhead fork-join workloads that can potentially be
//! run in parallel.
//!
//! # Examples
//!
//! ```
//! # use spice::{Scope, ThreadPool};
//! struct Node {
//!     val: u64,
//!     left: Option<Box<Node>>,
//!     right: Option<Box<Node>>,
//! }
//!
//! impl Node {
//!     pub fn tree(layers: usize) -> Self {
//!         Self {
//!             val: 1,
//!             left: (layers != 1).then(|| Box::new(Self::tree(layers - 1))),
//!             right: (layers != 1).then(|| Box::new(Self::tree(layers - 1))),
//!         }
//!     }
//! }
//!
//! fn sum(node: &Node, scope: &mut Scope<'_>) -> u64 {
//!     let (left, right) = scope.join(
//!         |s| node.left.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
//!         |s| node.right.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
//!     );
//!
//!     node.val + left + right
//! }
//!
//! let tree = Node::tree(10);
//!
//! let mut thread_pool = ThreadPool::new().unwrap();
//! let mut scope = thread_pool.scope();
//!
//! assert_eq!(sum(&tree, &mut scope), 1023);
//! ```

use std::{
    collections::{btree_map::Entry, BTreeMap},
    num::NonZero,
    ops::{Deref, DerefMut},
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

mod job;

use job::{Job, JobQueue, JobStack};

#[derive(Debug, Default)]
struct LockContext {
    time: u64,
    is_stopping: bool,
    shared_jobs: BTreeMap<usize, (u64, Job)>,
}

impl LockContext {
    pub fn pop_earliest_shared_job(&mut self) -> Option<Job> {
        self.shared_jobs
            .pop_first()
            .map(|(_, (_, shared_job))| shared_job)
    }
}

#[derive(Debug)]
struct Context {
    lock: Mutex<LockContext>,
    job_is_ready: Condvar,
    heartbeats: Vec<AtomicBool>,
}

fn execute_worker(context: Arc<Context>, worker_index: usize, barrier: Arc<Barrier>) -> Option<()> {
    let mut first_run = true;

    let mut job_queue = JobQueue::default();
    loop {
        let job = {
            let mut lock = context.lock.lock().unwrap();
            lock.pop_earliest_shared_job()
        };

        if let Some(job) = job {
            // Any `Job` that was shared between threads is waited upon before
            // the `JobStack` exits scope.
            unsafe {
                job.execute(&mut Scope::new_from_worker(
                    context.clone(),
                    worker_index,
                    &mut job_queue,
                ));
            }
        }

        if first_run {
            first_run = false;
            barrier.wait();
        };

        let lock = context.lock.lock().ok()?;
        if lock.is_stopping || context.job_is_ready.wait(lock).is_err() {
            break;
        }
    }

    Some(())
}

fn execute_heartbeat(context: Arc<Context>, heartbeat_interval: Duration) -> Option<()> {
    let interval_between_workers = heartbeat_interval / context.heartbeats.len() as u32;

    let mut i = 0;
    loop {
        if context.lock.lock().ok()?.is_stopping {
            break;
        }

        context.heartbeats.get(i)?.store(true, Ordering::Relaxed);

        i = (i + 1) % context.heartbeats.len();

        thread::sleep(interval_between_workers);
    }

    Some(())
}

#[derive(Debug)]
enum ThreadJobQueue<'s> {
    Worker(&'s mut JobQueue),
    Current(JobQueue),
}

impl Deref for ThreadJobQueue<'_> {
    type Target = JobQueue;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Worker(queue) => queue,
            Self::Current(queue) => queue,
        }
    }
}

impl DerefMut for ThreadJobQueue<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Worker(queue) => queue,
            Self::Current(queue) => queue,
        }
    }
}

/// A `Scope`d object that you can run fork-join workloads on.
///
/// # Examples
///
/// ```
/// # use spice::ThreadPool;
/// let mut tp = ThreadPool::new().unwrap();
/// let mut s = tp.scope();
///
/// let mut vals = [0; 2];
/// let (left, right) = vals.split_at_mut(1);
///
/// s.join(|_|left[0] = 1, |_| right[0] = 1);
///
/// assert_eq!(vals, [1; 2]);
/// ```
#[derive(Debug)]
pub struct Scope<'s> {
    context: Arc<Context>,
    worker_index: usize,
    job_queue: ThreadJobQueue<'s>,
}

impl<'s> Scope<'s> {
    fn new_from_thread_pool(thread_pool: &'s mut ThreadPool) -> Self {
        let worker_index = thread_pool.context.heartbeats.len() - 1;

        thread_pool.context.heartbeats[worker_index].store(true, Ordering::Relaxed);

        Self {
            context: thread_pool.context.clone(),
            worker_index,
            job_queue: ThreadJobQueue::Current(JobQueue::default()),
        }
    }

    fn new_from_worker(
        context: Arc<Context>,
        worker_index: usize,
        job_queue: &'s mut JobQueue,
    ) -> Self {
        Self {
            context,
            worker_index,
            job_queue: ThreadJobQueue::Worker(job_queue),
        }
    }

    fn wait_for_sent_job<T>(&mut self, job: &Job<T>) -> Option<thread::Result<T>> {
        {
            let mut lock = self.context.lock.lock().unwrap();
            if lock
                .shared_jobs
                .get(&self.worker_index)
                .map(|(_, shared_job)| job.eq(shared_job))
                .is_some()
            {
                if let Some((_, job)) = lock.shared_jobs.remove(&self.worker_index) {
                    // Since the `Future` has already been allocated when
                    // popping from the queue, the `Job` needs manual dropping.
                    unsafe {
                        job.drop();
                    }
                }

                return None;
            }
        }

        // For this `Job` to have crossed thread borders, it must have been
        // popped from the `JobQueue` and shared.
        while !unsafe { job.poll() } {
            let job = {
                let mut lock = self.context.lock.lock().unwrap();
                lock.pop_earliest_shared_job()
            };

            if let Some(job) = job {
                // Any `Job` that was shared between threads is waited upon
                // before the `JobStack` exits scope.
                unsafe {
                    job.execute(self);
                }
            } else {
                break;
            }
        }

        // Any `Job` that was shared between threads is waited upon before the
        // `JobStack` exits scope.
        unsafe { job.wait() }
    }

    #[cold]
    fn heartbeat(&mut self) {
        let mut lock = self.context.lock.lock().unwrap();

        let time = lock.time;
        if let Entry::Vacant(e) = lock.shared_jobs.entry(self.worker_index) {
            // Any `Job` previously pushed onto the queue will be waited upon
            // and will be alive until that point.
            if let Some(job) = unsafe { self.job_queue.pop_front() } {
                e.insert((time, job));

                lock.time += 1;
                self.context.job_is_ready.notify_one();
            }
        }

        self.context.heartbeats[self.worker_index].store(false, Ordering::Relaxed);
    }

    /// Runs `a` and `b` potentially in parallel on separate threads and
    /// returns the results.
    ///
    /// # Examples
    ///
    /// ```
    /// # use spice::ThreadPool;
    /// let mut tp = ThreadPool::new().unwrap();
    /// let mut s = tp.scope();
    ///
    /// let mut vals = [0; 2];
    /// let (left, right) = vals.split_at_mut(1);
    ///
    /// s.join(|_|left[0] = 1, |_| right[0] = 1);
    ///
    /// assert_eq!(vals, [1; 2]);
    /// ```
    pub fn join<A, B, RA, RB>(&mut self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce(&mut Scope<'_>) -> RA + Send,
        B: FnOnce(&mut Scope<'_>) -> RB + Send,
        RA: Send,
        RB: Send,
    {
        let a = move |scope: &mut Scope<'_>| {
            if scope.context.heartbeats[scope.worker_index].load(Ordering::Relaxed) {
                scope.heartbeat();
            }

            a(scope)
        };

        let stack = JobStack::new(a);
        let job = Job::new(&stack);

        // `job` is alive until the end of this scope.
        unsafe {
            self.job_queue.push_back(&job);
        }

        let rb = b(self);

        if job.is_in_queue() {
            // `job` is alive until the end of this scope and there has been no
            // other pop up to this point.
            unsafe {
                self.job_queue.pop_back();
            }

            // Since the `job` was popped from the back of the queue, it cannot
            // take the closure out of the `JobStack` anymore.
            // `JobStack::take_once` is thus called only once.
            (unsafe { (stack.take_once())(self) }, rb)
        } else {
            let ra = match self.wait_for_sent_job(&job) {
                Some(Ok(val)) => val,
                Some(Err(e)) => panic::resume_unwind(e),
                // Since the `job` didn't have the chance to be actually
                // sent across threads, it cannot take the closure out of the
                // `JobStack` anymore. `JobStack::take_once` is thus called
                // only once.
                None => unsafe { (stack.take_once())(self) },
            };

            (ra, rb)
        }
    }
}

/// `ThreadPool` configuration.
#[derive(Debug)]
pub struct Config {
    /// The number of threads or `None` to use
    /// `std::thread::available_parallelism`.
    pub thread_count: Option<usize>,
    /// The interaval between heartbeats on any particular thread.
    pub heartbeat_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            thread_count: None,
            heartbeat_interval: Duration::from_micros(100),
        }
    }
}

/// A thread pool for running fork-join workloads.
#[derive(Debug)]
pub struct ThreadPool {
    context: Arc<Context>,
    worker_handles: Vec<JoinHandle<()>>,
    heartbeat_handle: Option<JoinHandle<()>>,
}

impl ThreadPool {
    /// Crates a new thread pool with default `Config`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use spice::ThreadPool;
    /// let _tp = ThreadPool::new().unwrap();
    /// ```
    pub fn new() -> Option<Self> {
        Self::with_config(Config::default())
    }

    /// Creates a new thread pool with `config`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use spice::{Config, ThreadPool};
    /// let _tp = ThreadPool::with_config(Config {
    ///     thread_count: Some(1),
    ///     heartbeat_interval: Duration::from_micros(50),
    /// }).unwrap();
    /// ```
    pub fn with_config(config: Config) -> Option<Self> {
        let thread_count = config.thread_count.or_else(|| {
            thread::available_parallelism()
                .ok()
                .map(NonZero::<usize>::get)
        })? - 1;
        let worker_barrier = Arc::new(Barrier::new(thread_count + 1));

        let context = Arc::new(Context {
            lock: Mutex::new(LockContext::default()),
            job_is_ready: Condvar::new(),
            heartbeats: (0..=thread_count).map(|_| AtomicBool::new(true)).collect(),
        });

        let worker_handles = (0..thread_count)
            .map(|i| {
                let context = context.clone();
                let barrier = worker_barrier.clone();
                thread::spawn(move || {
                    execute_worker(context, i, barrier);
                })
            })
            .collect();

        worker_barrier.wait();

        Some(Self {
            context: context.clone(),
            worker_handles,
            heartbeat_handle: Some(thread::spawn(move || {
                execute_heartbeat(context, config.heartbeat_interval);
            })),
        })
    }

    /// Returns a `Scope`d object that you can run fork-join workloads on.
    ///
    /// # Examples
    ///
    /// ```
    /// # use spice::ThreadPool;
    /// let mut tp = ThreadPool::new().unwrap();
    /// let mut s = tp.scope();
    ///
    /// let mut vals = [0; 2];
    /// let (left, right) = vals.split_at_mut(1);
    ///
    /// s.join(|_|left[0] = 1, |_| right[0] = 1);
    ///
    /// assert_eq!(vals, [1; 2]);
    /// ```
    pub fn scope(&mut self) -> Scope<'_> {
        Scope::new_from_thread_pool(self)
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.context
            .lock
            .lock()
            .expect("locking failed")
            .is_stopping = true;
        self.context.job_is_ready.notify_all();

        for handle in self.worker_handles.drain(..) {
            handle.join().unwrap();
        }

        if let Some(handle) = self.heartbeat_handle.take() {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use thread::ThreadId;

    #[test]
    fn thread_pool_stops() {
        let _tp = ThreadPool::new();
    }

    #[test]
    fn join_basic() {
        let mut threat_pool = ThreadPool::new().unwrap();
        let mut scope = threat_pool.scope();

        let mut a = 0;
        let mut b = 0;
        scope.join(|_| a += 1, |_| b += 1);

        assert_eq!(a, 1);
        assert_eq!(b, 1);
    }

    #[test]
    fn join_long() {
        let mut threat_pool = ThreadPool::new().unwrap();

        fn increment(s: &mut Scope, slice: &mut [u32]) {
            match slice.len() {
                0 => (),
                1 => slice[0] += 1,
                _ => {
                    let (head, tail) = slice.split_at_mut(1);

                    s.join(|_| head[0] += 1, |s| increment(s, tail));
                }
            }
        }

        let mut vals = [0; 1_024];

        increment(&mut threat_pool.scope(), &mut vals);

        assert_eq!(vals, [1; 1_024]);
    }

    #[test]
    fn join_very_long() {
        let mut threat_pool = ThreadPool::new().unwrap();

        fn increment(s: &mut Scope, slice: &mut [u32]) {
            match slice.len() {
                0 => (),
                1 => slice[0] += 1,
                _ => {
                    let mid = slice.len() / 2;
                    let (left, right) = slice.split_at_mut(mid);

                    s.join(|s| increment(s, left), |s| increment(s, right));
                }
            }
        }

        let mut vals = vec![0; 1_024 * 1_024];

        increment(&mut threat_pool.scope(), &mut vals);

        assert_eq!(vals, vec![1; 1_024 * 1_024]);
    }

    #[test]
    fn join_wait() {
        let mut threat_pool = ThreadPool::with_config(Config {
            thread_count: Some(2),
            heartbeat_interval: Duration::from_micros(1),
            ..Default::default()
        })
        .unwrap();

        fn increment(s: &mut Scope, slice: &mut [u32]) {
            match slice.len() {
                0 => (),
                1 => slice[0] += 1,
                _ => {
                    let (head, tail) = slice.split_at_mut(1);

                    s.join(
                        |_| {
                            thread::sleep(Duration::from_micros(10));
                            head[0] += 1;
                        },
                        |s| increment(s, tail),
                    );
                }
            }
        }

        let mut vals = [0; 10];

        increment(&mut threat_pool.scope(), &mut vals);

        assert_eq!(vals, [1; 10]);
    }

    #[test]
    #[should_panic(expected = "panicked across threads")]
    fn join_panic() {
        let mut threat_pool = ThreadPool::with_config(Config {
            thread_count: Some(2),
            heartbeat_interval: Duration::from_micros(1),
        })
        .unwrap_or_else(|| {
            // Pass test artificially when only one thread is available.
            panic!("panicked across threads");
        });

        fn increment(s: &mut Scope, slice: &mut [u32], id: ThreadId) {
            match slice.len() {
                0 => (),
                1 => slice[0] += 1,
                _ => {
                    let (_, tail) = slice.split_at_mut(1);

                    s.join(
                        |_| {
                            thread::sleep(Duration::from_micros(10));

                            if thread::current().id() != id {
                                panic!("panicked across threads");
                            }
                        },
                        |s| increment(s, tail, id),
                    );
                }
            }
        }

        let mut vals = [0; 10];

        increment(&mut threat_pool.scope(), &mut vals, thread::current().id());
    }
}
