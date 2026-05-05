use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

use crate::stream::ServerUniStream;

/// Commands sent to the Pool Manager.
enum PoolCommand {
    AddStream {
        stream: ServerUniStream,
        interval: Duration,
    },
    WorkerUnderloaded {
        worker_id: usize,
        streams: Vec<ManagedStream>,
    },
    WorkerDropped {
        worker_id: usize,
    },
}

/// Commands sent to a Worker.
#[allow(dead_code)]
enum WorkerCommand {
    AddStream(ManagedStream),
    Drain,
    Shutdown,
}

/// A stream managed by the keep-alive system.
pub(crate) struct ManagedStream {
    /// The unidirectional stream used for heartbeats.
    pub stream: ServerUniStream,
    /// The interval at which pings are sent.
    pub interval: Duration,
    /// The timestamp of the last ping sent.
    pub last_ping: Instant,
}

/// A handle to a worker thread/task.
struct WorkerHandle {
    sender: mpsc::Sender<WorkerCommand>,
    count: Arc<AtomicUsize>,
}

/// A pool for managing keep-alive heartbeat streams.
///
/// `KeepAlivePool` distributes managed streams across worker tasks to ensure
/// heartbeats are sent periodically and efficiently.
pub(crate) struct KeepAlivePool {
    sender: mpsc::Sender<PoolCommand>,
}

impl KeepAlivePool {
    /// Creates a new `KeepAlivePool`.
    ///
    /// # Arguments
    /// * `limit` - The maximum number of streams each worker can manage.
    pub(crate) fn new(limit: u64) -> Self {
        let (tx, rx) = mpsc::channel(100);
        let pool_sender = tx.clone();
        tokio::spawn(async move {
            let mut manager = PoolManager::new(limit, rx, pool_sender);
            manager.run().await;
        });
        Self { sender: tx }
    }

    /// Adds a new stream to the keep-alive pool.
    pub(crate) async fn add_stream(&self, stream: ServerUniStream, interval: Duration) {
        let _ = self
            .sender
            .send(PoolCommand::AddStream { stream, interval })
            .await;
    }
}

/// Manages the distribution of streams across workers.
struct PoolManager {
    limit: u64,
    receiver: mpsc::Receiver<PoolCommand>,
    pool_sender: mpsc::Sender<PoolCommand>,
    workers: Vec<(usize, WorkerHandle)>,
    next_worker_id: usize,
}

impl PoolManager {
    fn new(
        limit: u64,
        receiver: mpsc::Receiver<PoolCommand>,
        pool_sender: mpsc::Sender<PoolCommand>,
    ) -> Self {
        Self {
            limit,
            receiver,
            pool_sender,
            workers: Vec::new(),
            next_worker_id: 0,
        }
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.receiver.recv().await {
            match cmd {
                PoolCommand::AddStream {
                    stream,
                    interval,
                } => {
                    self.handle_add_stream(ManagedStream {
                        stream,
                        interval,
                        last_ping: Instant::now(),
                    })
                    .await;
                }
                PoolCommand::WorkerUnderloaded { worker_id, streams } => {
                    self.handle_underloaded(worker_id, streams).await;
                }
                PoolCommand::WorkerDropped { worker_id } => {
                    self.workers.retain(|(id, _)| *id != worker_id);
                }
            }
        }
    }

    async fn handle_add_stream(&mut self, mut stream: ManagedStream) {
        // Find a worker with capacity
        if let Some((_, worker)) = self.workers.iter().find(|(_, w)| {
            let count = w.count.load(Ordering::SeqCst);
            self.limit == 0 || (count as u64) < self.limit
        }) {
            match worker.sender.send(WorkerCommand::AddStream(stream)).await {
                Ok(_) => return,
                Err(mpsc::error::SendError(WorkerCommand::AddStream(s))) => {
                    stream = s;
                }
                _ => unreachable!(),
            }
        }

        // Spawn new worker
        let worker_id = self.next_worker_id;
        self.next_worker_id += 1;

        let (tx, rx) = mpsc::channel(100);
        let count = Arc::new(AtomicUsize::new(0));
        let handle = WorkerHandle {
            sender: tx,
            count: count.clone(),
        };

        let mut worker = KeepAliveWorker {
            id: worker_id,
            limit: self.limit,
            streams: vec![stream],
            receiver: rx,
            pool_sender: self.pool_sender.clone(),
            count,
            local_count: 1,
        };

        // Initialize count
        worker.count.store(1, Ordering::SeqCst);

        self.workers.push((worker_id, handle));
        tokio::spawn(async move {
            worker.run().await;
        });
    }

    async fn handle_underloaded(&mut self, worker_id: usize, streams: Vec<ManagedStream>) {
        // Try to redistribute these streams to other workers
        // This is only called if limit > 0 and count < limit / 2

        // Remove the underloaded worker from our list first to avoid sending back to it
        self.workers.retain(|(id, _)| *id != worker_id);

        for stream in streams {
            self.handle_add_stream(stream).await;
        }
    }
}

/// A worker task that manages a subset of keep-alive streams.
struct KeepAliveWorker {
    id: usize,
    limit: u64,
    streams: Vec<ManagedStream>,
    receiver: mpsc::Receiver<WorkerCommand>,
    pool_sender: mpsc::Sender<PoolCommand>,
    count: Arc<AtomicUsize>,
    local_count: usize,
}

impl KeepAliveWorker {
    fn update_counts(&mut self) {
        self.local_count = self.streams.len();
        self.count.store(self.local_count, Ordering::SeqCst);
    }

    async fn run(&mut self) {
        let mut interval = time::interval(Duration::from_millis(500));
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                cmd = self.receiver.recv() => {
                    match cmd {
                        Some(WorkerCommand::AddStream(s)) => {
                            self.streams.push(s);
                            self.update_counts();
                        }
                        Some(WorkerCommand::Drain) | Some(WorkerCommand::Shutdown) | None => {
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    let now = Instant::now();
                    let mut i = 0;
                    let mut changed = false;
                    while i < self.streams.len() {
                        let s = &mut self.streams[i];
                        if now.duration_since(s.last_ping) >= s.interval {
                            if s.stream.write(b"ping").await.is_err() {
                                self.streams.remove(i);
                                changed = true;
                                continue;
                            }
                            s.last_ping = now;
                        }
                        i += 1;
                    }

                    if changed {
                        self.update_counts();
                    }

                    // Check for underload rebalancing
                    if self.limit > 0 && self.local_count > 0 && (self.local_count as u64) < self.limit / 2 {
                        // Notify pool and exit
                        let streams = std::mem::take(&mut self.streams);
                        self.update_counts();
                        let _ = self.pool_sender.send(PoolCommand::WorkerUnderloaded {
                            worker_id: self.id,
                            streams,
                        }).await;
                        return;
                    }
                }
            }
        }

        let _ = self
            .pool_sender
            .send(PoolCommand::WorkerDropped { worker_id: self.id })
            .await;
    }
}
