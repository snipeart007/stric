# RFC 2026-0002: stric-core Automated Heartbeat and Keep-Alive System

## 1. Objective
This RFC specifies the design and behavior of the automated, thread-pool-based connection keep-alive/heartbeat system in `stric-core`. It manages connection-level health checks, keeps QUIC connections active across NAT routers, and dynamically scales/consolidates worker tasks to optimize CPU resource allocation.

---

## 2. Core Architecture

```
                    ┌────────────────────────┐
                    │    ConnectionManager   │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │     KeepAlivePool      │
                    └───────────┬────────────┘
                                │
                 ┌──────────────┴──────────────┐
                 ▼                             ▼
       ┌──────────────────┐          ┌──────────────────┐
       │ KeepAliveWorker  │          │ KeepAliveWorker  │
       │ (Active Task 1)  │          │ (Active Task 2)  │
       └──────────────────┘          └──────────────────┘
```

### 2.1. Ping Strategy
To prevent QUIC connections from timing out or being closed by intermediate firewall NAT tables, a persistent unidirectional stream (`ServerUniStream`) is dedicated solely to keep-alive pings.
* **Frequency:** Every 5 seconds (which is half of the default 10-second idle timeout specified by Quinn).
* **Payload:** A standard byte sequence (`b"ping"`).
* **Failure Actions:** If writing a ping returns a write error (e.g. peer closed connection), the connection is declared dead and removed from the active connection registry.

---

## 3. Dynamic Scaling and Consolidation

### 3.1. Thread-Pool Configuration
* **Limit (`keep_alive_limit_per_thread`):** The maximum number of active streams a single `KeepAliveWorker` task can manage. If 0, limits are disabled and all streams run under one worker.

### 3.2. Worker Tasks (`KeepAliveWorker`)
Each worker runs in a separate tokio background task. The pool tracks connection loads using an atomic counter (`Arc<AtomicUsize>`).
* **Scale-up:** When a new keep-alive connection is registered and all running workers are at capacity, the `KeepAlivePool` spawns a new `KeepAliveWorker` task and binds the stream to it.
* **Scale-down (Consolidation):** If a worker's active connections drop below 50% capacity (i.e. `count < limit / 2`), it triggers a rebalancing process:
  1. The worker notifies the `KeepAlivePool` via an `mpsc` channel.
  2. The pool evaluates whether the worker's connections can fit into other running workers.
  3. If they fit, the pool commands the worker to drain its streams and transfer them to the target workers.
  4. Once drained, the underutilized worker task terminates cleanly.

---

## 4. Channels & Worker APIs

```rust
pub struct KeepAlivePool {
    workers: Vec<WorkerHandle>,
    limit: usize,
    tx_to_pool: tokio::sync::mpsc::Sender<WorkerEvent>,
}

pub struct KeepAliveWorker {
    id: usize,
    streams: HashMap<u64, ServerUniStream>,
    rx: tokio::sync::mpsc::Receiver<WorkerCommand>,
    tx_to_pool: tokio::sync::mpsc::Sender<WorkerEvent>,
    active_count: Arc<std::sync::atomic::AtomicUsize>,
}

pub enum WorkerCommand {
    AddStream(u64, ServerUniStream),
    DrainAndShutdown,
}

pub enum WorkerEvent {
    UnderCapacity(usize), // Worker ID
    ClosedConnection(u64),
}
```
* Custom connection management handles the routing of streams to threads asynchronously and guarantees graceful thread cleanup.
