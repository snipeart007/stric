# Keep-Alive System Implementation Plan

## Objective
Implement a thread-pool-based keep-alive ping system in `stric-core`. When a connection has `keep_alive` enabled, a dedicated tokio task periodically sends a `b"ping"` over a single, persistent `ServerUniStream` to keep the connection alive. The system will dynamically manage connection distribution among threads based on a configurable limit and consolidate threads when they fall below 50% capacity.

## Key Components
1. **Ping Strategy**: A persistent `ServerUniStream` is opened per keep-alive connection. Every 5 seconds (half of Quinn's default 10s idle timeout), `b"ping"` is written to the stream.
2. **`KeepAlivePool`**: Manages the collection of worker tasks (threads). Responsible for routing new connections to available workers, spinning up new workers when existing ones are full, and handling rebalancing.
3. **`KeepAliveWorker`**: A tokio task that loops based on the calculated dynamic ping interval. It iterates through its assigned connections, sends pings, and drops any connections that return errors (e.g., peer disconnected).
4. **Rebalancing Logic**: If `limit > 0` and a worker's active connection count drops below `limit / 2`, it will send a signal to the `KeepAlivePool`. The pool will instruct the worker to drain its remaining connections back to the pool to be distributed to other active workers, allowing the underutilized worker to shut down.

## Implementation Steps

1. **Create `src/keep_alive.rs`**:
   - Define `KeepAlivePool` and `KeepAliveWorker`.
   - Implement channels (`mpsc`) for communication between the Pool and Workers:
     - Pool -> Worker: Send new `ServerUniStream`s to manage.
     - Worker -> Pool: Notify when connection count drops below `< limit / 2`.
     - Pool -> Worker: Command to drain streams and shutdown.
   - Use `Arc<AtomicUsize>` for lightweight, shared tracking of each worker's active connection count.

2. **Update `ConnectionManager` (`src/connection.rs`)**:
   - Embed a `KeepAlivePool` within `ConnectionManager`.
   - Implement `set_keep_alive(&self, id: u64, val: bool)`:
     - If `val` is true, spawn a lightweight detached task that calls `conn.open_uni().await`, then adds the resulting stream to the `KeepAlivePool`.

3. **Update `ServerInstance` (`src/server.rs`)**:
   - Initialize `KeepAlivePool` using `config.keep_alive_limit_per_thread`.
   - In `handle_incoming`: If the `default_conn_context` indicates `keep_alive` is true, automatically spawn a task to open the persistent uni stream and add it to the pool (bypassing the need to manually call `set_keep_alive`).

4. **Testing**:
   - Update integration tests to verify the automatic initialization of keep-alive pings.
   - Test worker thread scale-up and consolidation (rebalancing) behavior by mocking stream closures.

## Verification
- Code compiles warning-free (`cargo clippy`).
- Integration tests confirm pings are reliably dispatched on idle connections without crashing or hanging the tokio executor.
