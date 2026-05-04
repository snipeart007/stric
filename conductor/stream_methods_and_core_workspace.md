# Stream Encapsulation and Workspace Migration Plan

## Objective
1. **Encapsulate `quinn` streams**: Prevent users of the framework from needing to interact directly with the underlying `quinn::SendStream` and `quinn::RecvStream` objects.
2. **Workspace Setup**: Move the existing codebase into a `stric-core` crate and create a new `stric` wrapper crate to set up a standard Cargo workspace at the project root.

## Scope & Impact
- `src/stream.rs` will be updated to encapsulate stream properties and expose specific `read`/`write` methods.
- `src/server.rs` will be updated to construct the modified stream structs.
- The entire project structure will change to a multi-crate workspace.

## Implementation Steps

### Phase 1: Stream Encapsulation
1. **Modify Struct Definitions in `src/stream.rs`**:
   - Make the `stream`, `recv_stream`, and `send_stream` fields private (or `pub(crate)` so `server.rs` can construct them).
2. **Implement Methods for `ServerUniStream`**:
   - `write(&mut self, buf: &[u8]) -> Result<usize, quinn::WriteError>`
   - `write_all(&mut self, buf: &[u8]) -> Result<(), quinn::WriteError>`
   - `write_chunk(&mut self, buf: bytes::Bytes) -> Result<(), quinn::WriteError>`
   - `finish(&mut self) -> Result<(), quinn::WriteError>`
   - `stopped(&mut self) -> Result<Option<usize>, quinn::StoppedError>`
3. **Implement Methods for `ClientUniStream`**:
   - `read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, quinn::ReadError>`
   - `read_exact(&mut self, buf: &mut [u8]) -> Result<(), quinn::ReadExactError>`
   - `read_to_end(&mut self, size_limit: usize) -> Result<Vec<u8>, quinn::ReadToEndError>`
   - `read_chunk(&mut self, max_length: usize, ordered: bool) -> Result<Option<quinn::Chunk>, quinn::ReadError>`
   - `stop(&mut self, error_code: quinn::VarInt) -> Result<(), quinn::UnknownStream>`
4. **Implement Methods for `BiStream`**:
   - Delegate both read and write methods (from above) to the underlying `recv_stream` and `send_stream`.
5. **Update Instantiations**:
   - Update `src/server.rs` where these structs are created (e.g., in `get_unistream` and `get_bistream`) to use new constructors or `pub(crate)` fields.

### Phase 2: Workspace Migration
1. **Create Root Workspace**:
   - Create a `Cargo.toml` at the root with `[workspace]` defining `members = ["stric-core", "stric"]`.
2. **Migrate to `stric-core`**:
   - Create `stric-core/` directory.
   - Move current `Cargo.toml` to `stric-core/Cargo.toml` and rename the package `name` to `"stric-core"`.
   - Move `src/` to `stric-core/src/`.
   - Move `tests/` to `stric-core/tests/` and update references from `stric::` to `stric_core::`.
3. **Create `stric` Wrapper**:
   - Create `stric/` directory.
   - Create `stric/Cargo.toml` defining the `stric` package and a path dependency on `stric-core = { path = "../stric-core" }`.
   - Create an empty or re-exporting `stric/src/lib.rs`.

## Verification
- Run `cargo fmt` and `cargo clippy --workspace`.
- Run `cargo test --workspace` to ensure the integration tests continue to pass with the new workspace structure and the modified stream structs.
