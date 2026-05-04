# Graph Report - stric  (2026-05-04)

## Corpus Check
- 18 files · ~7,120 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 153 nodes · 194 edges · 17 communities detected
- Extraction: 87% EXTRACTED · 13% INFERRED · 0% AMBIGUOUS · INFERRED: 25 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 20|Community 20]]

## God Nodes (most connected - your core abstractions)
1. `setup_crypto()` - 11 edges
2. `ConnectionManager<ConnectionMetadata>` - 7 edges
3. `BiStream` - 7 edges
4. `ServerInstance<ConnectionMetadata>` - 6 edges
5. `ProstCodec<Req, Res>` - 6 edges
6. `SerdeCodec<Req, Res, Format>` - 6 edges
7. `PoolManager` - 5 edges
8. `ServerUniStream` - 5 edges
9. `ClientUniStream` - 5 edges
10. `test_server_connection_lifecycle()` - 5 edges

## Surprising Connections (you probably didn't know these)
- `test_server_connection_lifecycle()` --calls--> `setup_crypto()`  [EXTRACTED]
  stric-core\tests\integration_test.rs → stric-tower\tests\integration_test.rs
- `test_connection_manager_updates()` --calls--> `setup_crypto()`  [EXTRACTED]
  stric-core\tests\integration_test.rs → stric-tower\tests\integration_test.rs
- `test_error_channel_and_handler_failure()` --calls--> `setup_crypto()`  [EXTRACTED]
  stric-core\tests\integration_test.rs → stric-tower\tests\integration_test.rs
- `test_custom_metadata()` --calls--> `setup_crypto()`  [EXTRACTED]
  stric-core\tests\integration_test.rs → stric-tower\tests\integration_test.rs
- `test_keep_alive_ping()` --calls--> `setup_crypto()`  [EXTRACTED]
  stric-core\tests\integration_test.rs → stric-tower\tests\integration_test.rs

## Communities

### Community 0 - "Community 0"
Cohesion: 0.11
Nodes (10): BincodeFormat, ProstCodec, ProstCodec<Req, Res>, read_length_prefixed(), SerdeCodec, SerdeCodec<Req, Res, Format>, SerdeFormat, ServiceCodec (+2 more)

### Community 1 - "Community 1"
Cohesion: 0.17
Nodes (17): EchoRequest, EchoResponse, EchoService, MyMetadata, ProstEchoRequest, ProstEchoResponse, ProstEchoService, setup_crypto() (+9 more)

### Community 2 - "Community 2"
Cohesion: 0.15
Nodes (3): BiStream, ClientUniStream, ServerUniStream

### Community 3 - "Community 3"
Cohesion: 0.18
Nodes (7): KeepAlivePool, KeepAliveWorker, ManagedStream, PoolCommand, PoolManager, WorkerCommand, WorkerHandle

### Community 4 - "Community 4"
Cohesion: 0.17
Nodes (5): EchoRequest, EchoResponse, JsonFormat, main(), SkipServerVerification

### Community 5 - "Community 5"
Cohesion: 0.2
Nodes (5): EchoRequest, EchoResponse, EchoService, JsonFormat, main()

### Community 6 - "Community 6"
Cohesion: 0.29
Nodes (1): ConnectionManager<ConnectionMetadata>

### Community 7 - "Community 7"
Cohesion: 0.4
Nodes (1): ServerInstance<ConnectionMetadata>

### Community 8 - "Community 8"
Cohesion: 0.4
Nodes (3): handle_stream(), TowerConnectionHandler, TowerConnectionHandler<S, C, Req, Res>

### Community 9 - "Community 9"
Cohesion: 0.5
Nodes (1): TowerClientService<C, Req, Res>

### Community 10 - "Community 10"
Cohesion: 0.67
Nodes (2): ConnectionManager, ConnectionManagerError

### Community 11 - "Community 11"
Cohesion: 0.67
Nodes (2): ConnectionContext, ConnectionWrapper

### Community 12 - "Community 12"
Cohesion: 0.67
Nodes (2): ServerError, ServerInstance

### Community 14 - "Community 14"
Cohesion: 1.0
Nodes (1): ServerConfig

### Community 15 - "Community 15"
Cohesion: 1.0
Nodes (1): TowerClientService

### Community 16 - "Community 16"
Cohesion: 1.0
Nodes (1): TowerError

### Community 20 - "Community 20"
Cohesion: 1.0
Nodes (1): Graphify Maintenance Rules

## Knowledge Gaps
- **28 isolated node(s):** `ConnectionManager`, `ConnectionManagerError`, `ConnectionWrapper`, `ConnectionContext`, `PoolCommand` (+23 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 6`** (7 nodes): `ConnectionManager<ConnectionMetadata>`, `.add_connection()`, `.set_client_bi()`, `.set_client_uni()`, `.set_keep_alive()`, `.set_server_bi()`, `.set_server_uni()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 7`** (6 nodes): `ServerInstance<ConnectionMetadata>`, `.get_bistream()`, `.get_unistream()`, `.handle_incoming()`, `.listen_connections()`, `.register_connection_handler()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 9`** (4 nodes): `TowerClientService<C, Req, Res>`, `.call()`, `.new()`, `.poll_ready()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 10`** (3 nodes): `ConnectionManager`, `ConnectionManagerError`, `connection.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 11`** (3 nodes): `ConnectionContext`, `ConnectionWrapper`, `connection_wrapper.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 12`** (3 nodes): `ServerError`, `ServerInstance`, `server.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 14`** (2 nodes): `ServerConfig`, `server_config.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 15`** (2 nodes): `TowerClientService`, `client.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 16`** (2 nodes): `TowerError`, `error.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 20`** (1 nodes): `Graphify Maintenance Rules`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `JsonFormat` connect `Community 0` to `Community 1`?**
  _High betweenness centrality (0.186) - this node is a cross-community bridge._
- **Why does `main()` connect `Community 4` to `Community 1`?**
  _High betweenness centrality (0.093) - this node is a cross-community bridge._
- **Why does `main()` connect `Community 5` to `Community 1`?**
  _High betweenness centrality (0.078) - this node is a cross-community bridge._
- **What connects `ConnectionManager`, `ConnectionManagerError`, `ConnectionWrapper` to the rest of the system?**
  _28 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.11 - nodes in this community are weakly interconnected._