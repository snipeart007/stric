# Graph Report - stric  (2026-05-03)

## Corpus Check
- 8 files · ~1,652 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 61 nodes · 69 edges · 12 communities detected
- Extraction: 90% EXTRACTED · 10% INFERRED · 0% AMBIGUOUS · INFERRED: 7 edges (avg confidence: 0.83)
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
- [[_COMMUNITY_Community 13|Community 13]]

## God Nodes (most connected - your core abstractions)
1. `ServerInstance<ConnectionMetadata>` - 8 edges
2. `ConnectionManager<ConnectionMetadata>` - 7 edges
3. `setup_crypto()` - 5 edges
4. `ConnectionContext` - 3 edges
5. `add()` - 3 edges
6. `test_server_connection_lifecycle()` - 3 edges
7. `test_connection_manager_updates()` - 3 edges
8. `test_error_channel_and_handler_failure()` - 3 edges
9. `test_custom_metadata()` - 3 edges
10. `ServerInstance` - 3 edges

## Surprising Connections (you probably didn't know these)
- `it_works()` --calls--> `add()`  [EXTRACTED]
  lib.rs → src/lib.rs
- `ServerInstance` --references--> `ServerConfig`  [EXTRACTED]
  src/server.rs → src/server_config.rs
- `ServerConfig` --references--> `ConnectionContext`  [EXTRACTED]
  src/server_config.rs → src/connection_wrapper.rs
- `ConnectionManager` --shares_data_with--> `ConnectionWrapper`  [EXTRACTED]
  src/connection.rs → src/connection_wrapper.rs
- `ServerInstance` --references--> `ConnectionManager`  [EXTRACTED]
  src/server.rs → src/connection.rs

## Hyperedges (group relationships)
- **Stric Server Core Components** — server_serverinstance, connection_connectionmanager, server_config_serverconfig [INFERRED 0.95]
- **QUIC Stream Abstractions** — stream_serverunistream, stream_clientunistream, stream_bistream [INFERRED 0.95]

## Communities

### Community 0 - "Community 0"
Cohesion: 0.36
Nodes (1): ServerInstance<ConnectionMetadata>

### Community 1 - "Community 1"
Cohesion: 0.29
Nodes (1): ConnectionManager<ConnectionMetadata>

### Community 2 - "Community 2"
Cohesion: 0.5
Nodes (6): MyMetadata, setup_crypto(), test_connection_manager_updates(), test_custom_metadata(), test_error_channel_and_handler_failure(), test_server_connection_lifecycle()

### Community 3 - "Community 3"
Cohesion: 0.4
Nodes (6): ConnectionManager, ConnectionContext, ConnectionWrapper, ConnectionHandlerFn, ServerConfig, ServerInstance

### Community 4 - "Community 4"
Cohesion: 0.6
Nodes (3): BiStream, ClientUniStream, ServerUniStream

### Community 5 - "Community 5"
Cohesion: 0.67
Nodes (2): ConnectionContext, ConnectionWrapper

### Community 6 - "Community 6"
Cohesion: 0.67
Nodes (2): ConnectionManager, ConnectionManagerError

### Community 7 - "Community 7"
Cohesion: 0.67
Nodes (2): add(), it_works()

### Community 8 - "Community 8"
Cohesion: 0.67
Nodes (2): ServerError, ServerInstance

### Community 9 - "Community 9"
Cohesion: 0.67
Nodes (1): ServerConfig

### Community 10 - "Community 10"
Cohesion: 0.67
Nodes (3): BiStream, ClientUniStream, ServerUniStream

### Community 13 - "Community 13"
Cohesion: 1.0
Nodes (1): Graphify Maintenance Rules

## Knowledge Gaps
- **5 isolated node(s):** `MyMetadata`, `ClientUniStream`, `BiStream`, `ConnectionHandlerFn`, `Graphify Maintenance Rules`
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 0`** (9 nodes): `ServerInstance<ConnectionMetadata>`, `.get_bistream()`, `.get_manager_read_lock()`, `.get_manager_write_lock()`, `.get_unistream()`, `.handle_incoming()`, `.listen_connections()`, `.new()`, `.register_connection_handler()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 1`** (8 nodes): `ConnectionManager<ConnectionMetadata>`, `.add_connection()`, `.get_connection()`, `.new()`, `.set_client_bi()`, `.set_client_uni()`, `.set_server_bi()`, `.set_server_uni()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 5`** (4 nodes): `connection_wrapper.rs`, `ConnectionContext`, `ConnectionWrapper`, `connection_wrapper.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 6`** (4 nodes): `connection.rs`, `ConnectionManager`, `ConnectionManagerError`, `connection.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 7`** (4 nodes): `lib.rs`, `add()`, `it_works()`, `lib.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 8`** (4 nodes): `server.rs`, `server.rs`, `ServerError`, `ServerInstance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 9`** (3 nodes): `server_config.rs`, `server_config.rs`, `ServerConfig`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 13`** (1 nodes): `Graphify Maintenance Rules`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ConnectionContext` connect `Community 5` to `Community 2`?**
  _High betweenness centrality (0.029) - this node is a cross-community bridge._
- **What connects `MyMetadata`, `ClientUniStream`, `BiStream` to the rest of the system?**
  _5 weakly-connected nodes found - possible documentation gaps or missing edges._