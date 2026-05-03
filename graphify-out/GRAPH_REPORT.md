# Graph Report - stric  (2026-05-03)

## Corpus Check
- 7 files · ~920 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 47 nodes · 44 edges · 11 communities detected
- Extraction: 93% EXTRACTED · 7% INFERRED · 0% AMBIGUOUS · INFERRED: 3 edges (avg confidence: 0.87)
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
- [[_COMMUNITY_Community 11|Community 11]]

## God Nodes (most connected - your core abstractions)
1. `ServerInstance<ConnectionMetadata>` - 8 edges
2. `ConnectionManager<ConnectionMetadata>` - 7 edges
3. `ServerInstance` - 3 edges
4. `ConnectionContext` - 2 edges
5. `add()` - 2 edges
6. `it_works()` - 2 edges
7. `ServerConfig` - 2 edges
8. `ServerUniStream` - 2 edges
9. `ConnectionWrapper` - 2 edges
10. `ConnectionContext` - 2 edges

## Surprising Connections (you probably didn't know these)
- `ServerInstance` --references--> `ServerConfig`  [EXTRACTED]
  src/server.rs → src/server_config.rs
- `ServerConfig` --references--> `ConnectionContext`  [EXTRACTED]
  src/server_config.rs → src/connection_wrapper.rs
- `ConnectionManager` --shares_data_with--> `ConnectionWrapper`  [EXTRACTED]
  src/connection.rs → src/connection_wrapper.rs
- `ServerInstance` --references--> `ConnectionManager`  [EXTRACTED]
  src/server.rs → src/connection.rs
- `ServerInstance` --references--> `ConnectionHandlerFn`  [EXTRACTED]
  src/server.rs → src/handler_types.rs

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
Cohesion: 0.4
Nodes (6): ConnectionManager, ConnectionContext, ConnectionWrapper, ConnectionHandlerFn, ServerConfig, ServerInstance

### Community 3 - "Community 3"
Cohesion: 0.5
Nodes (3): BiStream, ClientUniStream, ServerUniStream

### Community 4 - "Community 4"
Cohesion: 0.5
Nodes (2): ConnectionContext, ConnectionWrapper

### Community 5 - "Community 5"
Cohesion: 1.0
Nodes (2): add(), it_works()

### Community 6 - "Community 6"
Cohesion: 0.67
Nodes (2): ConnectionManager, ConnectionManagerError

### Community 7 - "Community 7"
Cohesion: 0.67
Nodes (2): ServerError, ServerInstance

### Community 8 - "Community 8"
Cohesion: 0.67
Nodes (3): BiStream, ClientUniStream, ServerUniStream

### Community 9 - "Community 9"
Cohesion: 1.0
Nodes (1): ServerConfig

### Community 11 - "Community 11"
Cohesion: 1.0
Nodes (1): Graphify Maintenance Rules

## Knowledge Gaps
- **13 isolated node(s):** `ServerConfig`, `ServerUniStream`, `ClientUniStream`, `BiStream`, `ConnectionWrapper` (+8 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 0`** (9 nodes): `ServerInstance<ConnectionMetadata>`, `.get_bistream()`, `.get_manager_read_lock()`, `.get_manager_write_lock()`, `.get_unistream()`, `.handle_incoming()`, `.listen_connections()`, `.new()`, `.register_connection_handler()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 1`** (8 nodes): `ConnectionManager<ConnectionMetadata>`, `.add_connection()`, `.get_connection()`, `.new()`, `.set_client_bi()`, `.set_client_uni()`, `.set_server_bi()`, `.set_server_uni()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 4`** (4 nodes): `connection_wrapper.rs`, `ConnectionContext`, `.default()`, `ConnectionWrapper`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 5`** (3 nodes): `lib.rs`, `add()`, `it_works()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 6`** (3 nodes): `connection.rs`, `ConnectionManager`, `ConnectionManagerError`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 7`** (3 nodes): `server.rs`, `ServerError`, `ServerInstance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 9`** (2 nodes): `server_config.rs`, `ServerConfig`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 11`** (1 nodes): `Graphify Maintenance Rules`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What connects `ServerConfig`, `ServerUniStream`, `ClientUniStream` to the rest of the system?**
  _13 weakly-connected nodes found - possible documentation gaps or missing edges._