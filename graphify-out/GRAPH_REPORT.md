# Graph Report - .  (2026-05-04)

## Corpus Check
- Corpus is ~1,885 words - fits in a single context window. You may not need a graph.

## Summary
- 77 nodes · 98 edges · 14 communities detected
- Extraction: 82% EXTRACTED · 18% INFERRED · 0% AMBIGUOUS · INFERRED: 18 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Core Server Orchestration|Core Server Orchestration]]
- [[_COMMUNITY_Integration Testing|Integration Testing]]
- [[_COMMUNITY_Connection Registry|Connection Registry]]
- [[_COMMUNITY_Server Resource Access|Server Resource Access]]
- [[_COMMUNITY_Connection Handling Flow|Connection Handling Flow]]
- [[_COMMUNITY_Stream Management|Stream Management]]
- [[_COMMUNITY_Connection Error Handling|Connection Error Handling]]
- [[_COMMUNITY_Connection Context|Connection Context]]
- [[_COMMUNITY_Library Entry Point|Library Entry Point]]
- [[_COMMUNITY_Server Error Handling|Server Error Handling]]
- [[_COMMUNITY_Server Configuration|Server Configuration]]
- [[_COMMUNITY_Documentation Rules|Documentation Rules]]
- [[_COMMUNITY_Registry Errors|Registry Errors]]
- [[_COMMUNITY_Instance Errors|Instance Errors]]

## God Nodes (most connected - your core abstractions)
1. `ConnectionManager<ConnectionMetadata>` - 9 edges
2. `ServerInstance` - 9 edges
3. `ServerInstance<ConnectionMetadata>` - 8 edges
4. `setup_crypto()` - 6 edges
5. `test_server_connection_lifecycle()` - 5 edges
6. `test_connection_manager_updates()` - 5 edges
7. `test_error_channel_and_handler_failure()` - 5 edges
8. `test_custom_metadata()` - 5 edges
9. `ConnectionManager` - 4 edges
10. `ConnectionContext` - 3 edges

## Surprising Connections (you probably didn't know these)
- `ServerInstance` --calls--> `test_server_connection_lifecycle`  [EXTRACTED]
  src/server.rs → tests/integration_test.rs
- `ServerInstance` --calls--> `test_error_channel_and_handler_failure`  [EXTRACTED]
  src/server.rs → tests/integration_test.rs
- `ServerInstance` --calls--> `test_custom_metadata`  [EXTRACTED]
  src/server.rs → tests/integration_test.rs
- `add()` --calls--> `it_works()`  [EXTRACTED]
  src\lib.rs → lib.rs
- `ConnectionManager` --calls--> `test_connection_manager_updates`  [EXTRACTED]
  src/connection.rs → tests/integration_test.rs

## Communities

### Community 0 - "Core Server Orchestration"
Cohesion: 0.19
Nodes (13): ServerConfig, ConnectionManager, ConnectionHandlerFn, ServerInstance, BiStream, ClientUniStream, ServerUniStream, test_error_channel_and_handler_failure (+5 more)

### Community 1 - "Integration Testing"
Cohesion: 0.47
Nodes (7): MyMetadata, setup_crypto(), test_connection_manager_locks(), test_connection_manager_updates(), test_custom_metadata(), test_error_channel_and_handler_failure(), test_server_connection_lifecycle()

### Community 2 - "Connection Registry"
Cohesion: 0.22
Nodes (1): ConnectionManager<ConnectionMetadata>

### Community 3 - "Server Resource Access"
Cohesion: 0.43
Nodes (1): ServerInstance<ConnectionMetadata>

### Community 4 - "Connection Handling Flow"
Cohesion: 0.4
Nodes (6): ConnectionManager, ConnectionContext, ConnectionWrapper, ConnectionHandlerFn, ServerConfig, ServerInstance

### Community 5 - "Stream Management"
Cohesion: 0.6
Nodes (3): BiStream, ClientUniStream, ServerUniStream

### Community 6 - "Connection Error Handling"
Cohesion: 0.67
Nodes (2): ConnectionContext, ConnectionWrapper

### Community 7 - "Connection Context"
Cohesion: 0.67
Nodes (2): ConnectionManager, ConnectionManagerError

### Community 8 - "Library Entry Point"
Cohesion: 0.67
Nodes (2): ServerError, ServerInstance

### Community 9 - "Server Error Handling"
Cohesion: 0.67
Nodes (2): add(), it_works()

### Community 10 - "Server Configuration"
Cohesion: 0.67
Nodes (1): ServerConfig

### Community 13 - "Documentation Rules"
Cohesion: 1.0
Nodes (1): Graphify Maintenance Rules

### Community 14 - "Registry Errors"
Cohesion: 1.0
Nodes (1): ConnectionManagerError

### Community 15 - "Instance Errors"
Cohesion: 1.0
Nodes (1): ServerError

## Knowledge Gaps
- **10 isolated node(s):** `MyMetadata`, `ClientUniStream`, `ConnectionHandlerFn`, `Graphify Maintenance Rules`, `ConnectionManagerError` (+5 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Connection Registry`** (9 nodes): `ConnectionManager<ConnectionMetadata>`, `.add_connection()`, `.get_connection()`, `.get_connection_lock()`, `.get_connection_write_lock()`, `.set_client_bi()`, `.set_client_uni()`, `.set_server_bi()`, `.set_server_uni()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Server Resource Access`** (8 nodes): `ServerInstance<ConnectionMetadata>`, `.get_bistream()`, `.get_manager_read_lock()`, `.get_manager_write_lock()`, `.get_unistream()`, `.handle_incoming()`, `.listen_connections()`, `.register_connection_handler()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Connection Error Handling`** (4 nodes): `connection_wrapper.rs`, `ConnectionContext`, `ConnectionWrapper`, `connection_wrapper.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Connection Context`** (4 nodes): `connection.rs`, `ConnectionManager`, `ConnectionManagerError`, `connection.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Library Entry Point`** (4 nodes): `server.rs`, `server.rs`, `ServerError`, `ServerInstance`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Server Error Handling`** (4 nodes): `lib.rs`, `add()`, `it_works()`, `lib.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Server Configuration`** (3 nodes): `server_config.rs`, `server_config.rs`, `ServerConfig`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Documentation Rules`** (1 nodes): `Graphify Maintenance Rules`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Registry Errors`** (1 nodes): `ConnectionManagerError`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Instance Errors`** (1 nodes): `ServerError`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ConnectionManager<ConnectionMetadata>` connect `Connection Registry` to `Integration Testing`?**
  _High betweenness centrality (0.066) - this node is a cross-community bridge._
- **Why does `ServerInstance<ConnectionMetadata>` connect `Server Resource Access` to `Integration Testing`?**
  _High betweenness centrality (0.045) - this node is a cross-community bridge._
- **Why does `ConnectionContext` connect `Connection Error Handling` to `Integration Testing`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `test_server_connection_lifecycle()` (e.g. with `.default()` and `.new()`) actually correct?**
  _`test_server_connection_lifecycle()` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `MyMetadata`, `ClientUniStream`, `ConnectionHandlerFn` to the rest of the system?**
  _10 weakly-connected nodes found - possible documentation gaps or missing edges._