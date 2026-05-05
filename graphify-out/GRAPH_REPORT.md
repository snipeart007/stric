# Graph Report - stric  (2026-05-05)

## Corpus Check
- 28 files · ~13,992 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 293 nodes · 462 edges · 24 communities detected
- Extraction: 78% EXTRACTED · 22% INFERRED · 0% AMBIGUOUS · INFERRED: 101 edges (avg confidence: 0.8)
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
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 29|Community 29]]

## God Nodes (most connected - your core abstractions)
1. `setup_crypto()` - 11 edges
2. `test_axum_like_tower_integration()` - 10 edges
3. `test_axum_like_404()` - 10 edges
4. `test_server_connection_lifecycle()` - 9 edges
5. `test_connection_manager_updates()` - 9 edges
6. `test_error_channel_and_handler_failure()` - 9 edges
7. `test_custom_metadata()` - 9 edges
8. `build_client()` - 9 edges
9. `main()` - 8 edges
10. `ConnectionManager<ConnectionMetadata>` - 7 edges

## Surprising Connections (you probably didn't know these)
- `hello_handler()` --calls--> `Json`  [INFERRED]
  stric-tower\examples\middleware.rs → stric-tower\src\http.rs
- `echo_handler()` --calls--> `Json`  [INFERRED]
  stric-tower\examples\server.rs → stric-tower\src\http.rs
- `main()` --calls--> `Json`  [INFERRED]
  stric-tower\examples\client.rs → stric-tower\src\http.rs
- `echo_handler()` --calls--> `Json`  [INFERRED]
  stric-tower\examples\tower_middleware.rs → stric-tower\src\http.rs
- `Json` --calls--> `echo_handler()`  [INFERRED]
  stric-tower\src\http.rs → stric-tower\tests\integration_test.rs

## Communities

### Community 0 - "Community 0"
Cohesion: 0.13
Nodes (26): main(), main(), run_client(), main(), run_client(), Request<B>, Response<Full<Bytes>>, TowerConnectionHandler<S, B> (+18 more)

### Community 1 - "Community 1"
Cohesion: 0.08
Nodes (16): SkipServerVerification, TowerClientService, BincodeFormat, ProstCodec, ProstCodec<Req, Res>, read_length_prefixed(), read_request_envelope(), read_response_envelope() (+8 more)

### Community 2 - "Community 2"
Cohesion: 0.12
Nodes (11): TowerError, Bincode<T>, http::Request<B>, http::Response<B>, Json<T>, Protobuf<T>, RawBytes, Response<B> (+3 more)

### Community 3 - "Community 3"
Cohesion: 0.16
Nodes (12): build_echo_request(), connect_insecure(), decode_echo_response(), EchoRequest, EchoResponse, main(), RetryOnUnavailable, run_buffer_and_limit_demo() (+4 more)

### Community 4 - "Community 4"
Cohesion: 0.1
Nodes (10): search_handler(), SearchRequest, SearchResponse, SearchResult, SkipServerVerification, hello_handler(), HelloRequest, HelloResponse (+2 more)

### Community 5 - "Community 5"
Cohesion: 0.13
Nodes (17): echo_handler(), AppState, Bincode, FromRequest, IntoResponse, invalid_json_extractor_returns_bad_request(), invalid_response_status_defaults_to_internal_server_error(), Json (+9 more)

### Community 6 - "Community 6"
Cohesion: 0.15
Nodes (3): BiStream, ClientUniStream, ServerUniStream

### Community 7 - "Community 7"
Cohesion: 0.18
Nodes (7): KeepAlivePool, KeepAliveWorker, ManagedStream, PoolCommand, PoolManager, WorkerCommand, WorkerHandle

### Community 8 - "Community 8"
Cohesion: 0.13
Nodes (10): hello_handler(), main(), Message, echo_handler(), EchoRequest, EchoResponse, main(), Server (+2 more)

### Community 9 - "Community 9"
Cohesion: 0.25
Nodes (3): HandlerServiceWrapper<H, T, S, B>, Router<(), Full<Bytes>>, Router<S, B>

### Community 10 - "Community 10"
Cohesion: 0.36
Nodes (2): HttpAdapter<S, L>, HttpServiceShim<S>

### Community 11 - "Community 11"
Cohesion: 0.25
Nodes (3): EchoRequest, EchoResponse, SkipServerVerification

### Community 12 - "Community 12"
Cohesion: 0.29
Nodes (1): ConnectionManager<ConnectionMetadata>

### Community 13 - "Community 13"
Cohesion: 0.4
Nodes (1): ServerInstance<ConnectionMetadata>

### Community 14 - "Community 14"
Cohesion: 0.5
Nodes (3): HandlerServiceTrait, HandlerServiceWrapper, Router

### Community 15 - "Community 15"
Cohesion: 0.5
Nodes (1): WrappedBody<B>

### Community 16 - "Community 16"
Cohesion: 0.67
Nodes (2): ConnectionManager, ConnectionManagerError

### Community 17 - "Community 17"
Cohesion: 0.67
Nodes (2): ConnectionContext, ConnectionWrapper

### Community 18 - "Community 18"
Cohesion: 0.67
Nodes (2): ServerError, ServerInstance

### Community 19 - "Community 19"
Cohesion: 0.67
Nodes (2): HttpAdapter, HttpServiceShim

### Community 20 - "Community 20"
Cohesion: 1.0
Nodes (1): ServerConfig

### Community 22 - "Community 22"
Cohesion: 1.0
Nodes (1): Handler

### Community 23 - "Community 23"
Cohesion: 1.0
Nodes (1): F

### Community 29 - "Community 29"
Cohesion: 1.0
Nodes (1): Graphify Maintenance Rules

## Knowledge Gaps
- **54 isolated node(s):** `ConnectionManager`, `ConnectionManagerError`, `ConnectionWrapper`, `ConnectionContext`, `PoolCommand` (+49 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 10`** (10 nodes): `HttpAdapter<S, L>`, `.call()`, `.clone()`, `.new()`, `.poll_ready()`, `HttpServiceShim<S>`, `.call()`, `.clone()`, `.new()`, `.poll_ready()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 12`** (7 nodes): `ConnectionManager<ConnectionMetadata>`, `.add_connection()`, `.set_client_bi()`, `.set_client_uni()`, `.set_keep_alive()`, `.set_server_bi()`, `.set_server_uni()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 13`** (6 nodes): `ServerInstance<ConnectionMetadata>`, `.get_bistream()`, `.get_unistream()`, `.handle_incoming()`, `.listen_connections()`, `.register_connection_handler()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 15`** (4 nodes): `WrappedBody<B>`, `.is_end_stream()`, `.poll_frame()`, `.size_hint()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 16`** (3 nodes): `ConnectionManager`, `ConnectionManagerError`, `connection.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 17`** (3 nodes): `ConnectionContext`, `ConnectionWrapper`, `connection_wrapper.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 18`** (3 nodes): `ServerError`, `ServerInstance`, `server.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 19`** (3 nodes): `HttpAdapter`, `HttpServiceShim`, `adapter.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 20`** (2 nodes): `ServerConfig`, `server_config.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 22`** (2 nodes): `Handler`, `handler.rs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 23`** (2 nodes): `F`, `.call()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 29`** (1 nodes): `Graphify Maintenance Rules`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `handle_stream_axum()` connect `Community 1` to `Community 8`, `Community 0`?**
  _High betweenness centrality (0.083) - this node is a cross-community bridge._
- **Why does `connect_insecure()` connect `Community 3` to `Community 0`?**
  _High betweenness centrality (0.061) - this node is a cross-community bridge._
- **Why does `main()` connect `Community 0` to `Community 11`, `Community 5`?**
  _High betweenness centrality (0.059) - this node is a cross-community bridge._
- **Are the 5 inferred relationships involving `test_axum_like_tower_integration()` (e.g. with `.new()` and `.from()`) actually correct?**
  _`test_axum_like_tower_integration()` has 5 INFERRED edges - model-reasoned connections that need verification._
- **Are the 5 inferred relationships involving `test_axum_like_404()` (e.g. with `.new()` and `.from()`) actually correct?**
  _`test_axum_like_404()` has 5 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `test_server_connection_lifecycle()` (e.g. with `.try_from()` and `.new()`) actually correct?**
  _`test_server_connection_lifecycle()` has 7 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `test_connection_manager_updates()` (e.g. with `.try_from()` and `.new()`) actually correct?**
  _`test_connection_manager_updates()` has 7 INFERRED edges - model-reasoned connections that need verification._