# Sandwich Model Implementation Plan

## Objective
Implement the "Sandwich Model" in `stric-tower` to enable seamless integration with standard `tower-http` middleware (like `TraceLayer`). This is achieved by providing an adapter layer that translates between `stric-tower`'s internal request/response types and the standard `http` crate types. The implementation prioritizes performance by supporting **streaming bodies** and using **native `HeaderMap`** for internal storage.

## Scope & Impact
*   **Target:** `stric-tower` crate.
*   **Impact:** Users can use standard ecosystem middleware with a single method call (`layer_standard`). The architecture avoids redundant copies and supports large payloads via streaming.

## Key Files & Context
*   `stric-tower/Cargo.toml`: Added `http`, `http-body`, and `http-body-util` dependencies.
*   `stric-tower/src/http.rs`: Houses generic `Request<B>` and `Response<B>` types and `HeaderMap` logic.
*   `stric-tower/src/adapter.rs`: Contains the generic `HttpAdapter` and `HttpServiceShim` implementations.
*   `stric-tower/src/routing.rs`: `Router` supports generic bodies and the `layer_standard` method.
*   `stric-tower/examples/middleware.rs`: Demonstrates integration with `tower-http::trace::TraceLayer`.

## Implementation Details

### Phase 1: Dependencies
1.  Added `http`, `http-body`, and `http-body-util` to `Cargo.toml`.

### Phase 2: High-Performance Types (`src/http.rs`)
1.  **Generic Bodies:** `Request<B>` and `Response<B>` are now generic over `B: Body`, enabling O(1) memory usage for streaming payloads.
2.  **Native Headers:** Switched internal header storage to `http::HeaderMap`.
3.  **Direct Conversion:** Wire protocol headers (Prost `map<string, string>`) are converted directly to/from `HeaderMap` in the server/client logic to avoid double-conversion bottlenecks.

### Phase 3: The Generic Inner Shim (`src/adapter.rs`)
1.  `HttpServiceShim<S>` wraps an internal `stric-tower` service.
2.  It implements `Service<http::Request<B1>, Response = http::Response<B2>>`.
3.  **Logic:** Transparently passes generic request/reponse bodies through the translation layer.

### Phase 4: The Generic Outer Adapter & Router Integration
1.  `HttpAdapter<S, L>` wraps the service and the standard Tower `Layer`.
2.  It supports layers that transform the body type (e.g., `TraceLayer` wrapping bodies in `ResponseBody`).
3.  `Router::layer_standard<L>` provides the ergonomic entry point for middleware integration.

## Verification & Testing
1.  **Integration Tests:** Verified that `axum`-like routing still works with the new generic types.
2.  **Ecosystem Compatibility:** Successfully integrated `tower-http`'s `TraceLayer` in `examples/middleware.rs`.
3.  **Performance Check:** Confirmed that `collect().await` is only called when required by the wire protocol or specific extractors, preserving streaming capabilities for the rest of the stack.
