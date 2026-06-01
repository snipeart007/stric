# RFC 2026-0004: stric-tower HTTP Sandwich and Middleware Adapter

## 1. Objective
This RFC specifies the design of the "Sandwich Model" in `stric-tower`. It enables standard Rust web ecosystem middleware (such as `tower-http` tracing, authorization, compression, etc.) to wrap `stric-tower` routing services. The architecture adapts between `stric-tower`'s wire envelope representations and standard `http::Request` / `http::Response` types while maintaining memory efficiency and supporting streaming payloads.

---

## 2. The Sandwich Architecture

```
                  Incoming Wire Envelope
                            │
                            ▼
              ┌───────────────────────────┐
              │     Translation Layer     │  <-- Standardizes request metadata
              └─────────────┬─────────────┘
                            │
                            ▼
              ┌───────────────────────────┐
              │  tower-http Middleware    │  <-- Standard HTTP middleware
              │     (e.g., TraceLayer)    │
              └─────────────┬─────────────┘
                            │
                            ▼
              ┌───────────────────────────┐
              │     Translation Layer     │  <-- Converts back to tower format
              └─────────────┬─────────────┘
                            │
                            ▼
              ┌───────────────────────────┐
              │       stric-tower         │  <-- Axum-like route executor
              │         Router            │
              └───────────────────────────┘
```

The middleware is "sandwiched" between translation layers, mapping external connections to standard request/response types.

---

## 3. High-Performance Types

To avoid buffer copying overhead:
1. **Generic Bodies:** Requests and responses are generic over the body type `B` implementing `http_body::Body`:
   ```rust
   pub struct Request<B> {
       pub method: http::Method,
       pub uri: http::Uri,
       pub headers: http::HeaderMap,
       pub body: B,
   }
   
   pub struct Response<B> {
       pub status: http::StatusCode,
       pub headers: http::HeaderMap,
       pub body: B,
   }
   ```
2. **Native Headers:** Internal headers are stored using the standard `http::HeaderMap`.
3. **Direct Deserialization:** Protobuf headers (`map<string, string>`) are converted directly to `HeaderMap` keys and values to prevent redundant string copies.

---

## 4. Adapter & Layer Specifications

### 4.1. `HttpServiceShim<S>`
Wraps an internal `stric-tower` router or endpoint and implements `tower::Service` for HTTP request types:

```rust
pub struct HttpServiceShim<S> {
    pub(crate) inner: S,
}

impl<S, B1, B2> tower::Service<http::Request<B1>> for HttpServiceShim<S>
where
    S: tower::Service<Request<B1>, Response = Response<B2>>,
{
    type Response = http::Response<B2>;
    type Error = S::Error;
    type Future = HttpServiceShimFuture<S::Future>;
    // Converts http::Request -> stric_tower::Request, forwards, then maps response back.
}
```

### 4.2. `HttpAdapter<S, L>`
The outer wrapper that applies a standard HTTP middleware Layer `L` to `HttpServiceShim`:

```rust
pub struct HttpAdapter<S, L> {
    pub(crate) service: HttpServiceShim<S>,
    pub(crate) layer: L,
}

impl<S, L, B> Router<S> {
    /// Ergonomic router extension to apply standard tower middleware layers
    pub fn layer_standard<L>(self, layer: L) -> HttpAdapter<Self, L>
    where
        L: tower::Layer<HttpServiceShim<Self>>;
}
```
This enables zero-copy streaming passes and standard logging/metrics integration directly into `stric-tower`.
