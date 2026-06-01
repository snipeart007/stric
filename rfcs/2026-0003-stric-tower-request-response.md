# RFC 2026-0003: stric-tower Request-Response Service Framework

## 1. Objective
This RFC specifies the Request-Response service framework of `stric-tower`. It implements a high-level, ergonomics-focused HTTP-like routing layer on top of `stric-core`'s transport streams, exposing an `axum`-style API with asynchronous handlers, extractors, and request dispatching.

---

## 2. Protobuf Wire Envelopes
To support path-based routing and headers over raw QUIC streams (which do not have native routing semantics), all requests and responses are framed with a 4-byte big-endian length prefix and encoded as Protobuf messages.

```protobuf
syntax = "proto3";
package stric_tower.wire;

message RequestEnvelope {
  string              path = 1;
  map<string, string> headers = 2;
  bytes               payload = 3;
}

message ResponseEnvelope {
  uint32              status_code = 1;
  map<string, string> headers = 2;
  bytes               payload = 3;
}
```

---

## 3. Core Framework Abstractions

### 3.1. Handlers (`Handler` Trait)
Handlers are written as simple async functions. The `Handler` trait is implemented for asynchronous functions of varying arguments using macro codegen.

```rust
pub trait Handler<T, S>: Clone + Send + Sized + 'static {
    type Future: Future<Output = Response> + Send + 'static;
    fn call(self, req: Request, state: S) -> Self::Future;
}
```

### 3.2. Extractors (`FromRequest`) & Responders (`IntoResponse`)
Arguments in handler signatures extract data from the incoming `RequestEnvelope`.

```rust
#[async_trait]
pub trait FromRequest: Sized {
    type Rejection: IntoResponse;
    async fn from_request(req: &mut Request) -> Result<Self, Self::Rejection>;
}

pub trait IntoResponse {
    fn into_response(self) -> Response;
}
```

Standard types provided out of the box:
* **`Json<T>`:** Parses request payload using `serde_json`.
* **`Bincode<T>`:** Parses payload using `bincode`.
* **`Protobuf<T>`:** Parses payload using `prost::Message`.
* **`RawBytes` / `Bytes`:** Extracts the payload directly as raw bytes.
* **`State<S>`:** Extracts shared application state.

---

## 4. The Router API

The `Router` registers paths and matches them to boxed handlers:

```rust
pub struct Router<S = ()> {
    routes: HashMap<String, BoxedHandler<S>>,
    state: S,
}

impl<S: Clone + Send + Sync + 'static> Router<S> {
    pub fn new() -> Self;
    pub fn route<H, T>(mut self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S>;
    pub fn with_state<S2>(self, state: S2) -> Router<S2>;
}
```
* On incoming connections, `stric-tower` listens for bidirectional streams (`BiStream`).
* For each stream, it reads the length prefix, deserializes the `RequestEnvelope`, routes to the matching path, executes the handler, and writes the `ResponseEnvelope` back before closing the write stream.
