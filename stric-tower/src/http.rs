use async_trait::async_trait;
pub use bytes::Bytes;
pub use http::{Method, Uri, HeaderName, HeaderValue, HeaderMap, Error as HttpError, StatusCode};
pub use http_body::Body;
pub use http_body_util::{BodyExt, Full};

/// A simplified request type used by `stric-tower` handlers and services.
///
/// The type keeps the request path and headers separate from the body so the
/// same request shape can be translated to and from `http::Request<B>` when
/// standard Tower middleware is involved.
#[derive(Debug, Clone)]
pub struct Request<B = Full<Bytes>> {
    /// The routed path sent over the Stric wire protocol.
    pub path: String,
    /// Request headers stored in the native `http::HeaderMap` format.
    pub headers: HeaderMap,
    /// The request body, generic over any `http_body::Body` implementation.
    pub body: B,
}

/// A simplified response type used by `stric-tower` handlers and services.
///
/// Responses carry a numeric status code instead of `http::StatusCode` so they
/// map directly onto the wire representation, while still being convertible to
/// `http::Response<B>` when needed.
#[derive(Debug)]
pub struct Response<B = Full<Bytes>> {
    /// The response status code.
    pub status: u16,
    /// Response headers stored in the native `http::HeaderMap` format.
    pub headers: HeaderMap,
    /// The response body, generic over any `http_body::Body` implementation.
    pub body: B,
}

impl<B> Response<B> {
    /// Creates a response with the provided status code and body.
    ///
    /// Headers start empty and can be populated by the caller or a middleware
    /// layer later in the pipeline.
    ///
    /// # Edge Cases
    /// `Response::new` does not validate the numeric status code. Invalid codes
    /// are preserved inside the Stric response and are only coerced to `500`
    /// when the response is converted into `http::Response<B>`.
    pub fn new(status: u16, body: B) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body,
        }
    }
}

impl Response<Full<Bytes>> {
    /// Creates an empty response body for status-only replies such as `404`.
    pub fn empty(status: u16) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: Full::new(Bytes::new()),
        }
    }
}

/// Converts handler return values into a concrete `Response<Full<Bytes>>`.
///
/// This mirrors the role of `axum::response::IntoResponse`: handlers can
/// return typed wrappers such as [`Json`] or a plain `String`, and the router
/// will normalize them into a concrete response before serializing them over
/// the network.
pub trait IntoResponse {
    /// Produces a concrete response value.
    fn into_response(self) -> Response<Full<Bytes>>;
}

impl IntoResponse for Response<Full<Bytes>> {
    fn into_response(self) -> Response<Full<Bytes>> {
        self
    }
}

impl IntoResponse for () {
    fn into_response(self) -> Response<Full<Bytes>> {
        Response::empty(200)
    }
}

impl IntoResponse for String {
    fn into_response(self) -> Response<Full<Bytes>> {
        Response {
            status: 200,
            headers: HeaderMap::new(),
            body: Full::new(Bytes::from(self)),
        }
    }
}

impl<T, E> IntoResponse for Result<T, E>
where
    T: IntoResponse,
    E: IntoResponse,
{
    fn into_response(self) -> Response<Full<Bytes>> {
        match self {
            Ok(t) => t.into_response(),
            Err(e) => e.into_response(),
        }
    }
}

#[async_trait]
/// Extracts typed values from an incoming request before the handler runs.
///
/// Extractors are generic over shared application state `S` and body type `B`.
/// They can fail with a rejection that is itself convertible into a response.
pub trait FromRequest<S, B = Full<Bytes>>: Sized {
    /// The rejection returned when extraction fails.
    type Rejection: IntoResponse;

    /// Attempts to build `Self` from the request and shared state.
    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection>;
}

/// JSON request extractor and JSON response wrapper.
///
/// As an extractor, it buffers the body and deserializes it with `serde_json`.
/// As a response, it serializes the value and sets `content-type:
/// application/json`.
///
/// Extraction failures are returned as `Response<Full<Bytes>>` rejections:
/// invalid JSON becomes `400`, while body collection failures become `500`.
pub struct Json<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Json<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
    B: Body + Send + Sync + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    type Rejection = Response<Full<Bytes>>;

    async fn from_request(req: Request<B>, _state: &S) -> Result<Self, Self::Rejection> {
        let body = req.body.collect().await.map_err(|e| {
             Response {
                status: 500,
                headers: HeaderMap::new(),
                body: Full::new(Bytes::from(format!("Body collection error: {}", e.into()))),
            }
        })?.to_bytes();

        serde_json::from_slice(&body)
            .map(Json)
            .map_err(|e| {
                Response {
                    status: 400,
                    headers: HeaderMap::new(),
                    body: Full::new(Bytes::from(format!("Invalid JSON: {}", e))),
                }
            })
    }
}

impl<T> IntoResponse for Json<T>
where
    T: serde::Serialize,
{
    fn into_response(self) -> Response<Full<Bytes>> {
        match serde_json::to_vec(&self.0) {
            Ok(body) => {
                let mut headers = HeaderMap::new();
                headers.insert("content-type", HeaderValue::from_static("application/json"));
                Response {
                    status: 200,
                    headers,
                    body: Full::new(Bytes::from(body)),
                }
            },
            Err(e) => Response {
                status: 500,
                headers: HeaderMap::new(),
                body: Full::new(Bytes::from(format!("JSON serialization error: {}", e))),
            },
        }
    }
}

/// Bincode request extractor and response wrapper.
///
/// This is useful when both sides of the connection share Rust-native payload
/// types and want a compact binary format.
///
/// Extraction failures are returned as `Response<Full<Bytes>>` rejections:
/// invalid bincode becomes `400`, while body collection failures become `500`.
pub struct Bincode<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Bincode<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
    B: Body + Send + Sync + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    type Rejection = Response<Full<Bytes>>;

    async fn from_request(req: Request<B>, _state: &S) -> Result<Self, Self::Rejection> {
        let body = req.body.collect().await.map_err(|e| {
             Response {
                status: 500,
                headers: HeaderMap::new(),
                body: Full::new(Bytes::from(format!("Body collection error: {}", e.into()))),
            }
        })?.to_bytes();

        bincode::deserialize(&body)
            .map(Bincode)
            .map_err(|e| {
                Response {
                    status: 400,
                    headers: HeaderMap::new(),
                    body: Full::new(Bytes::from(format!("Invalid Bincode: {}", e))),
                }
            })
    }
}

impl<T> IntoResponse for Bincode<T>
where
    T: serde::Serialize,
{
    fn into_response(self) -> Response<Full<Bytes>> {
        match bincode::serialize(&self.0) {
            Ok(body) => {
                let mut headers = HeaderMap::new();
                headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
                Response {
                    status: 200,
                    headers,
                    body: Full::new(Bytes::from(body)),
                }
            },
            Err(e) => Response {
                status: 500,
                headers: HeaderMap::new(),
                body: Full::new(Bytes::from(format!("Bincode serialization error: {}", e))),
            },
        }
    }
}

/// Protobuf request extractor and response wrapper.
///
/// The wrapped type must implement `prost::Message`. Request extraction buffers
/// the body before decoding, which matches the envelope-based Stric transport.
///
/// Extraction failures are returned as `Response<Full<Bytes>>` rejections:
/// invalid protobuf becomes `400`, while body collection failures become `500`.
pub struct Protobuf<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Protobuf<T>
where
    T: prost::Message + Default,
    S: Send + Sync,
    B: Body + Send + Sync + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    type Rejection = Response<Full<Bytes>>;

    async fn from_request(req: Request<B>, _state: &S) -> Result<Self, Self::Rejection> {
        let body = req.body.collect().await.map_err(|e| {
             Response {
                status: 500,
                headers: HeaderMap::new(),
                body: Full::new(Bytes::from(format!("Body collection error: {}", e.into()))),
            }
        })?.to_bytes();

        T::decode(body)
            .map(Protobuf)
            .map_err(|e| {
                Response {
                    status: 400,
                    headers: HeaderMap::new(),
                    body: Full::new(Bytes::from(format!("Invalid Protobuf: {}", e))),
                }
            })
    }
}

impl<T> IntoResponse for Protobuf<T>
where
    T: prost::Message,
{
    fn into_response(self) -> Response<Full<Bytes>> {
        let mut body = Vec::with_capacity(self.0.encoded_len());
        if let Err(e) = self.0.encode(&mut body) {
            return Response {
                status: 500,
                headers: HeaderMap::new(),
                body: Full::new(Bytes::from(format!("Protobuf encoding error: {}", e))),
            };
        }
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/x-protobuf"));
        Response {
            status: 200,
            headers,
            body: Full::new(Bytes::from(body)),
        }
    }
}

/// Raw request bytes extractor and response wrapper.
///
/// Use this when the handler wants the fully buffered payload without applying
/// a codec such as JSON or Protobuf.
///
/// Extraction failures are returned as `Response<Full<Bytes>>` rejections with
/// status `500` when body collection fails.
pub struct RawBytes(pub Bytes);

#[async_trait]
impl<S, B> FromRequest<S, B> for RawBytes
where
    S: Send + Sync,
    B: Body + Send + Sync + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    type Rejection = Response<Full<Bytes>>;

    async fn from_request(req: Request<B>, _state: &S) -> Result<Self, Self::Rejection> {
        let body = req.body.collect().await.map_err(|e| {
             Response {
                status: 500,
                headers: HeaderMap::new(),
                body: Full::new(Bytes::from(format!("Body collection error: {}", e.into()))),
            }
        })?.to_bytes();
        Ok(RawBytes(body))
    }
}

impl IntoResponse for RawBytes {
    fn into_response(self) -> Response<Full<Bytes>> {
        Response {
            status: 200,
            headers: HeaderMap::new(),
            body: Full::new(self.0),
        }
    }
}

/// Shared application state extractor.
///
/// The router state type must implement `AsRef<T>` so the extractor can clone
/// a typed view of the shared state into the handler.
///
/// This extractor does not currently fail; its rejection type exists only to
/// match the extractor trait signature.
pub struct State<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for State<T>
where
    T: Clone + Send + Sync + 'static,
    S: Send + Sync,
    S: AsRef<T>,
    B: Send + Sync + 'static,
{
    type Rejection = Response<Full<Bytes>>;

    async fn from_request(_req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        Ok(State(state.as_ref().clone()))
    }
}

// --- Conversions for HTTP compatibility ---

/// Converts a Stric request into a standard `http::Request`.
///
/// The method is fixed to `POST` because the current Stric wire format only
/// transmits a path, headers, and body.
impl<B> From<Request<B>> for http::Request<B> {
    fn from(stric_req: Request<B>) -> Self {
        let mut req = http::Request::new(stric_req.body);
        *req.uri_mut() = stric_req.path.parse().unwrap_or_default();
        *req.method_mut() = Method::POST;
        *req.headers_mut() = stric_req.headers;
        req
    }
}

/// Converts a standard `http::Response` into the Stric response type.
impl<B> From<http::Response<B>> for Response<B> {
    fn from(http_res: http::Response<B>) -> Self {
        let (parts, body) = http_res.into_parts();
        Response {
            status: parts.status.as_u16(),
            headers: parts.headers,
            body,
        }
    }
}

/// Converts a standard `http::Request` into the Stric request type.
///
/// Only the URI path is preserved because that is the portion understood by
/// the Stric router today.
impl<B> TryFrom<http::Request<B>> for Request<B> {
    type Error = HttpError;

    fn try_from(http_req: http::Request<B>) -> Result<Self, Self::Error> {
        let (parts, body) = http_req.into_parts();
        let path = parts.uri.path().to_string();
        Ok(Request {
            path,
            headers: parts.headers,
            body,
        })
    }
}

/// Converts a Stric response into a standard `http::Response`.
///
/// Invalid numeric status codes are coerced to `500 Internal Server Error`
/// because `http::StatusCode` requires a validated code.
impl<B> From<Response<B>> for http::Response<B> {
    fn from(stric_res: Response<B>) -> Self {
        let mut res = http::Response::new(stric_res.body);
        *res.status_mut() = StatusCode::from_u16(stric_res.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        *res.headers_mut() = stric_res.headers;
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Payload {
        value: String,
    }

    struct AppState(String);

    impl AsRef<String> for AppState {
        fn as_ref(&self) -> &String {
            &self.0
        }
    }

    #[tokio::test]
    async fn string_into_response_uses_ok_status_and_utf8_body() {
        let response = String::from("hello").into_response();

        assert_eq!(response.status, 200);
        let body = response.body.collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from("hello"));
    }

    #[test]
    fn request_and_http_request_round_trip_preserves_path_and_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-test", HeaderValue::from_static("present"));
        let request = Request {
            path: "/hello".to_string(),
            headers,
            body: Full::new(Bytes::from_static(b"payload")),
        };

        let http_request: http::Request<_> = request.into();
        assert_eq!(http_request.method(), Method::POST);
        assert_eq!(http_request.uri().path(), "/hello");
        assert_eq!(http_request.headers()["x-test"], "present");

        let round_tripped = Request::try_from(http_request).unwrap();
        assert_eq!(round_tripped.path, "/hello");
        assert_eq!(round_tripped.headers["x-test"], "present");
    }

    #[test]
    fn invalid_response_status_defaults_to_internal_server_error() {
        let response = Response::new(42, Full::new(Bytes::from_static(b"bad")));

        let http_response: http::Response<_> = response.into();

        assert_eq!(http_response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn json_into_response_sets_content_type_and_serializes_body() {
        let response = Json(Payload {
            value: "ok".to_string(),
        })
        .into_response();

        assert_eq!(response.status, 200);
        assert_eq!(response.headers["content-type"], "application/json");

        let body = response.body.collect().await.unwrap().to_bytes();
        let decoded: Payload = serde_json::from_slice(&body).unwrap();
        assert_eq!(decoded, Payload { value: "ok".to_string() });
    }

    #[tokio::test]
    async fn invalid_json_extractor_returns_bad_request() {
        let request = Request {
            path: "/json".to_string(),
            headers: HeaderMap::new(),
            body: Full::new(Bytes::from_static(b"{not-json")),
        };

        let rejection = match Json::<Payload>::from_request(request, &()).await {
            Ok(_) => panic!("expected invalid JSON rejection"),
            Err(rejection) => rejection,
        };

        assert_eq!(rejection.status, 400);
        let body = rejection.body.collect().await.unwrap().to_bytes();
        let body_text = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_text.contains("Invalid JSON"));
    }

    #[tokio::test]
    async fn state_extractor_clones_from_router_state() {
        let extracted = match State::<String>::from_request(
            Request {
                path: "/state".to_string(),
                headers: HeaderMap::new(),
                body: Full::new(Bytes::new()),
            },
            &AppState("shared".to_string()),
        )
        .await
        {
            Ok(extracted) => extracted,
            Err(_) => panic!("state extractor should succeed"),
        };

        assert_eq!(extracted.0, "shared");
    }
}
