use async_trait::async_trait;
pub use bytes::Bytes;
pub use http::{Method, Uri, HeaderName, HeaderValue, HeaderMap, Error as HttpError, StatusCode};
pub use http_body::Body;
pub use http_body_util::{BodyExt, Full};

#[derive(Debug, Clone)]
pub struct Request<B = Full<Bytes>> {
    pub path: String,
    pub headers: HeaderMap,
    pub body: B,
}

pub struct Response<B = Full<Bytes>> {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: B,
}

impl<B> Response<B> {
    pub fn new(status: u16, body: B) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body,
        }
    }
}

impl Response<Full<Bytes>> {
    pub fn empty(status: u16) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: Full::new(Bytes::new()),
        }
    }
}

pub trait IntoResponse {
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
pub trait FromRequest<S, B = Full<Bytes>>: Sized {
    type Rejection: IntoResponse;
    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection>;
}

// Standard Extractors

pub struct Json<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Json<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
    B: Body + Send + Sync + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
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

pub struct Bincode<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Bincode<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
    B: Body + Send + Sync + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
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

pub struct Protobuf<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Protobuf<T>
where
    T: prost::Message + Default,
    S: Send + Sync,
    B: Body + Send + Sync + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
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

pub struct RawBytes(pub Bytes);

#[async_trait]
impl<S, B> FromRequest<S, B> for RawBytes
where
    S: Send + Sync,
    B: Body + Send + Sync + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
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

impl<B> From<Request<B>> for http::Request<B> {
    fn from(stric_req: Request<B>) -> Self {
        let mut req = http::Request::new(stric_req.body);
        *req.uri_mut() = stric_req.path.parse().unwrap_or_default();
        *req.method_mut() = Method::POST;
        *req.headers_mut() = stric_req.headers;
        req
    }
}

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

impl<B> From<Response<B>> for http::Response<B> {
    fn from(stric_res: Response<B>) -> Self {
        let mut res = http::Response::new(stric_res.body);
        *res.status_mut() = StatusCode::from_u16(stric_res.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        *res.headers_mut() = stric_res.headers;
        res
    }
}
