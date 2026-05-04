use std::collections::HashMap;
use async_trait::async_trait;
pub use bytes::Bytes;

#[derive(Clone)]
pub struct Request {
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Bytes,
}

pub struct Response {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Bytes,
}

impl Response {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HashMap::new(),
            body: Bytes::new(),
        }
    }
}

pub trait IntoResponse {
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl IntoResponse for () {
    fn into_response(self) -> Response {
        Response::new(200)
    }
}

impl IntoResponse for String {
    fn into_response(self) -> Response {
        Response {
            status: 200,
            headers: HashMap::new(),
            body: Bytes::from(self),
        }
    }
}

impl<T, E> IntoResponse for Result<T, E>
where
    T: IntoResponse,
    E: IntoResponse,
{
    fn into_response(self) -> Response {
        match self {
            Ok(t) => t.into_response(),
            Err(e) => e.into_response(),
        }
    }
}

#[async_trait]
pub trait FromRequest<S>: Sized {
    type Rejection: IntoResponse;
    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection>;
}

// Standard Extractors

pub struct Json<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for Json<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        serde_json::from_slice(&req.body)
            .map(Json)
            .map_err(|e| {
                Response {
                    status: 400,
                    headers: HashMap::new(),
                    body: Bytes::from(format!("Invalid JSON: {}", e)),
                }
            })
    }
}

impl<T> IntoResponse for Json<T>
where
    T: serde::Serialize,
{
    fn into_response(self) -> Response {
        match serde_json::to_vec(&self.0) {
            Ok(body) => Response {
                status: 200,
                headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
                body: Bytes::from(body),
            },
            Err(e) => Response {
                status: 500,
                headers: HashMap::new(),
                body: Bytes::from(format!("JSON serialization error: {}", e)),
            },
        }
    }
}

pub struct Bincode<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for Bincode<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        bincode::deserialize(&req.body)
            .map(Bincode)
            .map_err(|e| {
                Response {
                    status: 400,
                    headers: HashMap::new(),
                    body: Bytes::from(format!("Invalid Bincode: {}", e)),
                }
            })
    }
}

impl<T> IntoResponse for Bincode<T>
where
    T: serde::Serialize,
{
    fn into_response(self) -> Response {
        match bincode::serialize(&self.0) {
            Ok(body) => Response {
                status: 200,
                headers: HashMap::from([("content-type".to_string(), "application/octet-stream".to_string())]),
                body: Bytes::from(body),
            },
            Err(e) => Response {
                status: 500,
                headers: HashMap::new(),
                body: Bytes::from(format!("Bincode serialization error: {}", e)),
            },
        }
    }
}

pub struct Protobuf<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for Protobuf<T>
where
    T: prost::Message + Default,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        T::decode(&req.body[..])
            .map(Protobuf)
            .map_err(|e| {
                Response {
                    status: 400,
                    headers: HashMap::new(),
                    body: Bytes::from(format!("Invalid Protobuf: {}", e)),
                }
            })
    }
}

impl<T> IntoResponse for Protobuf<T>
where
    T: prost::Message,
{
    fn into_response(self) -> Response {
        let mut body = Vec::with_capacity(self.0.encoded_len());
        if let Err(e) = self.0.encode(&mut body) {
            return Response {
                status: 500,
                headers: HashMap::new(),
                body: Bytes::from(format!("Protobuf encoding error: {}", e)),
            };
        }
        Response {
            status: 200,
            headers: HashMap::from([("content-type".to_string(), "application/x-protobuf".to_string())]),
            body: Bytes::from(body),
        }
    }
}

pub struct RawBytes(pub Bytes);

#[async_trait]
impl<S> FromRequest<S> for RawBytes
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(RawBytes(req.body))
    }
}

impl IntoResponse for RawBytes {
    fn into_response(self) -> Response {
        Response {
            status: 200,
            headers: HashMap::new(),
            body: self.0,
        }
    }
}

pub struct State<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for State<T>
where
    T: Clone + Send + Sync + 'static,
    S: Send + Sync,
    S: AsRef<T>,
{
    type Rejection = Response;

    async fn from_request(_req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Ok(State(state.as_ref().clone()))
    }
}
