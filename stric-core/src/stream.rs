//! QUIC stream abstractions.
//!
//! This module provides wrappers around `quinn`'s unidirectional and bidirectional streams,
//! offering a simplified API for reading and writing data.

/// A unidirectional stream where the server is the sender.
pub struct ServerUniStream {
    pub(crate) stream: quinn::SendStream,
}

impl ServerUniStream {
    pub(crate) fn new(stream: quinn::SendStream) -> Self {
        Self { stream }
    }

    /// Writes data to the stream.
    ///
    /// # Errors
    /// Propagates `quinn::WriteError` when the peer stops the stream, the
    /// connection closes, or flow control prevents the write from completing.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, quinn::WriteError> {
        self.stream.write(buf).await
    }

    /// Writes all data from the buffer to the stream.
    ///
    /// # Errors
    /// Propagates `quinn::WriteError` under the same conditions as [`write`](Self::write).
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), quinn::WriteError> {
        self.stream.write_all(buf).await
    }

    /// Gracefully shuts down the transmit side of the stream.
    ///
    /// # Errors
    /// Returns `quinn::ClosedStream` when the stream has already been closed.
    pub fn finish(&mut self) -> Result<(), quinn::ClosedStream> {
        self.stream.finish()
    }

    /// Returns a future that completes when the stream is stopped by the peer.
    ///
    /// # Errors
    /// Returns `quinn::StoppedError` if the stream cannot observe the peer stop state.
    pub async fn stopped(&mut self) -> Result<Option<quinn::VarInt>, quinn::StoppedError> {
        self.stream.stopped().await
    }
}

/// A unidirectional stream where the client is the receiver.
pub struct ClientUniStream {
    pub(crate) stream: quinn::RecvStream,
}

impl ClientUniStream {
    /// Reads data from the stream into the provided buffer.
    ///
    /// # Errors
    /// Propagates `quinn::ReadError` when the stream or connection fails.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, quinn::ReadError> {
        self.stream.read(buf).await
    }

    /// Reads the exact number of bytes required to fill the buffer.
    ///
    /// # Errors
    /// Returns `quinn::ReadExactError` when EOF or another stream failure
    /// occurs before the buffer is filled.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), quinn::ReadExactError> {
        self.stream.read_exact(buf).await
    }

    /// Reads the remainder of the stream until the end.
    ///
    /// # Arguments
    /// * `size_limit` - The maximum number of bytes to read to prevent memory exhaustion.
    ///
    /// # Errors
    /// Returns `quinn::ReadToEndError` when the peer exceeds `size_limit` or
    /// when the stream fails before completion.
    pub async fn read_to_end(
        &mut self,
        size_limit: usize,
    ) -> Result<Vec<u8>, quinn::ReadToEndError> {
        self.stream.read_to_end(size_limit).await
    }

    /// Sends a signal to the peer to stop sending data on this stream.
    ///
    /// # Errors
    /// Returns `quinn::ClosedStream` when the receiving side has already closed.
    pub fn stop(&mut self, error_code: quinn::VarInt) -> Result<(), quinn::ClosedStream> {
        self.stream.stop(error_code)
    }
}

/// A bidirectional QUIC stream.
///
/// Bidirectional streams allow both peers to send and receive data simultaneously.
/// In QUIC, a bidirectional stream consists of a `SendStream` and a `RecvStream`.
pub struct BiStream {
    /// Whether the stream was initiated by the server.
    server_initiated: bool,
    recv_stream: quinn::RecvStream,
    send_stream: quinn::SendStream,
}

impl BiStream {
    /// Creates a new bidirectional stream wrapper.
    ///
    /// This constructor is primarily intended for transport integrations such as
    /// `stric-tower`, not for ordinary application code.
    pub fn new(
        server_initiated: bool,
        send_stream: quinn::SendStream,
        recv_stream: quinn::RecvStream,
    ) -> Self {
        Self {
            server_initiated,
            recv_stream,
            send_stream,
        }
    }

    /// Returns `true` when this stream was opened by the server side of the connection.
    pub fn is_server_initiated(&self) -> bool {
        self.server_initiated
    }

    /// Writes data to the sending half of the stream.
    ///
    /// # Errors
    /// Propagates `quinn::WriteError` when the stream or connection fails.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, quinn::WriteError> {
        self.send_stream.write(buf).await
    }

    /// Writes all data from the buffer to the sending half of the stream.
    ///
    /// # Errors
    /// Propagates `quinn::WriteError` under the same conditions as [`write`](Self::write).
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), quinn::WriteError> {
        self.send_stream.write_all(buf).await
    }

    /// Gracefully shuts down the transmit side of the stream.
    ///
    /// # Errors
    /// Returns `quinn::ClosedStream` when the send half is already closed.
    pub fn finish(&mut self) -> Result<(), quinn::ClosedStream> {
        self.send_stream.finish()
    }

    /// Reads data from the receiving half of the stream.
    ///
    /// # Errors
    /// Propagates `quinn::ReadError` when the stream or connection fails.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, quinn::ReadError> {
        self.recv_stream.read(buf).await
    }

    /// Reads the exact number of bytes required to fill the buffer from the receiving half.
    ///
    /// # Errors
    /// Returns `quinn::ReadExactError` when EOF or another stream failure
    /// occurs before the buffer is filled.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), quinn::ReadExactError> {
        self.recv_stream.read_exact(buf).await
    }

    /// Reads the remainder of the receiving half of the stream until the end.
    ///
    /// # Arguments
    /// * `size_limit` - The maximum number of bytes to read to prevent memory exhaustion.
    ///
    /// # Errors
    /// Returns `quinn::ReadToEndError` when the peer exceeds `size_limit` or
    /// when the stream fails before completion.
    pub async fn read_to_end(
        &mut self,
        size_limit: usize,
    ) -> Result<Vec<u8>, quinn::ReadToEndError> {
        self.recv_stream.read_to_end(size_limit).await
    }
}
