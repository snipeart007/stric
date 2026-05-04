//! QUIC stream abstractions.
//!
//! This module provides wrappers around `quinn`'s unidirectional and bidirectional streams,
//! offering a simplified API for reading and writing data.

/// A unidirectional stream where the server is the sender.
pub struct ServerUniStream {
    pub(crate) stream: quinn::SendStream,
}

impl ServerUniStream {
    /// Writes data to the stream.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, quinn::WriteError> {
        self.stream.write(buf).await
    }

    /// Writes all data from the buffer to the stream.
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), quinn::WriteError> {
        self.stream.write_all(buf).await
    }

    /// Gracefully shuts down the transmit side of the stream.
    pub fn finish(&mut self) -> Result<(), quinn::ClosedStream> {
        self.stream.finish()
    }

    /// Returns a future that completes when the stream is stopped by the peer.
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
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, quinn::ReadError> {
        self.stream.read(buf).await
    }

    /// Reads the exact number of bytes required to fill the buffer.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), quinn::ReadExactError> {
        self.stream.read_exact(buf).await
    }

    /// Reads the remainder of the stream until the end.
    ///
    /// # Arguments
    /// * `size_limit` - The maximum number of bytes to read to prevent memory exhaustion.
    pub async fn read_to_end(
        &mut self,
        size_limit: usize,
    ) -> Result<Vec<u8>, quinn::ReadToEndError> {
        self.stream.read_to_end(size_limit).await
    }

    /// Sends a signal to the peer to stop sending data on this stream.
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
    pub server_initiated: bool,
    /// The receiving half of the stream.
    pub recv_stream: quinn::RecvStream,
    /// The sending half of the stream.
    pub send_stream: quinn::SendStream,
}

impl BiStream {
    // Write methods

    /// Writes data to the sending half of the stream.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, quinn::WriteError> {
        self.send_stream.write(buf).await
    }

    /// Writes all data from the buffer to the sending half of the stream.
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), quinn::WriteError> {
        self.send_stream.write_all(buf).await
    }

    /// Gracefully shuts down the transmit side of the stream.
    pub fn finish(&mut self) -> Result<(), quinn::ClosedStream> {
        self.send_stream.finish()
    }

    // Read methods

    /// Reads data from the receiving half of the stream.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, quinn::ReadError> {
        self.recv_stream.read(buf).await
    }

    /// Reads the exact number of bytes required to fill the buffer from the receiving half.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), quinn::ReadExactError> {
        self.recv_stream.read_exact(buf).await
    }

    /// Reads the remainder of the receiving half of the stream until the end.
    ///
    /// # Arguments
    /// * `size_limit` - The maximum number of bytes to read to prevent memory exhaustion.
    pub async fn read_to_end(
        &mut self,
        size_limit: usize,
    ) -> Result<Vec<u8>, quinn::ReadToEndError> {
        self.recv_stream.read_to_end(size_limit).await
    }
}
