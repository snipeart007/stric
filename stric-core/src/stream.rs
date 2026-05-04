pub struct ServerUniStream {
    pub(crate) stream: quinn::SendStream,
}

impl ServerUniStream {
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, quinn::WriteError> {
        self.stream.write(buf).await
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), quinn::WriteError> {
        self.stream.write_all(buf).await
    }

    pub fn finish(&mut self) -> Result<(), quinn::ClosedStream> {
        self.stream.finish()
    }

    pub async fn stopped(&mut self) -> Result<Option<quinn::VarInt>, quinn::StoppedError> {
        self.stream.stopped().await
    }
}

pub struct ClientUniStream {
    pub(crate) stream: quinn::RecvStream,
}

impl ClientUniStream {
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, quinn::ReadError> {
        self.stream.read(buf).await
    }

    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), quinn::ReadExactError> {
        self.stream.read_exact(buf).await
    }

    pub async fn read_to_end(
        &mut self,
        size_limit: usize,
    ) -> Result<Vec<u8>, quinn::ReadToEndError> {
        self.stream.read_to_end(size_limit).await
    }

    pub fn stop(&mut self, error_code: quinn::VarInt) -> Result<(), quinn::ClosedStream> {
        self.stream.stop(error_code)
    }
}

pub struct BiStream {
    // True if server initiated, false if client initiated
    pub server_initiated: bool,
    pub recv_stream: quinn::RecvStream,
    pub send_stream: quinn::SendStream,
}

impl BiStream {
    // Write methods
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, quinn::WriteError> {
        self.send_stream.write(buf).await
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), quinn::WriteError> {
        self.send_stream.write_all(buf).await
    }

    pub fn finish(&mut self) -> Result<(), quinn::ClosedStream> {
        self.send_stream.finish()
    }

    // Read methods
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, quinn::ReadError> {
        self.recv_stream.read(buf).await
    }

    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), quinn::ReadExactError> {
        self.recv_stream.read_exact(buf).await
    }

    pub async fn read_to_end(
        &mut self,
        size_limit: usize,
    ) -> Result<Vec<u8>, quinn::ReadToEndError> {
        self.recv_stream.read_to_end(size_limit).await
    }
}
