use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Writes a Protobuf message to an asynchronous writer, prefixed with its length as a 4-byte big-endian u32.
pub async fn write_frame<W, T>(writer: &mut W, msg: &T) -> Result<(), anyhow::Error>
where
    W: AsyncWriteExt + Unpin,
    T: prost::Message,
{
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf)?;
    let len = buf.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads a length-prefixed payload from an asynchronous reader.
/// Enforces the 16 MiB maximum frame size limit to prevent memory exhaustion attacks.
pub async fn read_frame<R>(reader: &mut R) -> Result<Vec<u8>, anyhow::Error>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    
    if len > 16_777_216 {
        return Err(anyhow::anyhow!("Frame length {} exceeds maximum size of 16 MiB", len));
    }
    
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Ping;

    #[tokio::test]
    async fn test_write_and_read_frame() {
        let mut buffer = Vec::new();
        let ping = Ping { sent_at: 42 };

        write_frame(&mut buffer, &ping).await.unwrap();

        let mut reader = &buffer[..];
        let bytes = read_frame(&mut reader).await.unwrap();

        let decoded = <Ping as prost::Message>::decode(&bytes[..]).unwrap();
        assert_eq!(decoded.sent_at, 42);
    }
}
