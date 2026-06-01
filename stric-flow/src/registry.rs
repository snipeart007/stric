use std::any::Any;
use std::collections::HashMap;

/// A registry for message parsers that allows dynamically registering and decoding
/// typed messages received over the network.
///
/// It stores parsers by their string message type identifier, enabling the application
/// to deserialize raw byte payloads into dynamic `Any` types that can be downcast
/// to their original concrete type.
pub struct MessageRegistry {
    parsers: HashMap<
        String,
        Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send + Sync>, String> + Send + Sync>,
    >,
}

impl MessageRegistry {
    /// Creates a new, empty `MessageRegistry`.
    pub fn new() -> Self {
        Self {
            parsers: HashMap::new(),
        }
    }

    /// Registers a parser function for a specific message type.
    ///
    /// The parser function must take a slice of bytes and return a `Result` containing
    /// the parsed message type `T` or a string error description.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The concrete message type being registered. It must implement `Send`, `Sync`, and `'static`.
    ///
    /// # Arguments
    ///
    /// * `message_type` - A unique string identifier for this message type.
    /// * `parser` - A function pointer that converts a byte slice into a parsed `T`.
    pub fn register<T>(&mut self, message_type: &str, parser: fn(&[u8]) -> Result<T, String>)
    where
        T: Send + Sync + 'static,
    {
        self.parsers.insert(
            message_type.to_string(),
            Box::new(move |bytes| {
                let parsed = parser(bytes)?;
                Ok(Box::new(parsed) as Box<dyn Any + Send + Sync>)
            }),
        );
    }

    /// Decodes a raw byte payload of a given message type into a dynamically typed box.
    ///
    /// Returns the parsed message wrapped as `Box<dyn Any + Send + Sync>` which can
    /// subsequently be downcast to the concrete type registered for `message_type`.
    ///
    /// # Errors
    ///
    /// Returns a `Result::Err` containing a string error if no parser is registered
    /// for the specified `message_type`, or if the parser fails to decode the payload.
    pub fn decode(&self, message_type: &str, data: &[u8]) -> Result<Box<dyn Any + Send + Sync>, String> {
        let parser = self
            .parsers
            .get(message_type)
            .ok_or_else(|| format!("No parser registered for message type: {}", message_type))?;
        parser(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_registry() {
        let mut registry = MessageRegistry::new();
        
        // Register a parser for type "test_msg"
        registry.register::<String>("test_msg", |bytes| {
            String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
        });

        // Decode
        let decoded = registry.decode("test_msg", b"hello").unwrap();
        let val = decoded.downcast_ref::<String>().unwrap();
        assert_eq!(val, "hello");

        // Try decoding unregistered
        assert!(registry.decode("unknown", b"hello").is_err());
    }
}
