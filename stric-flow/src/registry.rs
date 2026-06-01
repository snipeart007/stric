use std::any::Any;
use std::collections::HashMap;

pub struct MessageRegistry {
    parsers: HashMap<
        String,
        Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send + Sync>, String> + Send + Sync>,
    >,
}

impl MessageRegistry {
    pub fn new() -> Self {
        Self {
            parsers: HashMap::new(),
        }
    }

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
