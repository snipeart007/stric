pub mod connection;
pub mod connection_wrapper;
pub mod server;
pub mod server_config;
pub mod stream;
pub mod handler_types;
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}
// TODO: Change RwLock to parking_lot::RwLock
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
