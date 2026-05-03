use stric::{
    connection_wrapper::ConnectionContext, server::ServerInstance, server_config::ServerConfig,
};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

fn setup_crypto() {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
}

#[tokio::test]
async fn test_server_connection_lifecycle() {
    setup_crypto();
    // 1. Generate self-signed certs
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
    };

    // 2. Start Server
    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.endpoint.local_addr().unwrap();

    // Set a handler that just finishes immediately
    server.register_connection_handler(Arc::new(|_wrapper| {
        Box::pin(async move { Ok(()) })
    }));

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    // 3. Start Client
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der)).unwrap();
    
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let _connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    // 4. Verify connection registered in manager
    // Give some time for handle_incoming to run
    sleep(Duration::from_millis(200)).await;

    let server_id = {
        let manager = server_arc.conn_manager.read().await;
        let id = *manager.store.keys().next().expect("Connection should be in manager");
        println!("Test: Found server-side connection ID: {}", id);
        id
    };

    {
        let manager = server_arc.conn_manager.read().await;
        assert!(manager.store.contains_key(&server_id));
        
        let conn_lock = manager.store.get(&server_id).unwrap();
        let conn_wrapper = conn_lock.lock().await;
        assert_eq!(conn_wrapper.context.id, server_id);
    }

    // 5. Test stream opening from server side
    // Server opens a uni stream to client
    let server_uni = server_arc.get_unistream(&server_id).await.expect("Server should be able to open uni stream");
    drop(server_uni);

    // Server opens a bi stream
    let server_bi = server_arc.get_bistream(&server_id).await.expect("Server should be able to open bi stream");
    assert!(server_bi.server_initiated);
}

#[tokio::test]
async fn test_connection_manager_updates() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
    };

    let (server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.endpoint.local_addr().unwrap();
    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der)).unwrap();
    
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let _connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    let server_id = {
        let manager = server_arc.conn_manager.read().await;
        *manager.store.keys().next().expect("Connection should be in manager")
    };

    // Test updating context flags via manager
    {
        let manager = server_arc.conn_manager.read().await;
        manager.set_client_uni(server_id, true).await.unwrap();
        manager.set_server_bi(server_id, true).await.unwrap();
        
        let conn_lock = manager.store.get(&server_id).unwrap();
        let conn_wrapper = conn_lock.lock().await;
        assert!(conn_wrapper.context.client_uni);
        assert!(conn_wrapper.context.server_bi);
        assert!(!conn_wrapper.context.client_bi);
    }
}

#[tokio::test]
async fn test_error_channel_and_handler_failure() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
    };

    let (mut server, mut error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.endpoint.local_addr().unwrap();
    
    server.register_connection_handler(Arc::new(|_wrapper| {
        Box::pin(async move { Err(anyhow::anyhow!("Handler intentional failure")) })
    }));

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der)).unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let _connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    // Check for error in channel
    let err = tokio::time::timeout(Duration::from_secs(1), error_rx.recv())
        .await
        .expect("Timeout waiting for error")
        .expect("Error channel closed");
    
    assert_eq!(err.to_string(), "Handler intentional failure");

    // Verify connection NOT in manager
    let manager = server_arc.conn_manager.read().await;
    assert_eq!(manager.store.len(), 0, "Connection should not be in manager after handler failure");
}

#[derive(Default)]
struct MyMetadata {
    pub name: String,
}

#[tokio::test]
async fn test_custom_metadata() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
    };

    let (mut server, mut _error_rx) = ServerInstance::<MyMetadata>::new(config).unwrap();
    let server_addr = server.endpoint.local_addr().unwrap();
    
    server.register_connection_handler(Arc::new(|wrapper| {
        wrapper.metadata.name = "StricTest".to_string();
        Box::pin(async move { Ok(()) })
    }));

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der)).unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let _connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    let manager = server_arc.conn_manager.read().await;
    let id = *manager.store.keys().next().unwrap();
    let conn_lock = manager.store.get(&id).unwrap();
    let conn_wrapper = conn_lock.lock().await;
    assert_eq!(conn_wrapper.metadata.name, "StricTest");
}
