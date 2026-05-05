use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use stric_core::{ConnectionContext, ServerConfig, ServerInstance};
use tokio::time::{Duration, sleep};

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

    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(
        cert_der.clone(),
    )];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    // 2. Start Server
    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();

    // Set a handler that just finishes immediately
    server.register_connection_handler(Arc::new(|_wrapper| Box::pin(async move { Ok(()) })));

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    // 3. Start Client
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots
        .add(quinn::rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();

    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"stric".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
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
        let manager = server_arc.connection_manager();
        let id = *manager
            .store
            .iter()
            .next()
            .expect("Connection should be in manager")
            .key();
        println!("Test: Found server-side connection ID: {}", id);
        id
    };

    {
        let manager = server_arc.connection_manager();
        assert!(manager.store.contains_key(&server_id));

        let conn_ref = manager.store.get(&server_id).unwrap();
        assert_eq!(conn_ref.context.id, server_id);
    }

    // 5. Test stream opening from server side
    // Server opens a uni stream to client
    let server_uni = server_arc
        .get_unistream(&server_id)
        .await
        .expect("Server should be able to open uni stream");
    drop(server_uni);

    // Server opens a bi stream
    let server_bi = server_arc
        .get_bistream(&server_id)
        .await
        .expect("Server should be able to open bi stream");
        assert!(server_bi.is_server_initiated());
}

#[tokio::test]
async fn test_connection_manager_updates() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(
        cert_der.clone(),
    )];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    let (server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();
    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots
        .add(quinn::rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();

    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"stric".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let _connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    let server_id = {
        let manager = server_arc.connection_manager();
        *manager
            .store
            .iter()
            .next()
            .expect("Connection should be in manager")
            .key()
    };

    // Test updating context flags via manager
    {
        let manager = server_arc.connection_manager();
        manager.set_client_uni(server_id, true).unwrap();
        manager.set_server_bi(server_id, true).unwrap();

        let conn_ref = manager.store.get(&server_id).unwrap();
        assert!(conn_ref.context.client_uni);
        assert!(conn_ref.context.server_bi);
        assert!(!conn_ref.context.client_bi);
    }
}

#[tokio::test]
async fn test_error_channel_and_handler_failure() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(
        cert_der.clone(),
    )];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    let (mut server, mut error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();

    server.register_connection_handler(Arc::new(|_wrapper| {
        Box::pin(async move { Err(anyhow::anyhow!("Handler intentional failure")) })
    }));

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots
        .add(quinn::rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"stric".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
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
    let manager = server_arc.connection_manager();
    assert_eq!(
        manager.store.len(),
        0,
        "Connection should not be in manager after handler failure"
    );
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
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(
        cert_der.clone(),
    )];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    let (mut server, mut _error_rx) = ServerInstance::<MyMetadata>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();

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
    roots
        .add(quinn::rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"stric".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let _connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    let manager = server_arc.connection_manager();
    let id = *manager.store.iter().next().unwrap().key();
    let conn_ref = manager.store.get(&id).unwrap();
    assert_eq!(conn_ref.metadata.name, "StricTest");
}

#[tokio::test]
async fn test_keep_alive_ping() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(
        cert_der.clone(),
    )];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let mut context = ConnectionContext::default();
    context.keep_alive = true;

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: context,
        keep_alive_limit_per_thread: 10,
        idle_timeout: Some(Duration::from_secs(1)), // Short timeout for fast testing
    };

    let (server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();
    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots
        .add(quinn::rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"stric".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    // Client accepts the uni stream opened by the server for keep-alive
    let mut uni_stream = connection.accept_uni().await.unwrap();

    // Read the first ping
    let mut buf = [0u8; 4];
    uni_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    // Read the second ping to ensure it's periodic
    uni_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
}
