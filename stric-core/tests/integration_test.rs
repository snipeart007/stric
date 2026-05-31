use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use stric_core::{ConnectionContext, NodeConfig, QuicNode};
use tokio::time::{Duration, sleep};

fn setup_crypto() {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    let _ = tracing_subscriber::fmt::try_init();
}

#[tokio::test]
async fn test_node_connection_lifecycle() {
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

    let config = NodeConfig {
        certs: Some(certs),
        key: Some(key),
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: None,
        danger_accept_invalid_certs: false,
    };

    // 2. Start Responder Node
    let (mut node, mut _error_rx) = QuicNode::<()>::new(config).unwrap();
    let node_addr = node.local_addr().unwrap();

    // Set a handler that just finishes immediately
    node.on_inbound(Arc::new(|_wrapper| Box::pin(async move { Ok(()) })));

    let node_arc = Arc::new(node);
    let node_clone = node_arc.clone();
    tokio::spawn(async move {
        node_clone.listen().await;
    });

    // 3. Start Initiator Node (Manual Client)
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
        .connect(node_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    // 4. Verify connection registered in manager
    sleep(Duration::from_millis(200)).await;

    let node_id = {
        let manager = node_arc.connection_manager();
        let id = *manager
            .store
            .iter()
            .next()
            .expect("Connection should be in manager")
            .key();
        id
    };

    {
        let manager = node_arc.connection_manager();
        assert!(manager.store.contains_key(&node_id));

        let conn_ref = manager.store.get(&node_id).unwrap();
        assert_eq!(conn_ref.context.id, node_id);
    }

    // 5. Test stream opening
    let uni = node_arc
        .get_unistream(&node_id)
        .await
        .expect("Node should be able to open uni stream");
    drop(uni);

    let bi = node_arc
        .get_bistream(&node_id)
        .await
        .expect("Node should be able to open bi stream");
    assert!(bi.is_responder_initiated());
}

#[tokio::test]
async fn test_node_symmetric_connect() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(
        cert_der.clone(),
    )];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();

    let config_responder = NodeConfig {
        certs: Some(certs),
        key: Some(key.clone_key()),
        socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: None,
        danger_accept_invalid_certs: false,
    };

    let (mut responder, _) = QuicNode::<()>::new(config_responder).unwrap();
    let responder_addr = responder.local_addr().unwrap();
    tokio::spawn(async move {
        responder.listen().await;
    });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots
        .add(quinn::rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();

    let config_initiator = NodeConfig {
        certs: None, // Not acting as responder
        key: None,
        socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: Some(roots),
        danger_accept_invalid_certs: false,
    };

    let (mut initiator, _) = QuicNode::<()>::new(config_initiator).unwrap();

    // Test outbound handler
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    initiator.on_outbound(Arc::new(move |_wrapper| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(()).await;
            Ok(())
        })
    }));

    initiator
        .connect(responder_addr, "localhost")
        .await
        .unwrap();

    // Verify outbound handler was triggered
    tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Outbound handler should have been triggered")
        .expect("Channel closed");
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

    let config = NodeConfig {
        certs: Some(certs),
        key: Some(key),
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: None,
        danger_accept_invalid_certs: false,
    };

    let (node, mut _error_rx) = QuicNode::<()>::new(config).unwrap();
    let node_addr = node.local_addr().unwrap();
    let node_arc = Arc::new(node);
    let node_clone = node_arc.clone();
    tokio::spawn(async move {
        node_clone.listen().await;
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
        .connect(node_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    let node_id = {
        let manager = node_arc.connection_manager();
        *manager
            .store
            .iter()
            .next()
            .expect("Connection should be in manager")
            .key()
    };

    // Test updating context flags via manager
    {
        let manager = node_arc.connection_manager();
        manager.set_initiator_uni(node_id, true).unwrap();
        manager.set_responder_bi(node_id, true).unwrap();

        let conn_ref = manager.store.get(&node_id).unwrap();
        assert!(conn_ref.context.initiator_uni);
        assert!(conn_ref.context.responder_bi);
        assert!(!conn_ref.context.initiator_bi);
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

    let config = NodeConfig {
        certs: Some(certs),
        key: Some(key),
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: None,
        danger_accept_invalid_certs: false,
    };

    let (mut node, mut error_rx) = QuicNode::<()>::new(config).unwrap();
    let node_addr = node.local_addr().unwrap();

    node.on_inbound(Arc::new(|_wrapper| {
        Box::pin(async move { Err(anyhow::anyhow!("Handler intentional failure")) })
    }));

    let node_arc = Arc::new(node);
    let node_clone = node_arc.clone();
    tokio::spawn(async move {
        node_clone.listen().await;
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
        .connect(node_addr, "localhost")
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
    let manager = node_arc.connection_manager();
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

    let config = NodeConfig {
        certs: Some(certs),
        key: Some(key),
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: None,
        danger_accept_invalid_certs: false,
    };

    let (mut node, mut _error_rx) = QuicNode::<MyMetadata>::new(config).unwrap();
    let node_addr = node.local_addr().unwrap();

    node.on_inbound(Arc::new(|wrapper| {
        wrapper.metadata.name = "StricTest".to_string();
        Box::pin(async move { Ok(()) })
    }));

    let node_arc = Arc::new(node);
    let node_clone = node_arc.clone();
    tokio::spawn(async move {
        node_clone.listen().await;
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
        .connect(node_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    let manager = node_arc.connection_manager();
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

    let config = NodeConfig {
        certs: Some(certs),
        key: Some(key),
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: context,
        keep_alive_limit_per_thread: 10,
        idle_timeout: Some(Duration::from_secs(1)), // Short timeout for fast testing
        root_cert_store: None,
        danger_accept_invalid_certs: false,
    };

    let (node, mut _error_rx) = QuicNode::<()>::new(config).unwrap();
    let node_addr = node.local_addr().unwrap();
    let node_arc = Arc::new(node);
    let node_clone = node_arc.clone();
    tokio::spawn(async move {
        node_clone.listen().await;
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
        .connect(node_addr, "localhost")
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

#[tokio::test]
async fn test_node_insecure_connect() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(
        cert_der.clone(),
    )];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();

    let config_responder = NodeConfig {
        certs: Some(certs),
        key: Some(key.clone_key()),
        socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: None,
        danger_accept_invalid_certs: false,
    };

    let (mut responder, _) = QuicNode::<()>::new(config_responder).unwrap();
    let responder_addr = responder.local_addr().unwrap();
    tokio::spawn(async move {
        responder.listen().await;
    });

    let config_initiator = NodeConfig {
        certs: None,
        key: None,
        socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: None,
        danger_accept_invalid_certs: true,
    };

    let (initiator, _) = QuicNode::<()>::new(config_initiator).unwrap();

    let res = initiator.connect(responder_addr, "localhost").await;
    assert!(
        res.is_ok(),
        "Insecure connection should have succeeded: {:?}",
        res.err()
    );
}
