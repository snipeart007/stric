use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use dashmap::DashMap;
use prost::Message;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use stric_core::{ConnectionWrapper, NodeConfig, QuicNode};

use crate::error::FlowError;
use crate::frame::{read_frame, write_frame};
use crate::proto::{
    self, ControlMessage, Envelope, FlowHandshake, HandshakeAck,
    NodeDescriptor, NodeRole, Pong, SubscriptionEntry, SubscriptionUpdate,
};
use crate::routing::{match_topic, GlobalGraph};
use crate::registry::MessageRegistry;
use crate::reconciliation::Session;

/// Metadata associated with a connection in stric-flow.
#[derive(Default, Clone, Debug)]
pub struct FlowConnectionMetadata {
    pub peer_node_id: Option<String>,
}

/// A handler trait for incoming application messages matching a subscribed topic.
#[async_trait]
pub trait FlowHandler: Send + Sync + 'static {
    async fn handle_message(
        &self,
        flow_id: &str,
        topic_id: &str,
        message: Box<dyn Any + Send + Sync>,
    ) -> Result<(), String>;
}

/// A pluggable metric for estimating routing costs between nodes.
pub trait RoutingMetric: Send + Sync + 'static {
    fn estimate_cost(&self, node_a: &str, node_b: &str, base_hop_cost: u32, rtt_micros: u64) -> u32;
}

pub struct HopCountMetric;

impl RoutingMetric for HopCountMetric {
    fn estimate_cost(&self, _node_a: &str, _node_b: &str, base_hop_cost: u32, _rtt_micros: u64) -> u32 {
        base_hop_cost
    }
}

/// The main stric-flow Node coordinating control and data messaging.
pub struct FlowNode {
    node_id: String,
    core: Arc<QuicNode<FlowConnectionMetadata>>,
    graph: Arc<RwLock<GlobalGraph>>,
    sessions: Arc<DashMap<String, Session>>,
    topic_handlers: Arc<DashMap<String, Arc<dyn FlowHandler>>>,
    metric: Arc<dyn RoutingMetric>,
    registry: Arc<MessageRegistry>,
    peer_writers: Arc<DashMap<String, mpsc::Sender<ControlMessage>>>,
    local_subscriptions: Arc<RwLock<HashSet<String>>>,
    control_tx: mpsc::Sender<ControlEvent>,
}

enum ControlEvent {
    PeerConnected {
        node_id: String,
        tx: mpsc::Sender<ControlMessage>,
    },
    PeerDisconnected {
        node_id: String,
    },
    ControlMsgReceived {
        from: String,
        msg: ControlMessage,
    },
}

impl FlowNode {
    /// Creates a new `FlowNode` instance and begins listening/handshaking logic.
    pub fn new(
        node_id: String,
        mut config: NodeConfig,
        metric: Arc<dyn RoutingMetric>,
        registry: Arc<MessageRegistry>,
    ) -> Result<(Arc<Self>, mpsc::Receiver<anyhow::Error>), FlowError> {
        let (conn_tx, mut conn_rx) = mpsc::channel::<(ConnectionWrapper<FlowConnectionMetadata>, bool)>(50);

        // Bind core node callbacks
        let inbound_tx = conn_tx.clone();
        let outbound_tx = conn_tx.clone();

        let inbound_handler = Arc::new(move |conn_wrapper: &mut ConnectionWrapper<FlowConnectionMetadata>| {
            let tx = inbound_tx.clone();
            let conn = conn_wrapper.clone();
            Box::pin(async move {
                let _ = tx.send((conn, false)).await;
                Ok(())
            }) as Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>>
        });

        let outbound_handler = Arc::new(move |conn_wrapper: &mut ConnectionWrapper<FlowConnectionMetadata>| {
            let tx = outbound_tx.clone();
            let conn = conn_wrapper.clone();
            Box::pin(async move {
                let _ = tx.send((conn, true)).await;
                Ok(())
            }) as Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>>
        });

        // Set ALPN protocol to ensure connection compatibility
        if config.alpn_protocol_names.is_empty() {
            config.alpn_protocol_names = vec![b"stric-flow".to_vec()];
        }

        let (mut core_node, error_rx) = QuicNode::<FlowConnectionMetadata>::new(config)?;
        core_node.on_inbound(inbound_handler);
        core_node.on_outbound(outbound_handler);

        let core = Arc::new(core_node);
        let (control_tx, control_rx) = mpsc::channel(100);

        let node = Arc::new(Self {
            node_id: node_id.clone(),
            core,
            graph: Arc::new(RwLock::new(GlobalGraph::new())),
            sessions: Arc::new(DashMap::new()),
            topic_handlers: Arc::new(DashMap::new()),
            metric,
            registry,
            peer_writers: Arc::new(DashMap::new()),
            local_subscriptions: Arc::new(RwLock::new(HashSet::new())),
            control_tx,
        });

        // Spawn connection manager loop to process incoming connections
        let node_clone = node.clone();
        tokio::spawn(async move {
            while let Some((conn, is_initiator)) = conn_rx.recv().await {
                let node_ref = node_clone.clone();
                tokio::spawn(async move {
                    if let Err(e) = node_ref.handle_new_connection(conn, is_initiator).await {
                        error!("Failed handling connection: {}", e);
                    }
                });
            }
        });

        // Run the main control task coordinating mesh topology and routing updates
        let node_clone = node.clone();
        tokio::spawn(async move {
            node_clone.run_control_loop(control_rx).await;
        });

        Ok((node, error_rx))
    }

    /// Starts the node's underlying listener.
    pub async fn start(&self) {
        let core = self.core.clone();
        tokio::spawn(async move {
            core.listen().await;
        });
    }

    /// Connects to a remote peer node.
    pub async fn connect(&self, addr: SocketAddr, server_name: &str) -> Result<u64, FlowError> {
        Ok(self.core.connect(addr, server_name).await?)
    }

    /// Handles a newly established physical connection, negotiating the control stream.
    async fn handle_new_connection(
        &self,
        conn: ConnectionWrapper<FlowConnectionMetadata>,
        is_initiator: bool,
    ) -> Result<(), anyhow::Error> {
        debug!(
            "Starting control stream negotiation. ID: {}, Initiator: {}",
            conn.context.id, is_initiator
        );

        // Open or accept the bidirectional Control Stream
        let (mut send_stream, mut recv_stream) = if is_initiator {
            conn.conn.open_bi().await?
        } else {
            conn.conn.accept_bi().await?
        };

        // Phase 2: Exchange FlowHandshake
        let local_subs = self.local_subscriptions.read().await;
        let handshake = ControlMessage {
            message: Some(proto::control_message::Message::Handshake(FlowHandshake {
                protocol_version: 1,
                node_id: self.node_id.clone(),
                role: NodeRole::Flow as i32,
                capabilities: HashMap::new(),
                supported_codecs: vec!["protobuf".to_string(), "json".to_string()],
                subscribed_topics: local_subs.iter().cloned().collect(),
                identity_mode: proto::IdentityMode::IdentityTlsDerived as i32,
            })),
        };
        write_frame(&mut send_stream, &handshake).await?;

        // Read peer's FlowHandshake
        let peer_handshake_bytes = read_frame(&mut recv_stream).await?;
        let peer_msg = ControlMessage::decode(&peer_handshake_bytes[..])?;
        let peer_handshake = match peer_msg.message {
            Some(proto::control_message::Message::Handshake(h)) => h,
            _ => return Err(anyhow::anyhow!("Expected FlowHandshake")),
        };

        if peer_handshake.protocol_version != 1 {
            let reject = ControlMessage {
                message: Some(proto::control_message::Message::HandshakeAck(HandshakeAck {
                    accepted: false,
                    reject_reason: "Unsupported protocol version".to_string(),
                    protocol_version: 1,
                })),
            };
            let _ = write_frame(&mut send_stream, &reject).await;
            return Err(anyhow::anyhow!("Protocol version mismatch"));
        }

        let peer_id = peer_handshake.node_id.clone();

        // Send HandshakeAck
        let ack = ControlMessage {
            message: Some(proto::control_message::Message::HandshakeAck(HandshakeAck {
                accepted: true,
                reject_reason: String::new(),
                protocol_version: 1,
            })),
        };
        write_frame(&mut send_stream, &ack).await?;

        // Read peer's HandshakeAck
        let ack_bytes = read_frame(&mut recv_stream).await?;
        let peer_ack = ControlMessage::decode(&ack_bytes[..])?;
        let ack_payload = match peer_ack.message {
            Some(proto::control_message::Message::HandshakeAck(a)) => a,
            _ => return Err(anyhow::anyhow!("Expected HandshakeAck")),
        };

        if !ack_payload.accepted {
            return Err(anyhow::anyhow!("Handshake rejected: {}", ack_payload.reject_reason));
        }

        info!("Control stream successfully negotiated with peer {}", peer_id);

        // Register peer writer
        let (peer_control_tx, mut peer_control_rx) = mpsc::channel::<ControlMessage>(100);
        let _ = self.control_tx.send(ControlEvent::PeerConnected {
            node_id: peer_id.clone(),
            tx: peer_control_tx,
        }).await;

        // Spawn writer loop
        tokio::spawn(async move {
            while let Some(msg) = peer_control_rx.recv().await {
                if let Err(e) = write_frame(&mut send_stream, &msg).await {
                    error!("Failed writing to peer control stream: {}", e);
                    break;
                }
            }
        });

        // Spawn reader loop
        let control_tx_clone = self.control_tx.clone();
        let peer_id_clone = peer_id.clone();
        tokio::spawn(async move {
            loop {
                match read_frame(&mut recv_stream).await {
                    Ok(bytes) => {
                        if let Ok(msg) = ControlMessage::decode(&bytes[..]) {
                            let _ = control_tx_clone.send(ControlEvent::ControlMsgReceived {
                                from: peer_id_clone.clone(),
                                msg,
                            }).await;
                        }
                    }
                    Err(e) => {
                        warn!("Control stream closed for peer {}: {}", peer_id_clone, e);
                        let _ = control_tx_clone.send(ControlEvent::PeerDisconnected {
                            node_id: peer_id_clone.clone(),
                        }).await;
                        break;
                    }
                }
            }
        });

        // Spawn incoming data streams listener task
        let conn_clone = conn.conn.clone();
        let registry = self.registry.clone();
        let handlers = self.topic_handlers.clone();
        tokio::spawn(async move {
            while let Ok(mut recv) = conn_clone.accept_uni().await {
                let registry_ref = registry.clone();
                let handlers_ref = handlers.clone();
                tokio::spawn(async move {
                    if let Ok(bytes) = read_frame(&mut recv).await {
                        if let Ok(envelope) = Envelope::decode(&bytes[..]) {
                            if let Some(header) = &envelope.header {
                                // Dynamic decoding for local delivery
                                if let Ok(decoded) = registry_ref.decode(&envelope.message_type, &envelope.payload) {
                                    let mut matched_handler = None;
                                    for entry in handlers_ref.iter() {
                                        if match_topic(entry.key(), &header.topic_id) {
                                            matched_handler = Some(entry.value().clone());
                                            break;
                                        }
                                    }
                                    if let Some(handler) = matched_handler {
                                        let _ = handler.handle_message(&header.flow_id, &header.topic_id, decoded).await;
                                    }
                                }
                            }
                        }
                    }
                });
            }
        });

        Ok(())
    }

    /// Registers a handler for a topic.
    pub fn subscribe(&self, topic_pattern: String, handler: Arc<dyn FlowHandler>) {
        info!("Registering handler for subscription pattern: {}", topic_pattern);
        self.topic_handlers.insert(topic_pattern.clone(), handler);
        
        let local_subs = self.local_subscriptions.clone();
        let control_tx = self.control_tx.clone();
        let node_id = self.node_id.clone();
        tokio::spawn(async move {
            let mut subs = local_subs.write().await;
            subs.insert(topic_pattern);
            
            // Broadcast SubscriptionUpdate to all peers
            let update = ControlMessage {
                message: Some(proto::control_message::Message::SubscriptionUpdate(SubscriptionUpdate {
                    node_id: node_id.clone(),
                    entries: subs.iter().map(|pattern| SubscriptionEntry {
                        flow_id: String::new(),
                        pattern: pattern.clone(),
                        action: proto::SubscriptionAction::Subscribe as i32,
                    }).collect(),
                })),
            };
            let _ = control_tx.send(ControlEvent::ControlMsgReceived {
                from: node_id,
                msg: update,
            }).await;
        });
    }

    /// Publishes a message to a topic.
    pub async fn publish(
        &self,
        flow_id: String,
        topic_id: String,
        message_type: String,
        codec: proto::Codec,
        payload: Vec<u8>,
    ) -> Result<(), FlowError> {
        let graph = self.graph.read().await;

        let mut subscribers = HashSet::new();
        for (node_id, desc) in &graph.node_metadata {
            for pattern in &desc.capabilities {
                if pattern.0.starts_with("sub:") && match_topic(&pattern.0[4..], &topic_id) {
                    subscribers.insert(node_id.clone());
                }
            }
        }

        let local_subs = self.local_subscriptions.read().await;
        let mut local_interested = false;
        for pattern in &*local_subs {
            if match_topic(pattern, &topic_id) {
                local_interested = true;
                break;
            }
        }

        if local_interested {
            if let Ok(decoded) = self.registry.decode(&message_type, &payload) {
                let mut matched_handler = None;
                for entry in self.topic_handlers.iter() {
                    if match_topic(entry.key(), &topic_id) {
                        matched_handler = Some(entry.value().clone());
                        break;
                    }
                }
                if let Some(handler) = matched_handler {
                    let _ = handler.handle_message(&flow_id, &topic_id, decoded).await;
                }
            }
        }

        if subscribers.is_empty() {
            return Ok(());
        }

        let forwarding_table = graph.compute_forwarding_table(&self.node_id, &subscribers);

        let routing_header = proto::RoutingHeader {
            source_node_id: self.node_id.clone(),
            flow_id,
            topic_id,
            session_id: String::new(),
            nonce: rand::random::<u128>().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            deadline: 0,
            delivery_mode: proto::DeliveryMode::DeliveryGuaranteed as i32,
            forwarding_table,
        };

        let envelope = Envelope {
            header: Some(routing_header),
            message_type,
            codec: codec as i32,
            payload,
        };

        if let Some(header) = &envelope.header {
            if let Some(targets) = header.forwarding_table.get(&self.node_id) {
                for next_hop in &targets.send_to {
                    debug!("Forwarding data envelope to next hop: {}", next_hop);
                }
            }
        }

        Ok(())
    }

    async fn run_control_loop(&self, mut rx: mpsc::Receiver<ControlEvent>) {
        while let Some(event) = rx.recv().await {
            match event {
                ControlEvent::PeerConnected { node_id, tx } => {
                    self.peer_writers.insert(node_id.clone(), tx);
                    info!("Peer registered in control engine: {}", node_id);
                }
                ControlEvent::PeerDisconnected { node_id } => {
                    self.peer_writers.remove(&node_id);
                    info!("Peer deregistered from control engine: {}", node_id);
                }
                ControlEvent::ControlMsgReceived { from, msg } => {
                    if let Some(payload) = msg.message {
                        match payload {
                            proto::control_message::Message::TopologyUpdate(update) => {
                                let mut graph = self.graph.write().await;
                                graph.apply_update(update);
                            }
                            proto::control_message::Message::SubscriptionUpdate(update) => {
                                let mut graph = self.graph.write().await;
                                let mut capabilities = HashMap::new();
                                for entry in update.entries {
                                    if entry.action == proto::SubscriptionAction::Subscribe as i32 {
                                        capabilities.insert(format!("sub:{}", entry.pattern), String::new());
                                    }
                                }
                                graph.add_node(NodeDescriptor {
                                    node_id: update.node_id.clone(),
                                    role: NodeRole::Flow as i32,
                                    capabilities,
                                    last_seen: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis() as u64,
                                });
                            }
                            proto::control_message::Message::Ping(ping) => {
                                if let Some(tx) = self.peer_writers.get(&from) {
                                    let pong = ControlMessage {
                                        message: Some(proto::control_message::Message::Pong(Pong {
                                            ping_sent_at: ping.sent_at,
                                            pong_sent_at: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_millis() as u64,
                                        })),
                                    };
                                    let _ = tx.send(pong).await;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}
