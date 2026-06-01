use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use async_trait::async_trait;
use dashmap::DashMap;
use prost::Message;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use stric_core::{BoxFuture, ConnectionWrapper, NodeConfig, QuicNode};

use crate::error::FlowError;
use crate::frame::{read_frame, write_frame};
use crate::proto::{
    self, ControlMessage, Envelope, FlowHandshake, HandshakeAck,
    NodeDescriptor, NodeRole, Ping, Pong, SubscriptionEntry, SubscriptionUpdate,
    TopologyUpdate, LinkDescriptor,
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
    merge_fns: Arc<DashMap<String, crate::reconciliation::StateMergeFn>>,
    topic_handlers: Arc<DashMap<String, Arc<dyn FlowHandler>>>,
    metric: Arc<dyn RoutingMetric>,
    registry: Arc<MessageRegistry>,
    peer_writers: Arc<DashMap<String, mpsc::Sender<ControlMessage>>>,
    local_subscriptions: Arc<RwLock<HashSet<String>>>,
    control_tx: mpsc::Sender<ControlEvent>,
    last_epochs: Arc<DashMap<String, u64>>,
    last_subscription_epochs: Arc<DashMap<String, u64>>,
    flow_limiters: Arc<DashMap<String, crate::backpressure::TokenBucketRateLimiter>>,
    peer_addresses: Arc<DashMap<String, (SocketAddr, String)>>,
    pending_connects: Arc<DashMap<SocketAddr, String>>,
    reconnecting_peers: Arc<DashMap<String, ()>>,
    node_last_seen: Arc<DashMap<String, Instant>>,
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

fn make_connection_handler<F>(f: F) -> stric_core::ConnectionHandlerFn<FlowConnectionMetadata>
where
    F: for<'a> Fn(&'a mut ConnectionWrapper<FlowConnectionMetadata>) -> BoxFuture<'a, Result<(), anyhow::Error>> + Send + Sync + 'static,
{
    Arc::new(f)
}

impl FlowNode {
    /// Creates a new `FlowNode` instance and begins listening/handshaking logic.
    pub fn new(
        node_id: String,
        mut config: NodeConfig,
        metric: Arc<dyn RoutingMetric>,
        registry: Arc<MessageRegistry>,
    ) -> Result<(Arc<Self>, mpsc::Receiver<anyhow::Error>), FlowError> {
        // Set ALPN protocol to ensure connection compatibility
        if config.alpn_protocol_names.is_empty() {
            config.alpn_protocol_names = vec![b"stric-flow".to_vec()];
        }

        let (mut core_node, error_rx) = QuicNode::<FlowConnectionMetadata>::new(config)?;
        let (control_tx, control_rx) = mpsc::channel(100);

        let node = Arc::new_cyclic(|weak_self: &std::sync::Weak<Self>| {
            let weak_inbound = weak_self.clone();
            let inbound_handler = make_connection_handler(move |conn_wrapper| {
                let weak = weak_inbound.clone();
                Box::pin(async move {
                    if let Some(node_ref) = weak.upgrade() {
                        node_ref.handle_new_connection(conn_wrapper, false).await
                    } else {
                        Err(anyhow::anyhow!("Node dropped"))
                    }
                })
            });

            let weak_outbound = weak_self.clone();
            let outbound_handler = make_connection_handler(move |conn_wrapper| {
                let weak = weak_outbound.clone();
                Box::pin(async move {
                    if let Some(node_ref) = weak.upgrade() {
                        node_ref.handle_new_connection(conn_wrapper, true).await
                    } else {
                        Err(anyhow::anyhow!("Node dropped"))
                    }
                })
            });

            core_node.on_inbound(inbound_handler);
            core_node.on_outbound(outbound_handler);

            Self {
                node_id: node_id.clone(),
                core: Arc::new(core_node),
                graph: Arc::new(RwLock::new(GlobalGraph::new())),
                sessions: Arc::new(DashMap::new()),
                merge_fns: Arc::new(DashMap::new()),
                topic_handlers: Arc::new(DashMap::new()),
                metric,
                registry,
                peer_writers: Arc::new(DashMap::new()),
                local_subscriptions: Arc::new(RwLock::new(HashSet::new())),
                control_tx,
                last_epochs: Arc::new(DashMap::new()),
                last_subscription_epochs: Arc::new(DashMap::new()),
                flow_limiters: Arc::new(DashMap::new()),
                peer_addresses: Arc::new(DashMap::new()),
                pending_connects: Arc::new(DashMap::new()),
                reconnecting_peers: Arc::new(DashMap::new()),
                node_last_seen: Arc::new(DashMap::new()),
            }
        });

        // Run the main control task coordinating mesh topology and routing updates
        let node_clone = node.clone();
        tokio::spawn(async move {
            node_clone.run_control_loop(control_rx).await;
        });

        // Spawn session garbage collection task
        let sessions_clone = node.sessions.clone();
        let last_seen_clone = node.node_last_seen.clone();
        let control_tx_clone = node.control_tx.clone();
        let node_id_clone = node.node_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let evicted = crate::reconciliation::gc_inactive_sessions(
                    &sessions_clone,
                    &last_seen_clone,
                    Duration::from_secs(300),
                );
                for session_id in evicted {
                    let close_msg = ControlMessage {
                        message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                            message: Some(proto::session_control::Message::Close(proto::SessionClose {
                                session_id,
                                closed_by: node_id_clone.clone(),
                                reason: "Session TTL expired (creator inactive)".to_string(),
                            })),
                        })),
                    };
                    let _ = control_tx_clone.send(ControlEvent::ControlMsgReceived {
                        from: node_id_clone.clone(),
                        msg: close_msg,
                    }).await;
                }
            }
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
        self.pending_connects.insert(addr, server_name.to_string());
        match self.core.connect(addr, server_name).await {
            Ok(val) => Ok(val),
            Err(e) => {
                self.pending_connects.remove(&addr);
                Err(FlowError::from(e))
            }
        }
    }

    /// Handles a newly established physical connection, negotiating the control stream.
    async fn handle_new_connection(
        &self,
        conn: &mut ConnectionWrapper<FlowConnectionMetadata>,
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

        // Register peer socket address and server name
        let remote_addr = conn.conn.remote_address();
        let server_name = self.pending_connects.remove(&remote_addr)
            .map(|(_, name)| name)
            .unwrap_or_else(|| peer_id.clone());
        self.peer_addresses.insert(peer_id.clone(), (remote_addr, server_name));

        // Task 3.1: Register active peer connection metadata
        conn.metadata.peer_node_id = Some(peer_id.clone());

        // Register peer writer
        let (peer_control_tx, mut peer_control_rx) = mpsc::channel::<ControlMessage>(100);
        let _ = self.control_tx.send(ControlEvent::PeerConnected {
            node_id: peer_id.clone(),
            tx: peer_control_tx.clone(),
        }).await;

        // Task 2.2: Initial Topology Snapshot & Subscriptions Sync
        let (nodes_added, links_added) = {
            let graph = self.graph.read().await;
            graph.get_descriptors()
        };
        let topo_sync = ControlMessage {
            message: Some(proto::control_message::Message::TopologyUpdate(TopologyUpdate {
                origin_node_id: self.node_id.clone(),
                epoch: 1,
                nodes_added,
                nodes_removed: Vec::new(),
                links_added,
                links_removed: Vec::new(),
            })),
        };
        let _ = peer_control_tx.send(topo_sync).await;

        let my_sub_epoch = self.last_subscription_epochs.get(&self.node_id).map(|r| *r).unwrap_or(0);
        let subs = self.local_subscriptions.read().await;
        let sub_sync = ControlMessage {
            message: Some(proto::control_message::Message::SubscriptionUpdate(SubscriptionUpdate {
                node_id: self.node_id.clone(),
                entries: subs.iter().map(|pattern| SubscriptionEntry {
                    flow_id: String::new(),
                    pattern: pattern.clone(),
                    action: proto::SubscriptionAction::Subscribe as i32,
                }).collect(),
                epoch: my_sub_epoch,
            })),
        };
        let _ = peer_control_tx.send(sub_sync).await;

        // Task 2.1: Spawn control Ping/Pong loop
        let ping_tx = peer_control_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let ping = ControlMessage {
                    message: Some(proto::control_message::Message::Ping(Ping {
                        sent_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    })),
                };
                if ping_tx.send(ping).await.is_err() {
                    break;
                }
            }
        });

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
        let core_clone = self.core.clone();
        let conn_id = conn.context.id;
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
                        core_clone.connection_manager().remove_connection(conn_id); // Cleanup connection
                        let _ = control_tx_clone.send(ControlEvent::PeerDisconnected {
                            node_id: peer_id_clone.clone(),
                        }).await;
                        break;
                    }
                }
            }
        });

        // Task 3.1 & 3.2: Spawn incoming data streams listener task
        let conn_clone = conn.conn.clone();
        let registry = self.registry.clone();
        let handlers = self.topic_handlers.clone();
        let node_id_clone = self.node_id.clone();
        let core_node = self.core.clone();
        let flow_limiters = self.flow_limiters.clone();
        tokio::spawn(async move {
            while let Ok(mut recv) = conn_clone.accept_uni().await {
                let registry_ref = registry.clone();
                let handlers_ref = handlers.clone();
                let node_id_ref = node_id_clone.clone();
                let core_node_ref = core_node.clone();
                let flow_limiters_ref = flow_limiters.clone();
                
                tokio::spawn(async move {
                    if let Ok(bytes) = read_frame(&mut recv).await {
                        if let Ok(envelope) = Envelope::decode(&bytes[..]) {
                            if let Some(header) = &envelope.header {
                                let flow_id = &header.flow_id;
                                let topic_id = &header.topic_id;

                                // Task 3.2: Stateless Forwarding Lookup
                                if let Some(targets) = header.forwarding_table.get(&node_id_ref) {
                                    for next_hop in &targets.send_to {
                                        let next_conn = core_node_ref.connection_manager().store.iter()
                                            .find(|entry| entry.value().metadata.peer_node_id.as_deref() == Some(next_hop.as_str()))
                                            .map(|entry| entry.value().conn.clone());
                                        if let Some(conn) = next_conn {
                                            let conn = conn.clone();
                                            let envelope_clone = envelope.clone();
                                            let flow_limiters_clone = flow_limiters_ref.clone();
                                            let flow_id_clone = flow_id.clone();
                                            
                                            tokio::spawn(async move {
                                                // Task 3.3: Apply outbound backpressure
                                                let limiter = flow_limiters_clone.get(&flow_id_clone).map(|r| r.value().clone());
                                                if let Some(limiter) = limiter {
                                                    let size = envelope_clone.encoded_len() as u32;
                                                    limiter.wait_for_bytes(size).await;
                                                }
                                                if let Ok(mut send) = conn.open_uni().await {
                                                    let _ = write_frame(&mut send, &envelope_clone).await;
                                                }
                                            });
                                        }
                                    }
                                }

                                // Local Delivery if matching subscription
                                if let Ok(decoded) = registry_ref.decode(&envelope.message_type, &envelope.payload) {
                                    let mut matched_handler = None;
                                    for entry in handlers_ref.iter() {
                                        if match_topic(entry.key(), topic_id) {
                                            matched_handler = Some(entry.value().clone());
                                            break;
                                        }
                                    }
                                    if let Some(handler) = matched_handler {
                                        let _ = handler.handle_message(flow_id, topic_id, decoded).await;
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
        let last_sub_epochs = self.last_subscription_epochs.clone();
        tokio::spawn(async move {
            let mut subs = local_subs.write().await;
            subs.insert(topic_pattern);
            
            let my_epoch = {
                let current = last_sub_epochs.get(&node_id).map(|r| *r).unwrap_or(0);
                current + 1
            };
            
            // Broadcast SubscriptionUpdate to all peers
            let update = ControlMessage {
                message: Some(proto::control_message::Message::SubscriptionUpdate(SubscriptionUpdate {
                    node_id: node_id.clone(),
                    entries: subs.iter().map(|pattern| SubscriptionEntry {
                        flow_id: String::new(),
                        pattern: pattern.clone(),
                        action: proto::SubscriptionAction::Subscribe as i32,
                    }).collect(),
                    epoch: my_epoch,
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
            flow_id: flow_id.clone(),
            topic_id: topic_id.clone(),
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
                    let next_conn = self.core.connection_manager().store.iter()
                        .find(|entry| entry.value().metadata.peer_node_id.as_deref() == Some(next_hop.as_str()))
                        .map(|entry| entry.value().conn.clone());
                    if let Some(conn) = next_conn {
                        let conn = conn.clone();
                        let envelope_clone = envelope.clone();
                        let flow_limiters = self.flow_limiters.clone();
                        let flow_id_clone = flow_id.clone();
                        
                        tokio::spawn(async move {
                            let limiter = flow_limiters.get(&flow_id_clone).map(|r| r.value().clone());
                            if let Some(limiter) = limiter {
                                let size = envelope_clone.encoded_len() as u32;
                                limiter.wait_for_bytes(size).await;
                            }
                            match conn.open_uni().await {
                                Ok(mut send) => {
                                    if let Err(e) = write_frame(&mut send, &envelope_clone).await {
                                        error!("Failed to write data frame: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to open unidirectional stream for publishing: {}", e);
                                }
                            }
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn get_peer_writers(&self) -> Vec<(String, mpsc::Sender<ControlMessage>)> {
        self.peer_writers.iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    async fn run_control_loop(self: Arc<Self>, mut rx: mpsc::Receiver<ControlEvent>) {
        while let Some(event) = rx.recv().await {
            match event {
                ControlEvent::PeerConnected { node_id, tx } => {
                    self.peer_writers.insert(node_id.clone(), tx);
                    info!("Peer registered in control engine: {}", node_id);
                }
                ControlEvent::PeerDisconnected { node_id } => {
                    self.peer_writers.remove(&node_id);
                    info!("Peer deregistered from control engine: {}", node_id);

                    if let Some(addr_info) = self.peer_addresses.get(&node_id) {
                        let (addr, server_name) = addr_info.value().clone();
                        let self_clone = self.clone();
                        let node_id_clone = node_id.clone();
                        
                        if self.reconnecting_peers.insert(node_id.clone(), ()).is_none() {
                            tokio::spawn(async move {
                                let mut backoff = crate::reconciliation::ExponentialBackoff::new(
                                    Duration::from_secs(1),
                                    Duration::from_secs(60),
                                );
                                
                                loop {
                                    let is_connected = self_clone.core.connection_manager().store.iter()
                                        .any(|entry| entry.value().metadata.peer_node_id.as_deref() == Some(node_id_clone.as_str()));
                                    if is_connected {
                                        self_clone.reconnecting_peers.remove(&node_id_clone);
                                        break;
                                    }
                                    
                                    let delay = backoff.next_backoff();
                                    info!("Attempting reconnection to {} in {:?}", node_id_clone, delay);
                                    tokio::time::sleep(delay).await;
                                    
                                    let is_connected_after = self_clone.core.connection_manager().store.iter()
                                        .any(|entry| entry.value().metadata.peer_node_id.as_deref() == Some(node_id_clone.as_str()));
                                    if is_connected_after {
                                        self_clone.reconnecting_peers.remove(&node_id_clone);
                                        break;
                                    }
                                    
                                    match self_clone.connect(addr, &server_name).await {
                                        Ok(_) => {
                                            info!("Successfully reconnected to peer {}", node_id_clone);
                                            self_clone.reconnecting_peers.remove(&node_id_clone);
                                            break;
                                        }
                                        Err(e) => {
                                            warn!("Reconnection attempt to {} failed: {}", node_id_clone, e);
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
                ControlEvent::ControlMsgReceived { from, msg } => {
                    self.node_last_seen.insert(from.clone(), Instant::now());
                    if let Some(payload) = msg.message {
                        match payload {
                            proto::control_message::Message::TopologyUpdate(update) => {
                                self.node_last_seen.insert(update.origin_node_id.clone(), Instant::now());
                                for node in &update.nodes_added {
                                    self.node_last_seen.insert(node.node_id.clone(), Instant::now());
                                }
                                let has_epoch = self.last_epochs.contains_key(&update.origin_node_id);
                                let current_epoch = self.last_epochs.get(&update.origin_node_id).map(|r| *r).unwrap_or(0);
                                if !has_epoch || update.epoch > current_epoch {
                                    self.last_epochs.insert(update.origin_node_id.clone(), update.epoch);
                                    {
                                        let mut graph = self.graph.write().await;
                                        graph.apply_update(update.clone());
                                    }
                                    let gossip = ControlMessage {
                                        message: Some(proto::control_message::Message::TopologyUpdate(update.clone())),
                                    };
                                    let peers = self.get_peer_writers();
                                    for (peer, tx) in peers {
                                        if peer != from && peer != update.origin_node_id {
                                            let _ = tx.send(gossip.clone()).await;
                                        }
                                    }
                                }
                            }
                            proto::control_message::Message::SubscriptionUpdate(update) => {
                                self.node_last_seen.insert(update.node_id.clone(), Instant::now());
                                let has_epoch = self.last_subscription_epochs.contains_key(&update.node_id);
                                let current_epoch = self.last_subscription_epochs.get(&update.node_id).map(|r| *r).unwrap_or(0);
                                if !has_epoch || update.epoch > current_epoch {
                                    self.last_subscription_epochs.insert(update.node_id.clone(), update.epoch);
                                    {
                                        let mut graph = self.graph.write().await;
                                        let mut capabilities = HashMap::new();
                                        for entry in &update.entries {
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
                                    let gossip = ControlMessage {
                                        message: Some(proto::control_message::Message::SubscriptionUpdate(update.clone())),
                                    };
                                    let peers = self.get_peer_writers();
                                    for (peer, tx) in peers {
                                        if peer != from && peer != update.node_id {
                                            let _ = tx.send(gossip.clone()).await;
                                        }
                                    }
                                }
                            }
                            proto::control_message::Message::Ping(ping) => {
                                let tx_opt = self.peer_writers.get(&from).map(|entry| entry.value().clone());
                                if let Some(tx) = tx_opt {
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
                            proto::control_message::Message::Pong(pong) => {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis() as u64;
                                let rtt = now.saturating_sub(pong.ping_sent_at);
                                
                                let mut is_new_link = false;
                                {
                                    let mut graph = self.graph.write().await;
                                    let idx_a = graph.node_indices.get(&self.node_id).copied();
                                    let idx_b = graph.node_indices.get(&from).copied();
                                    let link_exists = match (idx_a, idx_b) {
                                        (Some(a), Some(b)) => graph.graph.find_edge(a, b).is_some(),
                                        _ => false,
                                    };
                                    if !link_exists {
                                        is_new_link = true;
                                    }
                                    graph.add_link(LinkDescriptor {
                                        node_a: self.node_id.clone(),
                                        node_b: from.clone(),
                                        hop_cost: 1,
                                        rtt_micros: rtt * 1000,
                                    });
                                }

                                if is_new_link {
                                    let my_epoch = {
                                        let mut entry = self.last_epochs.entry(self.node_id.clone()).or_insert(0);
                                        *entry += 1;
                                        *entry
                                    };
                                    let (nodes_added, links_added) = {
                                        let graph = self.graph.read().await;
                                        graph.get_descriptors()
                                    };
                                    let topo_update = ControlMessage {
                                        message: Some(proto::control_message::Message::TopologyUpdate(TopologyUpdate {
                                            origin_node_id: self.node_id.clone(),
                                            epoch: my_epoch,
                                            nodes_added,
                                            nodes_removed: Vec::new(),
                                            links_added,
                                            links_removed: Vec::new(),
                                        })),
                                    };
                                    let peers = self.get_peer_writers();
                                    for (_peer, tx) in peers {
                                        let _ = tx.send(topo_update.clone()).await;
                                    }
                                }
                            }
                            proto::control_message::Message::Backpressure(signal) => {
                                let limiter_opt = self.flow_limiters.entry(signal.flow_id.clone()).or_insert_with(|| {
                                    crate::backpressure::TokenBucketRateLimiter::new(0)
                                }).clone();
                                match signal.action {
                                    0 => { // PAUSE
                                        limiter_opt.pause().await;
                                        info!("Backpressure PAUSED flow: {}", signal.flow_id);
                                    }
                                    1 => { // RESUME
                                        limiter_opt.resume().await;
                                        info!("Backpressure RESUMED flow: {}", signal.flow_id);
                                    }
                                    2 => { // THROTTLE
                                        limiter_opt.resume().await;
                                        limiter_opt.set_rate(signal.max_rate).await;
                                        info!("Backpressure THROTTLED flow: {} to {} B/s", signal.flow_id, signal.max_rate);
                                    }
                                    _ => {}
                                }
                            }
                            proto::control_message::Message::SessionControl(session_ctrl) => {
                                if let Some(sub_msg) = session_ctrl.message {
                                    match sub_msg {
                                        proto::session_control::Message::Create(create) => {
                                            let mut added = false;
                                            if !self.sessions.contains_key(&create.session_id) {
                                                let session = Session {
                                                    session_id: create.session_id.clone(),
                                                    creator_node: create.creator_node.clone(),
                                                    flow_ids: create.flow_ids.clone(),
                                                    created_at: create.created_at,
                                                    metadata: create.metadata.clone(),
                                                    state_data: Vec::new(),
                                                    state_version: 0,
                                                    state_timestamp: 0,
                                                };
                                                self.sessions.insert(create.session_id.clone(), session);
                                                added = true;
                                            }
                                            if added {
                                                let gossip = ControlMessage {
                                                    message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                                                        message: Some(proto::session_control::Message::Create(create.clone())),
                                                    })),
                                                };
                                                let peers = self.get_peer_writers();
                                                for (peer, tx) in peers {
                                                    if peer != from && peer != create.creator_node {
                                                        let _ = tx.send(gossip.clone()).await;
                                                    }
                                                }
                                            }
                                        }
                                        proto::session_control::Message::Join(join) => {
                                            let gossip = ControlMessage {
                                                message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                                                    message: Some(proto::session_control::Message::Join(join.clone())),
                                                })),
                                            };
                                            let peers = self.get_peer_writers();
                                            for (peer, tx) in peers {
                                                if peer != from && peer != join.node_id {
                                                    let _ = tx.send(gossip.clone()).await;
                                                }
                                            }
                                        }
                                        proto::session_control::Message::Leave(leave) => {
                                            let gossip = ControlMessage {
                                                message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                                                    message: Some(proto::session_control::Message::Leave(leave.clone())),
                                                })),
                                            };
                                            let peers = self.get_peer_writers();
                                            for (peer, tx) in peers {
                                                if peer != from && peer != leave.node_id {
                                                    let _ = tx.send(gossip.clone()).await;
                                                }
                                            }
                                        }
                                        proto::session_control::Message::Close(close) => {
                                            let removed = self.sessions.remove(&close.session_id).is_some();
                                            if removed || close.closed_by == self.node_id {
                                                let gossip = ControlMessage {
                                                    message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                                                        message: Some(proto::session_control::Message::Close(close.clone())),
                                                    })),
                                                };
                                                let peers = self.get_peer_writers();
                                                for (peer, tx) in peers {
                                                    if peer != from && peer != close.closed_by {
                                                        let _ = tx.send(gossip.clone()).await;
                                                    }
                                                }
                                            }
                                        }
                                        proto::session_control::Message::StateSync(state_sync) => {
                                            let mut should_update = false;
                                            let mut updated_state = Vec::new();
                                            let mut updated_version = 0;
                                            let mut updated_timestamp = 0;

                                            if let Some(mut session) = self.sessions.get_mut(&state_sync.session_id) {
                                                should_update = if let Some(merge_fn) = self.merge_fns.get(&state_sync.session_id) {
                                                    match merge_fn(&session.state_data, &state_sync.data) {
                                                        Ok(merged_data) => {
                                                            session.state_data = merged_data.clone();
                                                            session.state_version = std::cmp::max(session.state_version, state_sync.version) + 1;
                                                            session.state_timestamp = std::cmp::max(session.state_timestamp, state_sync.timestamp);
                                                            updated_state = merged_data;
                                                            updated_version = session.state_version;
                                                            updated_timestamp = session.state_timestamp;
                                                            true
                                                        }
                                                        Err(e) => {
                                                            error!("Error merging state for session {}: {}", state_sync.session_id, e);
                                                            false
                                                        }
                                                    }
                                                } else {
                                                    if state_sync.timestamp > session.state_timestamp 
                                                        || (state_sync.timestamp == session.state_timestamp && state_sync.version > session.state_version) 
                                                    {
                                                        session.state_data = state_sync.data.clone();
                                                        session.state_version = state_sync.version;
                                                        session.state_timestamp = state_sync.timestamp;
                                                        updated_state = state_sync.data.clone();
                                                        updated_version = state_sync.version;
                                                        updated_timestamp = state_sync.timestamp;
                                                        true
                                                    } else {
                                                        false
                                                    }
                                                };
                                            }

                                            if should_update {
                                                let gossip = ControlMessage {
                                                    message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                                                        message: Some(proto::session_control::Message::StateSync(proto::SessionStateSync {
                                                            session_id: state_sync.session_id.clone(),
                                                            sender: self.node_id.clone(),
                                                            mode: state_sync.mode,
                                                            timestamp: updated_timestamp,
                                                            version: updated_version,
                                                            data: updated_state,
                                                            codec: state_sync.codec,
                                                        })),
                                                    })),
                                                };
                                                let peers = self.get_peer_writers();
                                                for (peer, tx) in peers {
                                                    if peer != from && peer != state_sync.sender {
                                                        let _ = tx.send(gossip.clone()).await;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    pub fn register_merge_fn(&self, session_id: String, merge_fn: crate::reconciliation::StateMergeFn) {
        self.merge_fns.insert(session_id, merge_fn);
    }

    pub async fn create_session(
        &self,
        session_id: String,
        flow_ids: Vec<String>,
        metadata: HashMap<String, String>,
    ) -> Result<(), FlowError> {
        let session = Session {
            session_id: session_id.clone(),
            creator_node: self.node_id.clone(),
            flow_ids: flow_ids.clone(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            metadata: metadata.clone(),
            state_data: Vec::new(),
            state_version: 0,
            state_timestamp: 0,
        };
        self.sessions.insert(session_id.clone(), session);

        let msg = ControlMessage {
            message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                message: Some(proto::session_control::Message::Create(proto::SessionCreate {
                    session_id,
                    creator_node: self.node_id.clone(),
                    flow_ids,
                    created_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                    metadata,
                })),
            })),
        };
        self.broadcast_control_message(msg).await;
        Ok(())
    }

    pub async fn join_session(&self, session_id: String) -> Result<(), FlowError> {
        let msg = ControlMessage {
            message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                message: Some(proto::session_control::Message::Join(proto::SessionJoin {
                    session_id,
                    node_id: self.node_id.clone(),
                })),
            })),
        };
        self.broadcast_control_message(msg).await;
        Ok(())
    }

    pub async fn leave_session(&self, session_id: String) -> Result<(), FlowError> {
        let msg = ControlMessage {
            message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                message: Some(proto::session_control::Message::Leave(proto::SessionLeave {
                    session_id,
                    node_id: self.node_id.clone(),
                })),
            })),
        };
        self.broadcast_control_message(msg).await;
        Ok(())
    }

    pub async fn close_session(&self, session_id: String, reason: String) -> Result<(), FlowError> {
        if self.sessions.remove(&session_id).is_some() {
            let msg = ControlMessage {
                message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                    message: Some(proto::session_control::Message::Close(proto::SessionClose {
                        session_id,
                        closed_by: self.node_id.clone(),
                        reason,
                    })),
                })),
            };
            self.broadcast_control_message(msg).await;
            Ok(())
        } else {
            Err(FlowError::Session(format!("Session {} not found", session_id)))
        }
    }

    pub async fn sync_session_state(&self, session_id: String, data: Vec<u8>) -> Result<(), FlowError> {
        if let Some(mut session) = self.sessions.get_mut(&session_id) {
            session.state_version += 1;
            session.state_timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            if let Some(merge_fn) = self.merge_fns.get(&session_id) {
                match merge_fn(&session.state_data, &data) {
                    Ok(merged) => {
                        session.state_data = merged;
                    }
                    Err(e) => {
                        return Err(FlowError::Session(format!("Local merge error: {}", e)));
                    }
                }
            } else {
                session.state_data = data.clone();
            }

            let msg = ControlMessage {
                message: Some(proto::control_message::Message::SessionControl(proto::SessionControl {
                    message: Some(proto::session_control::Message::StateSync(proto::SessionStateSync {
                        session_id,
                        sender: self.node_id.clone(),
                        mode: proto::SyncMode::Snapshot as i32,
                        timestamp: session.state_timestamp,
                        version: session.state_version,
                        data,
                        codec: proto::Codec::Raw as i32,
                    })),
                })),
            };
            drop(session); // Drop RefMut before broadcast_control_message!
            self.broadcast_control_message(msg).await;
            Ok(())
        } else {
            Err(FlowError::Session(format!("Session {} not found", session_id)))
        }
    }

    pub fn get_session_state(&self, session_id: &str) -> Option<Vec<u8>> {
        self.sessions.get(session_id).map(|s| s.state_data.clone())
    }

    pub fn peer_connections(&self) -> Vec<String> {
        self.core.connection_manager().store.iter()
            .filter_map(|entry| entry.value().metadata.peer_node_id.clone())
            .collect()
    }

    pub async fn print_debug_info(&self) {
        use petgraph::visit::EdgeRef;
        let graph = self.graph.read().await;
        debug!("FlowNode Debug Info for: {}", self.node_id);
        debug!("- Peer connections: {:?}", self.peer_connections());
        debug!("- Node metadata keys (nodes in graph): {:?}", graph.node_metadata.keys().collect::<Vec<_>>());
        debug!("- Edges in graph: {:?}", graph.graph.edge_references().map(|e| (graph.graph[e.source()].clone(), graph.graph[e.target()].clone())).collect::<Vec<_>>());
        
        let mut subs = Vec::new();
        for (node_id, desc) in &graph.node_metadata {
            for pattern in &desc.capabilities {
                if pattern.0.starts_with("sub:") {
                    subs.push((node_id.clone(), pattern.0.clone()));
                }
            }
        }
        debug!("- Subscription capabilities in graph: {:?}", subs);
    }

    pub fn sessions(&self) -> Vec<String> {
        self.sessions.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.core.local_addr()
    }

    pub async fn send_backpressure(
        &self,
        flow_id: String,
        action: proto::BackpressureAction,
        max_rate: u32,
    ) -> Result<(), FlowError> {
        let msg = ControlMessage {
            message: Some(proto::control_message::Message::Backpressure(proto::BackpressureSignal {
                flow_id,
                topic_id: String::new(),
                action: action as i32,
                max_rate,
            })),
        };
        self.broadcast_control_message(msg).await;
        Ok(())
    }

    async fn broadcast_control_message(&self, msg: ControlMessage) {
        let senders = self.get_peer_writers();
        for (_, sender) in senders {
            let _ = sender.send(msg.clone()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::MessageRegistry;
    use std::time::Duration;

    fn make_node_config(port: u16, cert_der: &[u8], key_der: &[u8]) -> NodeConfig {
        let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.to_vec())];
        let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der.to_vec()).unwrap();
        
        let mut roots = quinn::rustls::RootCertStore::empty();
        roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der.to_vec())).unwrap();

        NodeConfig {
            certs: Some(certs),
            key: Some(key),
            socket_addr: SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port),
            alpn_protocol_names: vec![b"stric-flow".to_vec()],
            error_channel_len: 100,
            default_conn_context: stric_core::ConnectionContext {
                id: 0,
                keep_alive: true,
                initiator_uni: true,
                initiator_bi: true,
                responder_uni: true,
                responder_bi: true,
            },
            keep_alive_limit_per_thread: 0,
            idle_timeout: None,
            root_cert_store: Some(roots),
            danger_accept_invalid_certs: true,
        }
    }

    fn init_logging() {
        use tracing_subscriber::{fmt, prelude::*, EnvFilter};
        let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
        let filter_str = if rust_log.to_lowercase().contains("debug") || rust_log.to_lowercase().contains("trace") {
            rust_log
        } else {
            format!("stric_flow=info,stric_core=warn,{}", rust_log.replace("info", "off"))
        };
        let _ = tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::new(filter_str))
            .try_init();
    }

    #[tokio::test]
    async fn test_flow_node_sessions_and_reconciliation() {
        init_logging();
        info!("test_flow_node_sessions_and_reconciliation START");
        let _ = quinn::rustls::crypto::ring::default_provider().install_default();

        // 1. Generate certificates
        info!("Generating certs...");
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().to_vec();
        let key_der = cert.signing_key.serialize_der();

        // 2. Start node_a
        info!("Starting node_a...");
        let config_a = make_node_config(0, &cert_der, &key_der);
        let registry = Arc::new(MessageRegistry::new());
        let (node_a, mut error_rx_a) = FlowNode::new(
            "node_a".to_string(),
            config_a,
            Arc::new(HopCountMetric),
            registry.clone(),
        ).unwrap();
        node_a.start().await;
        let addr_a = node_a.core.local_addr().unwrap();
        info!("node_a listening on {}", addr_a);

        // 3. Start node_b
        info!("Starting node_b...");
        let config_b = make_node_config(0, &cert_der, &key_der);
        let (node_b, mut error_rx_b) = FlowNode::new(
            "node_b".to_string(),
            config_b,
            Arc::new(HopCountMetric),
            registry.clone(),
        ).unwrap();
        node_b.start().await;

        // Give listener task time to bind
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Spawn error handler tasks so error channels don't block
        tokio::spawn(async move {
            while let Some(e) = error_rx_a.recv().await {
                error!("node_a error: {:?}", e);
            }
        });
        tokio::spawn(async move {
            while let Some(e) = error_rx_b.recv().await {
                error!("node_b error: {:?}", e);
            }
        });

        // 4. Connect node_b to node_a
        info!("Connecting node_b to node_a...");
        node_b.connect(addr_a, "localhost").await.unwrap();
        info!("node_b connected call finished!");

        // Wait for control stream handshakes and topology gossip to complete
        info!("Sleeping for 2000ms...");
        tokio::time::sleep(Duration::from_millis(2000)).await;
        info!("Wake up from sleep!");

        // Verify connection exists
        info!("Verifying connections...");
        assert!(node_a.peer_connections().contains(&"node_b".to_string()));
        assert!(node_b.peer_connections().contains(&"node_a".to_string()));

        // 5. Test create session
        info!("Creating session sess_123...");
        let flow_ids = vec!["flow1".to_string()];
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "val".to_string());
        
        node_a.create_session("sess_123".to_string(), flow_ids, metadata).await.unwrap();
        
        // Wait for create session to propagate
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Check if session exists on node_b
        assert!(node_b.sessions.contains_key("sess_123"));
        {
            let sess_b = node_b.sessions.get("sess_123").unwrap();
            assert_eq!(sess_b.creator_node, "node_a");
        }

        // 6. Test state sync (LWW)
        info!("Syncing state version 1...");
        node_a.sync_session_state("sess_123".to_string(), b"hello_version_1".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Verify version 1 state propagated
        let state_b = node_b.get_session_state("sess_123").unwrap();
        assert_eq!(state_b, b"hello_version_1".to_vec());

        // 7. Test custom state merge function
        info!("Testing merge function...");
        let merge_fn = Arc::new(|old_state: &[u8], new_state: &[u8]| {
            let mut merged = old_state.to_vec();
            merged.extend_from_slice(new_state);
            Ok(merged)
        });
        node_a.register_merge_fn("sess_123".to_string(), merge_fn.clone());
        node_b.register_merge_fn("sess_123".to_string(), merge_fn.clone());

        // Perform sync from node_a
        node_a.sync_session_state("sess_123".to_string(), b"_appended".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Check if state is merged correctly (should be hello_version_1_appended)
        let state_b = node_b.get_session_state("sess_123").unwrap();
        assert_eq!(state_b, b"hello_version_1_appended".to_vec());

        // 8. Test session close/eviction propagation
        info!("Closing session...");
        node_a.close_session("sess_123".to_string(), "done".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Session should be removed on both
        assert!(!node_a.sessions.contains_key("sess_123"));
        assert!(!node_b.sessions.contains_key("sess_123"));
        info!("test_flow_node_sessions_and_reconciliation FINISHED");
    }
}

