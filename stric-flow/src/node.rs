use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info};

use stric_core::{NodeConfig, QuicNode};

use crate::error::FlowError;
use crate::proto::{
    self, ControlMessage, Envelope, NodeDescriptor, NodeRole, Pong,
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
    /// Creates a new `FlowNode` instance.
    pub fn new(
        node_id: String,
        config: NodeConfig,
        metric: Arc<dyn RoutingMetric>,
        registry: Arc<MessageRegistry>,
    ) -> Result<(Arc<Self>, mpsc::Receiver<anyhow::Error>), FlowError> {
        let (core_node, error_rx) = QuicNode::<FlowConnectionMetadata>::new(config)?;
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

        // Run the main control task coordinating mesh topology and routing updates
        let node_clone = node.clone();
        tokio::spawn(async move {
            node_clone.run_control_loop(control_rx).await;
        });

        Ok((node, error_rx))
    }

    /// Registers a handler for a topic.
    pub fn subscribe(&self, topic_pattern: String, handler: Arc<dyn FlowHandler>) {
        info!("Registering handler for subscription pattern: {}", topic_pattern);
        self.topic_handlers.insert(topic_pattern.clone(), handler);
        
        let local_subs = self.local_subscriptions.clone();
        tokio::spawn(async move {
            let mut subs = local_subs.write().await;
            subs.insert(topic_pattern);
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

        // Find all subscribers in the network whose subscribed patterns match the topic_id
        let mut subscribers = HashSet::new();
        for (node_id, desc) in &graph.node_metadata {
            for pattern in &desc.capabilities {
                // If it starts with a subscription marker, check match
                if pattern.0.starts_with("sub:") && match_topic(&pattern.0[4..], &topic_id) {
                    subscribers.insert(node_id.clone());
                }
            }
        }

        // Add local subscriptions if they match
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
                for entry in self.topic_handlers.iter() {
                    if match_topic(entry.key(), &topic_id) {
                        let _ = entry.value().handle_message(&flow_id, &topic_id, decoded).await;
                        break;
                    }
                }
            }
        }

        if subscribers.is_empty() {
            return Ok(());
        }

        // Precompute the routing forwarding tree
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

        // Forward to the immediate next hops
        if let Some(header) = &envelope.header {
            if let Some(targets) = header.forwarding_table.get(&self.node_id) {
                for next_hop in &targets.send_to {
                    // Send over data streams to next_hop
                    // (Typically, FlowNode maintains data writer channels per peer node)
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
                                // Dynamic subscription registration of the peer
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
                                // Respond with Pong
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
