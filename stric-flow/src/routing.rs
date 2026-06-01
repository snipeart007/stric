use std::collections::{HashMap, HashSet, BinaryHeap};
use std::cmp::Ordering;
use petgraph::graph::DiGraph;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use crate::proto::{TopologyUpdate, NodeDescriptor, LinkDescriptor, NodeRole, ForwardingTargets};

/// Represents routing weights associated with a communication link between two nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkWeight {
    /// The logical cost of traversing the link (representing hops or static configurations).
    pub hop_cost: u32,
    /// The measured round-trip time of the link in microseconds.
    pub rtt_micros: u64,
}

/// Represents the global view of the mesh network topology as a directed graph.
///
/// It is used to run shortest-path calculations to build forwarding trees for pub/sub messaging.
pub struct GlobalGraph {
    /// The underlying directed graph representing the nodes and their link weights.
    pub graph: DiGraph<String, LinkWeight>,
    /// Map from node string identifiers to their respective node indices in the graph.
    pub node_indices: HashMap<String, NodeIndex>,
    /// Map storing metadata (role, capabilities, etc.) for each node.
    pub node_metadata: HashMap<String, NodeDescriptor>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct DijkstraState {
    cost: u32,
    node_idx: NodeIndex,
}

impl Ord for DijkstraState {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for DijkstraState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl GlobalGraph {
    /// Creates a new, empty `GlobalGraph`.
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_indices: HashMap::new(),
            node_metadata: HashMap::new(),
        }
    }

    /// Adds a new node to the graph or updates the metadata for an existing node.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - The descriptor of the node to add or update.
    pub fn add_node(&mut self, descriptor: NodeDescriptor) {
        let node_id = descriptor.node_id.clone();
        if !self.node_indices.contains_key(&node_id) {
            let idx = self.graph.add_node(node_id.clone());
            self.node_indices.insert(node_id.clone(), idx);
        }
        self.node_metadata.insert(node_id, descriptor);
    }

    /// Removes a node and all of its associated links from the graph.
    ///
    /// Re-maps all internal node indices to remain consistent with petgraph storage.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The identifier of the node to remove.
    pub fn remove_node(&mut self, node_id: &str) {
        if let Some(idx) = self.node_indices.remove(node_id) {
            self.graph.remove_node(idx);
            self.node_metadata.remove(node_id);
            // Re-map all indices since removing a node shifts indices in petgraph::Graph
            self.node_indices.clear();
            for node_idx in self.graph.node_indices() {
                let name = self.graph[node_idx].clone();
                self.node_indices.insert(name, node_idx);
            }
        }
    }

    /// Adds or updates a symmetric link between two nodes in the graph.
    ///
    /// This updates directed edges in both directions between `node_a` and `node_b`.
    ///
    /// # Arguments
    ///
    /// * `link` - The descriptor of the link to add or update.
    pub fn add_link(&mut self, link: LinkDescriptor) {
        let idx_a = *self.node_indices.entry(link.node_a.clone()).or_insert_with(|| {
            self.graph.add_node(link.node_a.clone())
        });
        let idx_b = *self.node_indices.entry(link.node_b.clone()).or_insert_with(|| {
            self.graph.add_node(link.node_b.clone())
        });

        let weight = LinkWeight {
            hop_cost: link.hop_cost,
            rtt_micros: link.rtt_micros,
        };

        // Add both directions for symmetric peer links
        self.graph.update_edge(idx_a, idx_b, weight.clone());
        self.graph.update_edge(idx_b, idx_a, weight);
    }

    /// Removes the link between two nodes in the graph.
    ///
    /// # Arguments
    ///
    /// * `node_a` - The identifier of the first node.
    /// * `node_b` - The identifier of the second node.
    pub fn remove_link(&mut self, node_a: &str, node_b: &str) {
        if let (Some(&idx_a), Some(&idx_b)) = (self.node_indices.get(node_a), self.node_indices.get(node_b)) {
            if let Some(edge) = self.graph.find_edge(idx_a, idx_b) {
                self.graph.remove_edge(edge);
            }
            if let Some(edge) = self.graph.find_edge(idx_b, idx_a) {
                self.graph.remove_edge(edge);
            }
        }
    }

    /// Applies a batch of updates to the graph, adding/removing nodes and links.
    ///
    /// # Arguments
    ///
    /// * `update` - The topology update received from the network.
    pub fn apply_update(&mut self, update: TopologyUpdate) {
        for node in update.nodes_added {
            self.add_node(node);
        }
        for node_id in update.nodes_removed {
            self.remove_node(&node_id);
        }
        for link in update.links_added {
            self.add_link(link);
        }
        for link_removed in update.links_removed {
            self.remove_link(&link_removed.node_a, &link_removed.node_b);
        }
    }

    /// Computes the shortest-path forwarding table from a source node to a set of subscribers.
    ///
    /// Returns a map associating each relay node along the forwarding tree with its downstream targets.
    ///
    /// # Arguments
    ///
    /// * `source` - The ID of the node initiating the publication.
    /// * `subscribers` - The set of node IDs that are subscribed to the topic.
    pub fn compute_forwarding_table(
        &self,
        source: &str,
        subscribers: &HashSet<String>,
    ) -> HashMap<String, ForwardingTargets> {
        if subscribers.is_empty() {
            return HashMap::new();
        }

        let source_idx = match self.node_indices.get(source) {
            Some(&idx) => idx,
            None => return HashMap::new(),
        };

        let mut distances = HashMap::new();
        let mut predecessors = HashMap::new();
        let mut heap = BinaryHeap::new();

        distances.insert(source_idx, 0);
        heap.push(DijkstraState { cost: 0, node_idx: source_idx });

        while let Some(DijkstraState { cost, node_idx }) = heap.pop() {
            let current_dist = *distances.get(&node_idx).unwrap_or(&u32::MAX);
            if cost > current_dist {
                continue;
            }

            for edge in self.graph.edges(node_idx) {
                let target_idx = edge.target();
                let target_name = &self.graph[target_idx];
                
                let mut edge_cost = edge.weight().hop_cost;
                
                // Aggregator routing cost penalty
                if let Some(target_meta) = self.node_metadata.get(target_name) {
                    if target_meta.role == NodeRole::Aggregator as i32 {
                        edge_cost += 10000;
                    }
                }

                let next_cost = cost + edge_cost;
                let current_target_dist = distances.get(&target_idx).copied().unwrap_or(u32::MAX);
                
                if next_cost < current_target_dist {
                    distances.insert(target_idx, next_cost);
                    predecessors.insert(target_idx, node_idx);
                    heap.push(DijkstraState { cost: next_cost, node_idx: target_idx });
                }
            }
        }

        let mut forwarding_tree: HashMap<String, HashSet<String>> = HashMap::new();

        for sub in subscribers {
            let mut current_idx = match self.node_indices.get(sub) {
                Some(&idx) => idx,
                None => continue,
            };

            while let Some(&parent_idx) = predecessors.get(&current_idx) {
                let parent_name = self.graph[parent_idx].clone();
                let current_name = self.graph[current_idx].clone();
                
                forwarding_tree
                    .entry(parent_name)
                    .or_default()
                    .insert(current_name);

                current_idx = parent_idx;
                if current_idx == source_idx {
                    break;
                }
            }
        }

        forwarding_tree
            .into_iter()
            .map(|(node, targets)| {
                (
                    node,
                    ForwardingTargets {
                        send_to: targets.into_iter().collect(),
                    },
                )
            })
            .collect()
    }

    /// Serializes the graph's nodes and edges into lists of descriptors.
    ///
    /// Used for initial state synchronization when connecting to new peers.
    pub fn get_descriptors(&self) -> (Vec<NodeDescriptor>, Vec<LinkDescriptor>) {
        let nodes = self.node_metadata.values().cloned().collect();
        let mut links = Vec::new();
        for edge in self.graph.edge_references() {
            let u = &self.graph[edge.source()];
            let v = &self.graph[edge.target()];
            links.push(LinkDescriptor {
                node_a: u.clone(),
                node_b: v.clone(),
                hop_cost: edge.weight().hop_cost,
                rtt_micros: edge.weight().rtt_micros,
            });
        }
        (nodes, links)
    }
}

/// Matches a hierarchical topic ID against a subscription topic pattern.
///
/// Supports MQTT-style wildcards:
/// - `*` matches a single hierarchy level (e.g., `sensor.*` matches `sensor.temp` but not `sensor.temp.celsius`).
/// - `#` matches all remaining levels at the end (e.g., `sensor.#` matches `sensor.temp` and `sensor.temp.celsius`).
///
/// # Arguments
///
/// * `pattern` - The subscription topic pattern containing optional wildcards.
/// * `topic` - The concrete topic ID to match.
pub fn match_topic(pattern: &str, topic: &str) -> bool {
    let p_parts: Vec<&str> = pattern.split('.').collect();
    let t_parts: Vec<&str> = topic.split('.').collect();
    
    let mut p_idx = 0;
    let mut t_idx = 0;
    
    while p_idx < p_parts.len() {
        let p_part = p_parts[p_idx];
        if p_part == "#" {
            return true;
        }
        
        if t_idx >= t_parts.len() {
            return false;
        }
        
        let t_part = t_parts[t_idx];
        if p_part == "*" {
            p_idx += 1;
            t_idx += 1;
            continue;
        }
        
        if p_part != t_part {
            return false;
        }
        
        p_idx += 1;
        t_idx += 1;
    }
    
    t_idx == t_parts.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard_topic_matching() {
        assert!(match_topic("sensor.*", "sensor.temperature"));
        assert!(match_topic("sensor.*", "sensor.humidity"));
        assert!(!match_topic("sensor.*", "sensor.temperature.celsius"));
        
        assert!(match_topic("sensor.#", "sensor.temperature"));
        assert!(match_topic("sensor.#", "sensor.temperature.celsius"));
        assert!(!match_topic("sensor.#", "actuator.motor"));
        
        assert!(match_topic("sensor.*.celsius", "sensor.temperature.celsius"));
        assert!(!match_topic("sensor.*.celsius", "sensor.temperature"));
        
        assert!(match_topic("*", "sensor"));
        assert!(!match_topic("*", "sensor.temperature"));
        assert!(match_topic("#", "sensor.temperature.celsius"));
    }

    #[test]
    fn test_dijkstra_routing_tree() {
        let mut graph = GlobalGraph::new();
        
        graph.add_node(NodeDescriptor {
            node_id: "A".into(),
            role: NodeRole::Flow as i32,
            ..Default::default()
        });
        graph.add_node(NodeDescriptor {
            node_id: "B".into(),
            role: NodeRole::Flow as i32,
            ..Default::default()
        });
        graph.add_node(NodeDescriptor {
            node_id: "C".into(),
            role: NodeRole::Flow as i32,
            ..Default::default()
        });
        graph.add_node(NodeDescriptor {
            node_id: "D".into(),
            role: NodeRole::Flow as i32,
            ..Default::default()
        });
        
        // A - B (cost 1)
        // B - C (cost 1)
        // B - D (cost 1)
        graph.add_link(LinkDescriptor { node_a: "A".into(), node_b: "B".into(), hop_cost: 1, ..Default::default() });
        graph.add_link(LinkDescriptor { node_a: "B".into(), node_b: "C".into(), hop_cost: 1, ..Default::default() });
        graph.add_link(LinkDescriptor { node_a: "B".into(), node_b: "D".into(), hop_cost: 1, ..Default::default() });

        let mut subscribers = HashSet::new();
        subscribers.insert("C".to_string());
        subscribers.insert("D".to_string());

        let table = graph.compute_forwarding_table("A", &subscribers);

        assert_eq!(table.len(), 2);
        
        let a_targets = &table.get("A").unwrap().send_to;
        assert_eq!(a_targets.len(), 1);
        assert!(a_targets.contains(&"B".to_string()));

        let b_targets = &table.get("B").unwrap().send_to;
        assert_eq!(b_targets.len(), 2);
        assert!(b_targets.contains(&"C".to_string()));
        assert!(b_targets.contains(&"D".to_string()));
    }
}
