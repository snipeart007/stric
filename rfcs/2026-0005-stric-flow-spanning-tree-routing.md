# RFC 2026-0005: stric-flow Mesh Routing and Dijkstra Spanning Tree forwarding

## 1. Objective
This RFC specifies the mesh-routing architecture of `stric-flow`. It defines how data envelopes are routed across multi-hop node networks to multiple topic subscribers. The system pre-computes delivery trees at the source node to keep intermediate transit nodes completely stateless and perform $O(1)$ forwarding lookups without graph computation.

---

## 2. Mesh Routing Architecture

### 2.1. Spanning Tree Generation
When a message is published:
1. The originating source node queries its local copy of the **Global Interconnection Graph** (updated via background control flow gossip).
2. It identifies all active nodes subscribed to the destination topic.
3. It executes a modified Dijkstra Spanning Tree algorithm (optimizing for hop counts and applying penalties, e.g. avoiding aggregator-tagged storage nodes for transit).
4. The output is a pre-computed **Forwarding Table** embedded in the packet's `RoutingHeader`.

### 2.2. Pre-Computed Map-Based Forwarding
The forwarding table is represented as a Map:
```protobuf
map<string, ForwardingTargets> forwarding_table = 9;
```
* **Key:** Node ID of a forwarder node in the delivery tree path.
* **Value:** A list of direct neighbors (`send_to`) to transmit the envelope to.

### 2.3. Stateless Transit Mandate
Transit nodes do not perform graph lookups, pathfinding calculations, or payload re-serializations. 
1. When transit Node B receives an envelope, it performs a simple $O(1)$ key lookup in the `forwarding_table` using its own node ID.
2. If found, Node B clones the envelope bytes and transmits them **unmodified** to each listed neighbor in the `send_to` list.
3. If not found, Node B terminates the forwarding loop.
4. If Node B itself is a subscriber, it forwards a copy of the payload to its local application handler.

---

## 3. Example Scenario

Consider node topology:
```
        A ─── B ─── C
              │
              └─ D ─── E
```
Subscribers: `C`, `E`, and `B`.

1. **Source Node A** runs Dijkstra and determines:
   * A sends to B.
   * B sends to C and D.
   * D sends to E.
2. **A** embeds this in `forwarding_table`:
   ```json
   {
     "A": { "send_to": ["B"] },
     "B": { "send_to": ["C", "D"] },
     "D": { "send_to": ["E"] }
   }
   ```
3. **B** performs O(1) lookup on key `"B"`, gets `["C", "D"]`, delivers locally to its own topic handlers, and forwards the byte-envelope unmodified to `C` and `D`.
4. **D** performs O(1) lookup on key `"D"`, gets `["E"]`, and forwards to `E`.

---

## 4. Transmission Efficiency and Wildcards

### 4.1. Topic Wildcard Patterns
Topic subscription patterns follow MQTT-style conventions:
* `sensor.*` matches `sensor.temperature` and `sensor.humidity` (single-level wildcard).
* `sensor/#` matches `sensor.temperature.celsius` (multi-level wildcard).

### 4.2. Physical Link Deduplication
To optimize bandwidth usage across physical links:
* If the routing engine dictates that Node A must send Message M to Node B for both Flow X and Flow Y (due to multiple overlapping subscriptions), **Node A transmits the payload over the QUIC connection exactly once**.
* The metadata inside `RoutingHeader` specifies the logical flow scopes, allowing Node B to route the single payload to the multiple local handlers internally.
