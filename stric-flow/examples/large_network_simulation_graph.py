import sys

num_nodes = 40
connections = {i: set() for i in range(num_nodes)}

# 1. Ring connections
for i in range(num_nodes - 1):
    connections[i].add(i + 1)
    connections[i + 1].add(i)

# 2. Chord connections
for i in range(num_nodes):
    if i % 3 == 0:
        target = (i + 5) % num_nodes
        if target > i:
            connections[i].add(target)
            connections[target].add(i)
    if i % 7 == 0:
        target = (i + 13) % num_nodes
        if target > i:
            connections[i].add(target)
            connections[target].add(i)

# Generate Graphviz DOT representation (recommended for large circles/force-directed 2D layouts)
dot_lines = [
    "graph G {",
    "    layout=circo;  // Arranges nodes in a circle to show the chords clearly",
    "    node [shape=circle, style=filled, fillcolor=lightblue, fontname=\"Helvetica\", fontsize=10, width=0.6];",
    "    edge [color=gray, penwidth=1.2];"
]

# Track added edges to avoid duplicate lines in undirected graph
added_edges = set()
for i in range(num_nodes):
    for peer in sorted(list(connections[i])):
        edge = tuple(sorted([i, peer]))
        if edge not in added_edges:
            dot_lines.append(f"    \"Node {i}\" -- \"Node {peer}\";")
            added_edges.add(edge)
dot_lines.append("}")

with open("large_network_simulation_mesh.dot", "w") as f:
    f.write("\n".join(dot_lines))
print("Generated large_network_simulation_mesh.dot successfully.")

# Generate Mermaid.js representation
mermaid_lines = [
    "graph TD",
    "    %% Use a circular or force-directed renderer"
]
added_edges = set()
for i in range(num_nodes):
    for peer in sorted(list(connections[i])):
        edge = tuple(sorted([i, peer]))
        if edge not in added_edges:
            mermaid_lines.append(f"    Node{i}((Node {i})) --- Node{peer}((Node {peer}))")
            added_edges.add(edge)

with open("large_network_simulation_mesh.mmd", "w") as f:
    f.write("\n".join(mermaid_lines))
print("Generated large_network_simulation_mesh.mmd successfully.")
