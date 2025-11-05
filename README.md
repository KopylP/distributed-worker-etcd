# Distributed Worker Cluster (Example)

> ⚠️ **Disclaimer:**  
> This repository is an **example implementation** of a distributed coordination system for workers built with **.NET** and **etcd**.  
> It is designed for **educational and illustrative purposes only** — **not production-ready**.  
> The goal is to demonstrate how workers can **scale horizontally** and **process distinct work segments without conflicts** by achieving **distributed consensus** via etcd.

---

## System Goal

This system models a **distributed cluster of worker nodes** that cooperate without central orchestration.  
The main objective is to ensure that:

- Each worker processes **only its assigned subset of data (segments)**  
- No two nodes ever process the **same segment simultaneously**  
- When nodes **join, leave, or fail**, the system **automatically rebalances assignments**  
- **Leadership** is determined through consensus, so only one node at a time coordinates assignments  
- The system can **scale horizontally** by simply adding more nodes  

In other words — it’s an **example of distributed coordination** and **conflict-free work partitioning** built around **etcd’s consensus model**.

---

## System Requirements

| Requirement | Description |
|--------------|-------------|
| **Distributed coordination** | Workers must operate independently but stay consistent through a shared state. |
| **No conflicting work** | Two workers cannot process the same segment. |
| **Automatic leader election** | One node at a time acts as the coordinator (leader). |
| **Dynamic rebalancing** | When nodes join or leave, work segments are redistributed. |
| **Resilience** | If the leader fails, the system elects a new one. |
| **Scalability** | New workers can join seamlessly and take on part of the workload. |

---

## How This Project Solves It (Using etcd)

| Mechanism | Component | Description |
|------------|------------|-------------|
| **Consensus & Coordination** | [etcd](https://etcd.io/) | Serves as the distributed key–value store and consensus system. |
| **Node Liveness Tracking** | `LeaseManagerService` | Each node acquires and renews an etcd **lease** — its presence in the cluster depends on it. |
| **Leader Election** | `LeaderElectionService` | Uses etcd’s **election API** — one node becomes the leader, others are followers. |
| **Cluster State Management** | `ClusterState` + `WatchService` | Watches etcd for node joins/leaves and updates local state reactively. |
| **Work Distribution** | `AssignmentManagerService` + `SegmentDistributor` | The leader evenly divides all work **segments** among active nodes and stores the mapping in etcd. |
| **Reactive Updates** | `WatchService` | All nodes watch assignment keys and instantly update their local view when changes occur. |
| **Work Execution** | `WorkerService` | Each worker processes **only its assigned segments** on a schedule. |

---

## How It Works (Step by Step)

1. **Startup**  
   Each node starts, connects to etcd, and requests a **lease** to represent its liveness.

2. **Leader Election**  
   All nodes participate in etcd’s election process.  
   One node becomes the **leader**, others remain **followers**.

3. **Segment Distribution**  
   The leader calls `SegmentDistributor.DistributeEvenly()` to divide all segments (e.g. 0–9) among active nodes.  
   Assignments are serialized to etcd (`cluster/segments` key).

4. **Reactive Updates**  
   Each node watches etcd for changes.  
   When a node joins or leaves, or the leader changes, **new assignments** are generated.

5. **Work Execution**  
   Each node’s `WorkerService` runs a background loop, processing only its assigned segments.  
   If segments change or the node loses its lease, it automatically pauses until reassigned.

6. **Failure & Recovery**  
   If the leader fails or loses its lease, the cluster elects a **new leader**, which redistributes segments accordingly.

---

## Example: Segment Distribution in Action

Let’s see how the system behaves step by step when nodes join and leave the cluster.

### Initial State

There are **10 total segments**:  
`[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]`

---

### Step 1 — One Node Joins

```shell
Active nodes: [node1]
Leader: node1
```

**Leader node1** assigns all segments to itself:

| Node   | Segments   |
|---------|------------|
| node1   | [0–9]      |

---

###� Step 2 — Second Node Joins

```shell
Active nodes: [node1, node2]
Leader: node1
```

Leader detects a cluster change and **redistributes segments evenly**:

| Node   | Segments   |
|---------|------------|
| node1   | [0–4]      |
| node2   | [5–9]      |

---

### Step 3 — Third Node Joins

```shell
Active nodes: [node1, node2, node3]
Leader: node1
```

Leader **rebalances** all segments again:

| Node   | Segments   |
|---------|------------|
| node1   | [0–3]      |
| node2   | [4–6]      |
| node3   | [7–9]      |

---

###� Step 4 — Node Fails (node2 leaves)

```shell
Active nodes: [node1, node3]
Leader: node1
```

etcd detects the lease expiration for `node2` → leader redistributes segments:

| Node   | Segments   |
|---------|------------|
| node1   | [0–4]      |
| node3   | [5–9]      |

---

### Step 5 — Leader Fails (node1 goes down)

```shell
Active nodes: [node3]
Leader election triggered...
```

A new leader is **elected via etcd election API**:  
→ `node3` becomes the **leader**  
→ Redistributes all segments to itself:

| Node   | Segments   |
|---------|------------|
| node3   | [0–9]      |

---

### Key Takeaways

- Segment assignments are **stored in etcd** under `cluster/segments`  
- When cluster membership changes, the **leader recalculates** the distribution  
- Other nodes **watch etcd** and update their local assignments automatically  
- Only one node (the leader) performs coordination, ensuring **consensus** and **no duplicate processing**

