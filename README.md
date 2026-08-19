# Sharded Causally Consistent Key-Value Store

A distributed key-value store that partitions its key space across shards,
stays available under network partitions, and guarantees causal consistency to
its clients.

Written in Python with Flask, deployed as Docker containers, and coordinated
entirely over HTTP. Every node makes placement and replication decisions locally
from a cluster view pushed to it and from messages exchanged with its peers.

```
.
├── Dockerfile
├── requirements.txt
└── server.py
```

---

## What it guarantees

| Property | Mechanism |
| --- | --- |
| **Horizontal scalability** | Key space partitioned across *S* shards by consistent hashing; capacity grows with shard count |
| **Availability** | Any replica of the owning shard can serve any operation — no quorum, no leader, no coordination on the write path |
| **Eventual consistency** | Gossip-based anti-entropy with per-peer delivery queues, retried until acknowledged |
| **Causal consistency** | Client-carried version metadata; reads block until the replica has caught up to what the client has already observed |

The trade is explicit: this is an AP system. Writes are acknowledged before they
have replicated, so losing every replica of a shard can lose an acknowledged
write.

---

## How to run

Each node needs a unique integer `NODE_IDENTIFIER` and listens on port 8081.

### With Docker

```bash
docker build -t kvs .
docker network create --subnet=10.0.0.0/16 kvs-net

docker run -d --name node1 --network kvs-net --ip 10.0.0.2 \
  -p 8081:8081 -e NODE_IDENTIFIER=1 kvs
docker run -d --name node2 --network kvs-net --ip 10.0.0.3 \
  -p 8082:8081 -e NODE_IDENTIFIER=2 kvs
docker run -d --name node3 --network kvs-net --ip 10.0.0.4 \
  -p 8083:8081 -e NODE_IDENTIFIER=3 kvs
```

Containers reach each other on their network IPs at port 8081; you reach them
from the host on the mapped ports (8081-8083).

### Without Docker

```bash
pip install -r requirements.txt

NODE_IDENTIFIER=1 python server.py 8081 &
NODE_IDENTIFIER=2 python server.py 8082 &
NODE_IDENTIFIER=3 python server.py 8083 &
```

Use `127.0.0.1:<port>` as the node addresses in the view below.

### Forming a cluster

Nodes start with no view and reject data operations with `503` until one is
installed. Send the same view to every node — here, three nodes across two
shards:

```bash
VIEW='{"view": {
  "shard1": [{"address": "10.0.0.2:8081", "id": 1},
             {"address": "10.0.0.3:8081", "id": 2}],
  "shard2": [{"address": "10.0.0.4:8081", "id": 3}]
}}'

for port in 8081 8082 8083; do
  curl -sX PUT "localhost:$port/view" \
    -H 'Content-Type: application/json' -d "$VIEW"
done
```

### Using it

```bash
# write
curl -X PUT localhost:8081/data/foo -H 'Content-Type: application/json' \
  -d '{"value": "bar", "causal-metadata": {}}'

# read it back from a different node, in a different shard
curl -X GET localhost:8083/data/foo -H 'Content-Type: application/json' \
  -d '{"causal-metadata": {}}'

# every key the receiving node's shard owns
curl -X GET localhost:8082/data -H 'Content-Type: application/json' \
  -d '{"causal-metadata": {}}'
```

The read succeeds from any node regardless of which shard owns `foo` — requests
are proxied transparently.

Pass the `causal-metadata` you receive back on the next request to get causal
guarantees across a session; `{}` starts a fresh one.

### Exercising it

**Resharding** — add or remove a shard in the view and re-issue `PUT /view` to
every node. Keys relocate automatically; roughly *K/S* of them move.

**Partitions** — use `docker network disconnect kvs-net node2` to isolate a
node, write to both sides, then `connect` it again and watch the replicas
reconverge.

---

## API

| Endpoint | Method | Description |
| --- | --- | --- |
| `/ping` | GET | Liveness check |
| `/data/<key>` | PUT | Write a value. Body: `{"value": ..., "causal-metadata": {...}}` |
| `/data/<key>` | GET | Read a value; `404` if the key does not exist |
| `/data` | GET | Read every key this node's shard owns |
| `/view` | PUT | Install a new cluster view, triggering a reshard |

A view groups nodes into named shards:

```json
{ "view": {
    "shard1": [ { "address": "10.0.0.2:8081", "id": 1 } ],
    "shard2": [ { "address": "10.0.0.3:8081", "id": 2 } ]
} }
```

First-time clients send `"causal-metadata": {}`. The value is opaque and should
be echoed back unmodified on subsequent requests.

`/kvs/internal/gossip` and `/kvs/internal/handoff` are used between nodes and
are not part of the client contract.

---

## Architecture

### Sharding and transparent proxying

Each node belongs to exactly one shard and stores only that shard's keys.
Clients do not need to know the partitioning — a node that receives a request
for a key it does not own forwards it to a node that does and returns the
result as its own, so the cluster presents a single logical store.

Forwarded requests are tagged, so a node that has been asked to serve a key it
does not own returns an error rather than forwarding again. That closes the
door on proxy loops during the window when nodes briefly disagree about the
topology. If a replica is unreachable the request is retried against the
shard's other replicas rather than surfacing a transient failure to the client.

### Consistent hashing

Ownership is decided by a hash ring with 256 virtual nodes per shard.

The naive approach, `hash(key) % num_shards`, remaps nearly every key whenever
the shard count changes — adding one shard to a cluster of four relocates
roughly 80% of the data. A hash ring only moves the keys in the affected arcs,
about *K/S*.

Virtual nodes matter for the same reason. Placed once, a new shard would claim a
single contiguous arc and steal disproportionately from one neighbour; with 256
pseudo-random placements it takes a thin slice from every existing shard, which
keeps both storage load and migration cost evenly spread.

The result that matters in practice: **resharding gets cheaper as the cluster
grows**, which is the opposite of the naive behavior.

### Pull-based resharding

When a new view arrives, each node independently:

1. builds the new ring from the shard names in the view;
2. drops the keys it no longer owns, keeping a snapshot to serve others;
3. computes which keys it is *newly* responsible for;
4. requests exactly those keys from the nodes of the previous view;
5. commits the new view once the transfer completes.

Data is **pulled, not pushed**, and that decision is load-bearing. A node being
removed from the cluster is never told — it receives no new view, cannot know
the new topology, and therefore has no way to push its data anywhere useful.
Under a pull model it does not need to know: the nodes inheriting its keys come
and ask, supplying the shard layout in the request so the departing node can
compute exactly what to hand over.

Handoff requests carry shard *names* only, never addresses. Two nodes may hold
different but equally valid addresses for the same peer, so topology is never
propagated node to node — each node trusts only the view delivered directly to
it.

A node staying in its shard pulls only the keys that actually changed hands. A
node joining a shard it was not previously in pulls that shard's full key set,
since it starts empty.

### Causal consistency

Every response carries an opaque `causal-metadata` map of `{key: version}`
describing everything the client has observed; the client passes it back on its
next request. A node refuses to serve a read until its own replica is at least
as new as the versions the client already knows about, so a client can never
observe the system moving backwards in causal time. Reading a value also
inherits that value's dependencies, which makes causality transitive across
keys.

Cross-shard causality falls out of this: each shard enforces dependencies on the
keys it owns, and dependencies on other shards' keys are enforced by those
shards when the client reads them.

### Versioning and conflict resolution

Versions are **per-key vector clocks**, so concurrent writes are detected rather
than silently lost. Where two versions are genuinely concurrent, a
`(timestamp, node id)` arbitration order breaks the tie. Because node ids are
unique this is a *total* order, so every replica independently converges on the
same winner with zero coordination.

### Anti-entropy

Each node tracks, per peer, the set of keys that peer has not yet acknowledged.
A gossip round sends only those keys and clears them only on a successful
response.

Tracking this per peer rather than as one global "recently written" set is what
makes delivery guaranteed: a round that reaches one replica does not discharge
the obligation to the others. Steady-state traffic stays proportional to the
write rate rather than to the size of the store, and under a partition the
queues simply accumulate and drain once the partition heals.

Gossip never crosses a shard boundary — replicas of different shards hold
disjoint key sets, so there is nothing useful to exchange.
