"""A horizontally sharded, causally consistent key-value store.

This extends the fully replicated store from assignment 3 with sharding. The
key space is partitioned across S shards; every node belongs to exactly one
shard and replicates only that shard's keys. A node that receives a request for
a key it does not own transparently proxies it to a node that does, so the
cluster still looks like a single store from the outside.

Two design choices carry most of the weight:

* Consistent hashing with virtual nodes decides key ownership. Adding or
  removing a shard remaps roughly K/S keys instead of nearly all of them, so
  resharding cost falls as the cluster grows.

* Resharding is pull-based. On a view change each node computes which keys it
  is newly responsible for and requests exactly those from the nodes of the
  previous view. Pulling rather than pushing means a node being removed from
  the view -- which never receives the view change and cannot know where to
  send its data -- still gets drained correctly.

Causality is tracked with per-key vector clocks. Concurrent writes are detected
rather than silently lost, and are then resolved with a deterministic
(timestamp, node id) arbitration order so every replica converges identically.
"""

import bisect
import copy
import hashlib
import logging
import os
import random
import sys
import threading
import time

import requests
from flask import Flask, jsonify, request

LOG_FORMAT = "%(asctime)s %(levelname)s [node %(node_id)s] %(message)s"

VIRTUAL_NODES_PER_SHARD = 256

GOSSIP_INTERVAL_SECONDS = 1.0
GOSSIP_TIMEOUT_SECONDS = 2.0
PROXY_TIMEOUT_SECONDS = 5.0
HANDOFF_TIMEOUT_SECONDS = 15.0
HANDOFF_ATTEMPTS = 3
READ_TIMEOUT_SECONDS = 10.0
READ_POLL_INTERVAL_SECONDS = 0.1
PROXY_RETRY_INTERVAL_SECONDS = 0.25

# Marks a request that has already been forwarded once, so a node that does not
# own the key cannot bounce it onward and create a proxy loop.
PROXY_HEADER = "X-KVS-Proxied"

READ_OK = "ok"
READ_MISSING = "missing"
READ_STALE = "stale"


class ConsistentHashRing:
    """Maps keys to shard names on a hash ring.

    Each shard is placed at many pseudo-random positions ("virtual nodes") so
    that load stays even and so that adding a shard steals a thin slice from
    every existing shard rather than a contiguous block from one neighbour.
    """

    def __init__(self, shard_names=()):
        self._positions = []
        self._owner_by_position = {}
        for shard_name in sorted(shard_names):
            self._add_shard(shard_name)
        self._positions.sort()

    @staticmethod
    def _hash(token):
        return int(hashlib.sha256(token.encode("utf-8")).hexdigest(), 16)

    def _add_shard(self, shard_name):
        for replica_index in range(VIRTUAL_NODES_PER_SHARD):
            position = self._hash(f"{shard_name}#{replica_index}")
            self._positions.append(position)
            self._owner_by_position[position] = shard_name

    def __bool__(self):
        return bool(self._positions)

    def owner_of(self, key):
        """Return the shard responsible for key, or None if the ring is empty."""
        if not self._positions:
            return None
        index = bisect.bisect_left(self._positions, self._hash(key))
        if index == len(self._positions):
            index = 0  # wrap around the ring
        return self._owner_by_position[self._positions[index]]


def compare_clocks(left, right):
    """Compare two vector clocks.

    Returns 1 if left strictly dominates, -1 if right does, 0 if equal, and
    None if the two are concurrent.
    """
    left = left or {}
    right = right or {}
    left_ahead = right_ahead = False
    for node_id in set(left) | set(right):
        a, b = left.get(node_id, 0), right.get(node_id, 0)
        if a > b:
            left_ahead = True
        elif a < b:
            right_ahead = True
    if left_ahead and right_ahead:
        return None
    if left_ahead:
        return 1
    if right_ahead:
        return -1
    return 0


def compare_versions(left, right):
    """Total order over versions, falling back to arbitration when concurrent.

    A version is {"clock": {node: counter}, "ts": ms, "node": id}. Causality
    wins where it applies; genuinely concurrent writes are ordered by
    (timestamp, node id), which is deterministic on every replica.
    """
    if right is None:
        return 0 if left is None else 1
    if left is None:
        return -1

    causal = compare_clocks(left.get("clock"), right.get("clock"))
    if causal is not None:
        return causal

    if left.get("ts") != right.get("ts"):
        return 1 if left.get("ts", 0) > right.get("ts", 0) else -1
    if left.get("node") != right.get("node"):
        return 1 if left.get("node", 0) > right.get("node", 0) else -1
    return 0


def merge_clocks(left, right):
    merged = dict(left or {})
    for node_id, counter in (right or {}).items():
        merged[node_id] = max(merged.get(node_id, 0), counter)
    return merged


class ShardedNode:
    def __init__(self, import_name, node_id):
        self.node_id = node_id

        # key -> {"value": str, "version": {...}, "deps": {key: version}}
        self.store = {}
        # Snapshot taken at the start of a view change so that nodes which are
        # still resharding can answer handoff requests for keys they have
        # already dropped.
        self.previous_store = {}
        # peer id -> set of keys that peer has not acknowledged yet. Tracking
        # this per peer (rather than one global "recently written" set) is what
        # guarantees delivery: a round that reaches one replica cannot
        # discharge our obligation to the others.
        self.pending = {}
        self.store_lock = threading.RLock()

        self.shard_id = None            # name of the shard this node belongs to
        self.shard_map = {}             # shard name -> [{"address", "id"}, ...]
        self.ring = ConsistentHashRing()
        self.view_lock = threading.RLock()

        self.app = Flask(import_name)
        self._register_routes()

        self._gossip_thread = threading.Thread(target=self._gossip_loop, daemon=True)
        self._gossip_thread.start()

    # ------------------------------------------------------------------
    # Placement helpers
    # ------------------------------------------------------------------

    def is_online(self):
        return self.shard_id is not None

    def owner_of(self, key):
        with self.view_lock:
            return self.ring.owner_of(key)

    def nodes_in_shard(self, shard_name):
        with self.view_lock:
            return list(self.shard_map.get(shard_name, []))

    def all_nodes(self):
        with self.view_lock:
            return [node for nodes in self.shard_map.values() for node in nodes]

    # ------------------------------------------------------------------
    # Intra-shard replication
    # ------------------------------------------------------------------

    def _gossip_loop(self):
        while True:
            time.sleep(GOSSIP_INTERVAL_SECONDS)
            try:
                self._gossip_once()
            except Exception:
                logging.exception("gossip round failed")

    def _gossip_once(self):
        """Push everything each peer in our own shard is still missing.

        Gossip never crosses a shard boundary: replicas of different shards
        hold disjoint key sets, so there is nothing useful to exchange.
        """
        if not self.is_online():
            return
        peers = [n for n in self.nodes_in_shard(self.shard_id) if n["id"] != self.node_id]

        for peer in peers:
            with self.store_lock:
                outstanding = set(self.pending.get(peer["id"], ()))
                if not outstanding:
                    continue
                payload = {k: self.store[k] for k in outstanding if k in self.store}

            try:
                response = requests.post(
                    f"http://{peer['address']}/kvs/internal/gossip",
                    json=payload,
                    timeout=GOSSIP_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                logging.debug("gossip to %s failed: %s", peer["address"], exc)
                continue

            if response.status_code == 200:
                with self.store_lock:
                    queue = self.pending.get(peer["id"])
                    if queue is not None:
                        queue -= outstanding

    def _enqueue(self, key):
        """Mark a key as needing delivery to every peer. Caller holds the lock."""
        for queue in self.pending.values():
            queue.add(key)

    def _merge_record(self, key, record):
        """Adopt an incoming record if it beats ours. Caller holds the lock."""
        incoming = record.get("version")
        if incoming is None:
            return False
        current = self.store.get(key)
        if current is not None and compare_versions(incoming, current["version"]) <= 0:
            return False
        self.store[key] = {
            "value": record["value"],
            "version": incoming,
            "deps": record.get("deps", {}),
        }
        self._enqueue(key)
        return True

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def _dependencies_satisfied(self, client_metadata):
        """Check the dependencies this shard is actually able to check.

        Dependencies on keys owned by other shards are enforced by those
        shards when the client reads them, so checking them here would block on
        data we are not allowed to hold.
        """
        for key, required in client_metadata.items():
            if self.ring.owner_of(key) != self.shard_id:
                continue
            record = self.store.get(key)
            if record is None:
                return False
            if compare_versions(record["version"], required) < 0:
                return False
        return True

    def _try_read(self, key, client_metadata):
        """Attempt one causally safe read. Caller holds the store lock."""
        if not self._dependencies_satisfied(client_metadata):
            return READ_STALE, None, None, None

        record = self.store.get(key)
        known_version = client_metadata.get(key)

        if record is None:
            if known_version is not None:
                return READ_STALE, None, None, None
            return READ_MISSING, None, None, None

        if compare_versions(record["version"], known_version) < 0:
            return READ_STALE, None, None, None

        return READ_OK, record["value"], record["version"], record["deps"]

    # ------------------------------------------------------------------
    # Proxying
    # ------------------------------------------------------------------

    def _proxy(self, shard_name, method, path, body):
        """Forward a request to the shard that owns the key.

        The spec requires that we do not answer the client until some node in
        the owning shard responds, so this retries across the shard's replicas
        rather than surfacing a transient failure.
        """
        deadline = time.time() + READ_TIMEOUT_SECONDS
        while True:
            candidates = self.nodes_in_shard(shard_name)
            random.shuffle(candidates)
            for node in candidates:
                try:
                    response = requests.request(
                        method,
                        f"http://{node['address']}{path}",
                        json=body,
                        headers={PROXY_HEADER: "1"},
                        timeout=PROXY_TIMEOUT_SECONDS,
                    )
                except requests.RequestException as exc:
                    logging.debug("proxy to %s failed: %s", node["address"], exc)
                    continue
                if response.status_code < 500:
                    return response
            if time.time() >= deadline:
                return None
            time.sleep(PROXY_RETRY_INTERVAL_SECONDS)

    @staticmethod
    def _relay(response):
        if response is None:
            return jsonify({"error": "no node in the responsible shard is reachable"}), 503
        return (response.content, response.status_code, {"Content-Type": "application/json"})

    # ------------------------------------------------------------------
    # Resharding
    # ------------------------------------------------------------------

    def _collect_handoff(self, old_shard_names, new_shard_names, target_shard, full):
        """Return the records the requester is newly responsible for.

        Ownership is recomputed locally from the shard *names* the requester
        sends. Nodes never exchange addresses or view structure, because two
        nodes may legitimately hold different addresses for the same peer.
        """
        old_ring = ConsistentHashRing(old_shard_names)
        new_ring = ConsistentHashRing(new_shard_names)

        with self.store_lock:
            # Union of what we hold now and what we held before this view
            # change began, preferring the newer record for each key.
            candidates = dict(self.previous_store)
            for key, record in self.store.items():
                existing = candidates.get(key)
                if existing is None or compare_versions(record["version"], existing["version"]) > 0:
                    candidates[key] = record

            handoff = {}
            for key, record in candidates.items():
                if new_ring.owner_of(key) != target_shard:
                    continue
                # A node that stays in the same shard only needs the keys that
                # actually moved -- this is what keeps resharding O(K/S).
                if not full and old_ring and old_ring.owner_of(key) == target_shard:
                    continue
                handoff[key] = record
        return handoff

    def _pull_from_peer(self, node, payload):
        for attempt in range(HANDOFF_ATTEMPTS):
            try:
                response = requests.post(
                    f"http://{node['address']}/kvs/internal/handoff",
                    json=payload,
                    timeout=HANDOFF_TIMEOUT_SECONDS,
                )
                if response.status_code == 200:
                    return response.json()
                logging.warning(
                    "handoff from node %s returned %s", node["id"], response.status_code
                )
            except requests.RequestException as exc:
                logging.warning(
                    "handoff from node %s failed (attempt %d): %s", node["id"], attempt + 1, exc
                )
            time.sleep(PROXY_RETRY_INTERVAL_SECONDS)
        return {}

    def _reshard(self, new_view):
        """Move to a new view, relocating data to wherever it now belongs."""
        with self.view_lock:
            old_shard_map = self.shard_map
            old_shard_id = self.shard_id
            old_shard_names = sorted(old_shard_map)

        new_shard_names = sorted(new_view)
        new_ring = ConsistentHashRing(new_shard_names)

        my_shard = next(
            (name for name, nodes in new_view.items()
             if any(n["id"] == self.node_id for n in nodes)),
            None,
        )
        if my_shard is None:
            return None

        # Snapshot before mutating, so peers mid-reshard can still pull from us.
        with self.store_lock:
            self.previous_store = copy.deepcopy(self.store)
            self.store = {
                key: record for key, record in self.store.items()
                if new_ring.owner_of(key) == my_shard
            }

        # If we changed shards (or are joining for the first time) we hold none
        # of our new shard's data and need a complete copy, not just the delta.
        needs_full_copy = old_shard_id != my_shard

        payload = {
            "old_shards": old_shard_names,
            "new_shards": new_shard_names,
            "target": my_shard,
            "full": needs_full_copy,
        }

        # Ask every node from the old view -- including any being removed, who
        # will never hear about this view change themselves -- plus the nodes
        # of the new view, in case data has already migrated to them.
        peers, seen_ids = [], {self.node_id}
        for source in (old_shard_map, new_view):
            for nodes in source.values():
                for node in nodes:
                    if node["id"] not in seen_ids:
                        seen_ids.add(node["id"])
                        peers.append(node)

        pulled = 0
        for node in peers:
            for key, record in self._pull_from_peer(node, payload).items():
                with self.store_lock:
                    if self._merge_record(key, record):
                        pulled += 1

        with self.view_lock:
            self.shard_map = new_view
            self.ring = new_ring
            self.shard_id = my_shard
            peer_ids = {n["id"] for n in new_view[my_shard] if n["id"] != self.node_id}

        with self.store_lock:
            # Shard membership may have changed underneath us, so rebuild the
            # queues from scratch: every peer starts owing the whole shard.
            self.pending = {peer_id: set(self.store) for peer_id in peer_ids}

        logging.info(
            "view change complete: shard=%s shards=%d pulled=%d keys=%d",
            my_shard, len(new_shard_names), pulled, len(self.store),
        )
        return my_shard

    # ------------------------------------------------------------------
    # HTTP API
    # ------------------------------------------------------------------

    def _register_routes(self):
        app = self.app

        @app.route("/ping", methods=["GET"])
        def ping():
            return jsonify({"status": "ok"}), 200

        @app.route("/kvs/internal/gossip", methods=["POST"])
        def receive_gossip():
            records = request.get_json(force=True, silent=True)
            if not isinstance(records, dict):
                return jsonify({"error": "expected a json object of records"}), 400
            with self.store_lock:
                for key, record in records.items():
                    self._merge_record(key, record)
            return jsonify({"status": "ok"}), 200

        @app.route("/kvs/internal/handoff", methods=["POST"])
        def send_handoff():
            body = request.get_json(force=True, silent=True)
            if not isinstance(body, dict) or "target" not in body:
                return jsonify({"error": "expected a handoff request"}), 400
            handoff = self._collect_handoff(
                body.get("old_shards", []),
                body.get("new_shards", []),
                body["target"],
                bool(body.get("full")),
            )
            return jsonify(handoff), 200

        @app.route("/data/<key>", methods=["PUT"])
        def put_key(key):
            if not self.is_online():
                return jsonify({"error": "node is not in the current view"}), 503

            body = request.get_json(force=True, silent=True)
            if not isinstance(body, dict) or "value" not in body:
                return jsonify({"error": "body must be json with a 'value' field"}), 400

            owner = self.owner_of(key)
            if owner != self.shard_id:
                if request.headers.get(PROXY_HEADER):
                    # Already forwarded once; bounce back so the origin retries
                    # elsewhere instead of starting a proxy loop.
                    return jsonify({"error": "not responsible for this key"}), 503
                return self._relay(self._proxy(owner, "PUT", f"/data/{key}", body))

            client_metadata = body.get("causal-metadata") or {}
            if not isinstance(client_metadata, dict):
                client_metadata = {}

            with self.store_lock:
                current = self.store.get(key)
                base = merge_clocks(
                    (current or {}).get("version", {}).get("clock"),
                    (client_metadata.get(key) or {}).get("clock"),
                )
                base[str(self.node_id)] = base.get(str(self.node_id), 0) + 1
                version = {
                    "clock": base,
                    "ts": int(time.time() * 1000),
                    "node": self.node_id,
                }
                self.store[key] = {
                    "value": body["value"],
                    "version": version,
                    "deps": dict(client_metadata),
                }
                self._enqueue(key)

            response_metadata = dict(client_metadata)
            response_metadata[key] = version
            return jsonify({"causal-metadata": response_metadata}), 200

        @app.route("/data/<key>", methods=["GET"])
        def get_key(key):
            if not self.is_online():
                return jsonify({"error": "node is not in the current view"}), 503

            body = request.get_json(force=True, silent=True) or {}
            client_metadata = body.get("causal-metadata") or {}
            if not isinstance(client_metadata, dict):
                client_metadata = {}

            owner = self.owner_of(key)
            if owner != self.shard_id:
                if request.headers.get(PROXY_HEADER):
                    return jsonify({"error": "not responsible for this key"}), 503
                return self._relay(self._proxy(owner, "GET", f"/data/{key}", body))

            deadline = time.time() + READ_TIMEOUT_SECONDS
            while True:
                with self.store_lock:
                    outcome, value, version, deps = self._try_read(key, client_metadata)

                if outcome == READ_MISSING:
                    return "", 404
                if outcome == READ_OK:
                    response_metadata = dict(client_metadata)
                    response_metadata.update(deps)
                    response_metadata[key] = version
                    return jsonify({"value": value, "causal-metadata": response_metadata}), 200

                if time.time() >= deadline:
                    return jsonify({"error": "timed out waiting for a causally valid version"}), 503
                time.sleep(READ_POLL_INTERVAL_SECONDS)

        @app.route("/data", methods=["GET"])
        def get_all():
            if not self.is_online():
                return jsonify({"error": "node is not in the current view"}), 503

            body = request.get_json(force=True, silent=True) or {}
            client_metadata = body.get("causal-metadata") or {}
            if not isinstance(client_metadata, dict):
                client_metadata = {}

            deadline = time.time() + READ_TIMEOUT_SECONDS
            while True:
                with self.store_lock:
                    # Only this shard's keys; this endpoint deliberately does
                    # not proxy to the other shards.
                    candidates = {
                        key for key in set(self.store) | set(client_metadata)
                        if self.ring.owner_of(key) == self.shard_id
                    }
                    items = {}
                    response_metadata = dict(client_metadata)
                    blocked = False

                    for candidate in candidates:
                        outcome, value, version, deps = self._try_read(candidate, client_metadata)
                        if outcome == READ_STALE:
                            blocked = True
                            break
                        if outcome == READ_OK:
                            items[candidate] = value
                            response_metadata.update(deps)
                            response_metadata[candidate] = version

                if not blocked:
                    return jsonify({"items": items, "causal-metadata": response_metadata}), 200

                if time.time() >= deadline:
                    return jsonify({"error": "timed out waiting for a causally valid version"}), 503
                time.sleep(READ_POLL_INTERVAL_SECONDS)

        @app.route("/view", methods=["PUT"])
        def put_view():
            body = request.get_json(force=True, silent=True)
            if not isinstance(body, dict) or "view" not in body:
                return jsonify({"error": "body must be json with a 'view' field"}), 400

            new_view = body["view"]
            if not isinstance(new_view, dict) or not new_view:
                return jsonify({"error": "'view' must be a non-empty object of shards"}), 400
            for shard_name, nodes in new_view.items():
                if not isinstance(nodes, list):
                    return jsonify({"error": f"shard '{shard_name}' must map to a list"}), 400
                for node in nodes:
                    if not isinstance(node, dict) or "address" not in node or "id" not in node:
                        return jsonify({"error": "each node needs an 'address' and an 'id'"}), 400

            if self._reshard(new_view) is None:
                return jsonify({"error": "this node is not present in the new view"}), 400
            return jsonify({"status": "ok"}), 200


def _log_factory_with_node_id(base_factory, node_id):
    def factory(*args, **kwargs):
        record = base_factory(*args, **kwargs)
        record.node_id = node_id
        return record

    return factory


def main():
    try:
        node_id = int(os.environ["NODE_IDENTIFIER"])
    except (KeyError, ValueError):
        print("NODE_IDENTIFIER must be set to an integer", file=sys.stderr)
        return 1

    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.setLogRecordFactory(
        _log_factory_with_node_id(logging.getLogRecordFactory(), node_id)
    )

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
    node = ShardedNode(__name__, node_id)
    node.app.run(host="0.0.0.0", port=port, threaded=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
