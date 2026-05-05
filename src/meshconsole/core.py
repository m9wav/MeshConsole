#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MeshConsole Core / Orchestrator
--------------------------------
Central orchestrator managing backends, database, and web UI.

v3.2.0: Multi-device support.  ``self.backends`` is a list of MeshBackend
instances; legacy ``self._backend`` / ``self._meshcore_backend`` are backward-
compatible properties that search the list.

Author: M9WAV
License: MIT
Version: 3.4.4
"""

import argparse
import configparser
import json
import logging
import queue
import signal
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import os
import hashlib
import secrets

# Flask imports (used by require_auth decorator for backward compat)
from flask import jsonify, session
from functools import wraps

# ── New modular imports ───────────────────────────────────────────
from meshconsole.models import (
    BackendType,
    ConnectionType,
    UnifiedPacket,
    UnifiedNode,
    PacketSummary,
)
from meshconsole.database import DatabaseHandler
from meshconsole.config import MeshConsoleConfig
from meshconsole.backend.base import MeshBackend

# Set up module-level logger
logger = logging.getLogger(__name__)

# Define constants
DEFAULT_CONFIG_FILE = 'config.ini'


# ── Custom exceptions ─────────────────────────────────────────────

class MeshtasticToolError(Exception):
    """Custom exception class for Meshtastic Tool errors."""
    pass


# ── Authentication helpers (kept here for wsgi.py backward compat) ──

def hash_password(password):
    """Hash a password for secure storage."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def check_password(password, hashed):
    """Check if password matches the stored hash."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest() == hashed

def require_auth(f):
    """Decorator to require authentication for sensitive endpoints."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        config = configparser.ConfigParser()
        config.read(DEFAULT_CONFIG_FILE)
        auth_password = config.get('Security', 'auth_password', fallback='')
        if not auth_password:
            return f(*args, **kwargs)
        if not session.get('authenticated'):
            return jsonify({'success': False, 'error': 'Authentication required', 'auth_required': True}), 401
        auth_timeout = config.getint('Security', 'auth_timeout', fallback=60)
        if 'auth_time' in session:
            auth_time = datetime.fromisoformat(session['auth_time'])
            if datetime.now() - auth_time > timedelta(minutes=auth_timeout):
                session.clear()
                return jsonify({'success': False, 'error': 'Session expired', 'auth_required': True}), 401
        return f(*args, **kwargs)
    return decorated_function


# ══════════════════════════════════════════════════════════════════
# GeoResolver — coordinate-based hash collision disambiguation
# ══════════════════════════════════════════════════════════════════

class GeoResolver:
    """Geographic disambiguation for 1-byte MeshCore path hash collisions.

    Uses node coordinates from live MeshCore contacts and historical
    NODEINFO packets to score candidates based on proximity to resolved
    neighbors.  Three-phase scoring with increasing cost and decreasing
    confidence thresholds:

      Phase 1 — Regional clustering (avg distance to all neighbors)
      Phase 2 — Nearest-k neighbours + different-closest check
      Phase 3 — Route coherence (total route distance with substitution)

    Thread-safe.  Coordinate cache refreshes on a 5-minute TTL.
    """

    _COORD_TTL = 300  # seconds

    def __init__(self):
        self._lock = threading.Lock()
        self._coords: dict[str, tuple[float, float]] = {}
        self._last_refresh: float = 0.0

    # ── Haversine ──────────────────────────────────────────────

    @staticmethod
    def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Great-circle distance in km between two (lat, lon) points."""
        import math
        R = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(lat1))
             * math.cos(math.radians(lat2))
             * math.sin(dlon / 2) ** 2)
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # ── Coordinate cache ───────────────────────────────────────

    def refresh_coords(self, backends: list, db_handler) -> None:
        """Rebuild the name → (lat, lon) cache from live contacts + DB."""
        new_coords: dict[str, tuple[float, float]] = {}

        # Source 1: live MeshCore contacts
        for b in backends:
            if b.backend_type == BackendType.MESHCORE:
                for _prefix, contact in getattr(b, '_contacts', {}).items():
                    name = contact.get('adv_name', '')
                    lat = contact.get('adv_lat')
                    lon = contact.get('adv_lon')
                    if name and lat is not None and lon is not None:
                        try:
                            lat_f, lon_f = float(lat), float(lon)
                            if abs(lat_f) <= 90 and abs(lon_f) <= 180 and abs(lat_f) > 0.01:
                                new_coords[name] = (lat_f, lon_f)
                        except (ValueError, TypeError):
                            pass

        # Source 2: historical NODEINFO packets (fallback)
        try:
            with db_handler.lock:
                db_handler.cursor.execute(
                    "SELECT from_id, raw_packet FROM packets "
                    "WHERE port_name IN ('NODEINFO','NODEINFO_APP') "
                    "AND backend='meshcore' ORDER BY timestamp DESC"
                )
                rows = db_handler.cursor.fetchall()
        except Exception:
            rows = []

        seen = set()
        for from_id, raw_json in rows:
            if from_id in seen:
                continue
            seen.add(from_id)
            try:
                raw = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                name = raw.get('adv_name', '')
                if not name or name in new_coords:
                    continue
                lat = raw.get('adv_lat') or raw.get('latitude')
                lon = raw.get('adv_lon') or raw.get('longitude')
                if lat is not None and lon is not None:
                    lat_f, lon_f = float(lat), float(lon)
                    if abs(lat_f) <= 90 and abs(lon_f) <= 180 and abs(lat_f) > 0.01:
                        new_coords[name] = (lat_f, lon_f)
            except (ValueError, TypeError, json.JSONDecodeError):
                pass

        with self._lock:
            self._coords = new_coords
            self._last_refresh = time.time()

        logger.debug(f"GeoResolver: refreshed {len(new_coords)} coordinate entries")

    def _ensure_fresh(self, backends: list, db_handler) -> None:
        """Refresh if stale."""
        if time.time() - self._last_refresh > self._COORD_TTL:
            self.refresh_coords(backends, db_handler)

    @property
    def coord_count(self) -> int:
        with self._lock:
            return len(self._coords)

    def get_coords(self, name: str):
        with self._lock:
            return self._coords.get(name)

    # ── Scoring ────────────────────────────────────────────────

    def score_candidates(
        self,
        candidates: list[str],
        resolved_neighbors: list[str],
        all_hops: list[dict] | None = None,
    ) -> list[tuple[str, float, float]]:
        """Score candidates geographically.  Three-phase.

        Returns [(name, distance_score, confidence), ...] sorted by
        distance ascending (lower = better).  confidence is 0.0-1.0.
        """
        with self._lock:
            coords = dict(self._coords)

        # Gather coordinates
        neighbor_coords = [(n, coords[n]) for n in resolved_neighbors if n in coords]
        cand_coords = {c: coords[c] for c in candidates if c in coords}

        if len(cand_coords) < 2 or not neighbor_coords:
            return [(c, 0.0, 0.0) for c in candidates]

        # ── Phase 1: regional clustering ──
        p1 = {}
        for cname, (clat, clon) in cand_coords.items():
            dists = [self._haversine(clat, clon, nlat, nlon)
                     for _, (nlat, nlon) in neighbor_coords]
            p1[cname] = sum(dists) / len(dists)

        ranked_p1 = sorted(p1.values())
        if len(ranked_p1) >= 2 and ranked_p1[0] > 0:
            ratio = ranked_p1[1] / ranked_p1[0]
            if ratio >= 1.5:
                best = min(p1, key=p1.get)
                conf = min(0.85, 0.6 + 0.05 * min(ratio, 5))
                return self._build(candidates, cand_coords, p1, conf, best)

        # ── Phase 2: nearest-k + different-closest ──
        K = min(3, len(neighbor_coords))
        p2 = {}
        closest = {}
        for cname, (clat, clon) in cand_coords.items():
            dists = sorted(
                (self._haversine(clat, clon, nlat, nlon), nname)
                for nname, (nlat, nlon) in neighbor_coords
            )
            top_k = dists[:K]
            p2[cname] = sum(d for d, _ in top_k) / len(top_k)
            closest[cname] = top_k[0][1] if top_k else None

        ranked_p2 = sorted(p2.values())
        diff_closest = len(set(closest.values())) > 1
        if len(ranked_p2) >= 2 and ranked_p2[0] > 0:
            ratio = ranked_p2[1] / ranked_p2[0]
            if ratio >= 1.3 and diff_closest:
                best = min(p2, key=p2.get)
                conf = min(0.80, 0.55 + 0.05 * min(ratio, 5))
                return self._build(candidates, cand_coords, p2, conf, best)

        # ── Phase 3: route coherence ──
        if all_hops and len(cand_coords) <= 5:
            p3 = self._route_coherence(cand_coords, all_hops, coords)
            if p3:
                ranked_p3 = sorted(p3.values())
                if len(ranked_p3) >= 2 and ranked_p3[0] > 0:
                    ratio = ranked_p3[1] / ranked_p3[0]
                    if ratio >= 1.2:
                        best = min(p3, key=p3.get)
                        conf = min(0.75, 0.5 + 0.05 * min(ratio, 5))
                        return self._build(candidates, cand_coords, p3, conf, best)

        # Inconclusive — return Phase 1 scores with zero confidence
        best = min(p1, key=p1.get) if p1 else candidates[0]
        return self._build(candidates, cand_coords, p1, 0.0, best)

    def _route_coherence(
        self,
        cand_coords: dict[str, tuple[float, float]],
        all_hops: list[dict],
        coords: dict[str, tuple[float, float]],
    ) -> dict[str, float]:
        """Substitute each candidate into the route, measure total distance."""
        cand_names = set(cand_coords)
        scores = {}
        for cname, (clat, clon) in cand_coords.items():
            total = 0.0
            prev = None
            segments = 0
            for hop in all_hops:
                hop_cands = hop.get('candidate_names') or []
                if cname in hop_cands:
                    cur = (clat, clon)
                else:
                    # Use resolved name for this hop
                    cur = coords.get(hop.get('name', ''))
                if cur and prev:
                    total += self._haversine(prev[0], prev[1], cur[0], cur[1])
                    segments += 1
                if cur:
                    prev = cur
            if segments > 0:
                scores[cname] = total
        return scores

    @staticmethod
    def _build(
        candidates: list[str],
        cand_coords: dict[str, tuple[float, float]],
        scores: dict[str, float],
        confidence: float,
        best: str,
    ) -> list[tuple[str, float, float]]:
        """Build sorted result.  Candidates without coords get a penalty."""
        max_s = max(scores.values()) if scores else 1.0
        penalty = max_s * 2 if max_s > 0 else 1000.0
        result = []
        for c in candidates:
            if c in scores:
                conf = confidence if c == best else max(0.0, confidence - 0.2)
                result.append((c, scores[c], conf))
            else:
                result.append((c, penalty, 0.0))
        result.sort(key=lambda x: x[1])
        return result


# ══════════════════════════════════════════════════════════════════
# RouteAnalyzer — learns path hash adjacencies for better decoding
# ══════════════════════════════════════════════════════════════════

class RouteAnalyzer:
    """Learns which nodes commonly appear near each other in MeshCore routes.

    MeshCore path hashes are 1-byte values (first byte of each node's public
    key).  With 300+ nodes and only 256 possible hash values, collisions are
    inevitable.  This class tracks adjacency patterns — which node candidates
    appear next to which neighbors — so that ambiguous hashes can be resolved
    by context over time.

    Thread-safe: writes happen from backend packet callbacks (background
    threads), reads happen from web request threads.  An in-memory cache
    provides <1ms lookups; SQLite is used only for persistence.
    """

    # How many pending writes to batch before flushing to SQLite
    _FLUSH_THRESHOLD = 50

    def __init__(self, db_handler):
        self._db = db_handler
        self._lock = threading.Lock()

        # In-memory adjacency cache:
        # {(node_hash, neighbor_hash): {candidate_name: count}}
        self._cache: dict[tuple[str, str], dict[str, int]] = {}

        # Secondary index: hash -> set of neighbor hashes (O(1) neighbor lookup)
        self._hash_neighbors: dict[str, set[str]] = {}

        # Pre-materialized graph data (rebuilt incrementally)
        self._graph_edge_counts: dict[tuple[str, str], int] = {}
        self._graph_node_hashes: set[str] = set()
        self._graph_generation: int = 0

        # Pending writes waiting to be flushed to SQLite
        self._pending: list[tuple[str, str, str, str]] = []

        # Geographic resolver for coordinate-based disambiguation
        self._geo = GeoResolver()

        # Load existing data from database
        self._load_from_db()

    def _load_from_db(self) -> None:
        """Bootstrap the in-memory cache from the SQLite table."""
        rows = self._db.load_adjacency_all()
        for node_hash, neighbor_hash, candidate, count in rows:
            key = (node_hash, neighbor_hash)
            if key not in self._cache:
                self._cache[key] = {}
            self._cache[key][candidate] = count
            # Secondary index
            self._hash_neighbors.setdefault(node_hash, set()).add(neighbor_hash)
        # Materialize graph edge counts
        self._rebuild_graph_materialization()
        if rows:
            logger.info(f"RouteAnalyzer: loaded {len(rows)} adjacency records")

    def _rebuild_graph_materialization(self) -> None:
        """Rebuild pre-computed graph edge counts from the adjacency cache."""
        edge_counts: dict[tuple[str, str], int] = {}
        node_hashes: set[str] = set()
        for (node_hash, neighbor_hash), candidates in self._cache.items():
            total = sum(candidates.values())
            if total < 2:
                continue
            edge_key = tuple(sorted([node_hash, neighbor_hash]))
            edge_counts[edge_key] = edge_counts.get(edge_key, 0) + total
            node_hashes.add(node_hash)
            node_hashes.add(neighbor_hash)
        # Filter weak edges
        edge_counts = {k: v for k, v in edge_counts.items() if v >= 2}
        node_hashes = set()
        for a, b in edge_counts:
            node_hashes.add(a)
            node_hashes.add(b)
        self._graph_edge_counts = edge_counts
        self._graph_node_hashes = node_hashes
        self._graph_generation += 1

    def learn_route(self, hops: list[dict]) -> None:
        """Learn adjacency patterns from a decoded route.

        For each pair of adjacent hops, if at least one hop has a unique match
        (exactly 1 candidate), record the adjacency between that known node
        and all candidates of the neighboring hop.

        Args:
            hops: List of hop dicts from decode_route(), each with keys
                  'hash', 'name', 'candidates', 'candidate_names'.
        """
        if len(hops) < 2:
            return

        now = datetime.now().isoformat()
        new_observations: list[tuple[str, str, str, str]] = []

        for i in range(len(hops) - 1):
            left = hops[i]
            right = hops[i + 1]
            # A hop counts as "resolved" if uniquely matched OR
            # confidently resolved by geo/adjacency scoring (>= 0.7)
            left_resolved = (
                left['candidates'] == 1
                or (left.get('confidence', 0) >= 0.7 and left.get('name'))
            )
            right_resolved = (
                right['candidates'] == 1
                or (right.get('confidence', 0) >= 0.7 and right.get('name'))
            )

            if not left_resolved and not right_resolved:
                continue

            # If left is resolved, record adjacency for all right candidates
            if left_resolved and right.get('candidate_names'):
                known_name = (left['candidate_names'][0]
                              if left['candidates'] == 1
                              else left['name'])
                for candidate in right['candidate_names']:
                    new_observations.append(
                        (right['hash'], left['hash'], candidate, now)
                    )
                    new_observations.append(
                        (left['hash'], right['hash'], known_name, now)
                    )

            # If right is resolved, record adjacency for all left candidates
            if right_resolved and left.get('candidate_names'):
                known_name = (right['candidate_names'][0]
                              if right['candidates'] == 1
                              else right['name'])
                for candidate in left['candidate_names']:
                    new_observations.append(
                        (left['hash'], right['hash'], candidate, now)
                    )
                    new_observations.append(
                        (right['hash'], left['hash'], known_name, now)
                    )

        if not new_observations:
            return

        with self._lock:
            # Update in-memory cache + secondary index
            for node_hash, neighbor_hash, candidate, _ts in new_observations:
                key = (node_hash, neighbor_hash)
                if key not in self._cache:
                    self._cache[key] = {}
                self._cache[key][candidate] = self._cache[key].get(candidate, 0) + 1
                self._hash_neighbors.setdefault(node_hash, set()).add(neighbor_hash)

                # Incrementally update materialized edge counts
                edge_key = tuple(sorted([node_hash, neighbor_hash]))
                self._graph_edge_counts[edge_key] = self._graph_edge_counts.get(edge_key, 0) + 1
                self._graph_node_hashes.add(node_hash)
                self._graph_node_hashes.add(neighbor_hash)

            self._graph_generation += 1

            # Buffer for batch DB write
            self._pending.extend(new_observations)

            if len(self._pending) >= self._FLUSH_THRESHOLD:
                self._flush_pending()

    def _flush_pending(self) -> None:
        """Flush buffered observations to SQLite.  Caller must hold self._lock."""
        if not self._pending:
            return
        batch = list(self._pending)
        self._pending.clear()
        # Release lock before DB I/O by doing the write outside
        # Actually we need to keep it simple and just call the DB
        # (the DB handler has its own lock)
        self._db.batch_upsert_adjacency(batch)

    def flush(self) -> None:
        """Force-flush any pending observations to SQLite.  Thread-safe."""
        with self._lock:
            self._flush_pending()

    def resolve_ambiguous_hop(
        self,
        hop_hash: str,
        neighbor_hashes: list[str],
        candidates: list[str],
    ) -> list[tuple[str, float]]:
        """Rank candidates for an ambiguous hop based on adjacency history.

        Args:
            hop_hash: The 1-byte hex hash of the ambiguous hop.
            neighbor_hashes: Hashes of the left and/or right neighbors in the route.
            candidates: List of candidate node names for this hash.

        Returns:
            List of (candidate_name, score) sorted by score descending.
            Score is the sum of adjacency counts across all neighbor matches.
        """
        if not candidates or not neighbor_hashes:
            return [(c, 0.0) for c in candidates]

        scores: dict[str, float] = {c: 0.0 for c in candidates}

        with self._lock:
            for nh in neighbor_hashes:
                key = (hop_hash, nh)
                adj = self._cache.get(key, {})
                for candidate in candidates:
                    if candidate in adj:
                        scores[candidate] += adj[candidate]

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return ranked

    def compute_confidence(
        self,
        num_candidates: int,
        ranked_scores: list[tuple[str, float]],
    ) -> float:
        """Compute a confidence score (0.0-1.0) for a hop resolution.

        Args:
            num_candidates: Total number of candidate nodes for this hash.
            ranked_scores: Output from resolve_ambiguous_hop().

        Returns:
            Confidence between 0.0 and 1.0.
        """
        if num_candidates == 0:
            return 0.0
        if num_candidates == 1:
            return 1.0

        if not ranked_scores:
            return 0.3

        top_score = ranked_scores[0][1]
        if top_score == 0:
            # No adjacency data at all
            return 0.3

        # If there's a clear winner, confidence is higher
        if len(ranked_scores) >= 2:
            second_score = ranked_scores[1][1]
            if second_score == 0:
                # Only one candidate has any evidence
                # Confidence grows with observation count but caps at 0.95
                return min(0.95, 0.7 + 0.05 * min(top_score, 5))
            else:
                # Both have evidence — confidence based on how dominant the leader is
                ratio = top_score / (top_score + second_score)
                # ratio of 1.0 -> conf 0.9, ratio of 0.5 -> conf 0.5
                return max(0.3, min(0.9, ratio))
        else:
            # Only one candidate
            return min(0.95, 0.7 + 0.05 * min(top_score, 5))

    @property
    def total_observations(self) -> int:
        """Total adjacency observations in the cache."""
        with self._lock:
            return sum(
                sum(candidates.values())
                for candidates in self._cache.values()
            )

    # ── Geo-enhanced resolution ────────────────────────────────

    @property
    def geo_resolver(self) -> GeoResolver:
        return self._geo

    def resolve_ambiguous_hop_geo(
        self,
        hop_hash: str,
        neighbor_hashes: list[str],
        candidates: list[str],
        resolved_neighbors: list[str],
        all_hops: list[dict] | None = None,
        backends: list | None = None,
        db_handler=None,
    ) -> tuple[list[tuple[str, float]], float]:
        """Resolve with adjacency first, geographic fallback for ties.

        Returns (ranked_list, confidence) where ranked_list is
        [(name, score), ...] sorted by score descending.
        """
        # Step 1: adjacency scoring
        adj_ranked = self.resolve_ambiguous_hop(hop_hash, neighbor_hashes, candidates)
        adj_conf = self.compute_confidence(len(candidates), adj_ranked)

        if adj_conf >= 0.7:
            return adj_ranked, adj_conf

        # Step 2: check if adjacency is tied / inconclusive
        tied = (len(adj_ranked) >= 2 and adj_ranked[0][1] == adj_ranked[1][1])
        if adj_conf >= 0.7 and not tied:
            return adj_ranked, adj_conf

        # Step 3: geographic fallback
        if backends and db_handler:
            self._geo._ensure_fresh(backends, db_handler)

        if self._geo.coord_count == 0:
            return adj_ranked, adj_conf

        geo_results = self._geo.score_candidates(
            candidates, resolved_neighbors, all_hops
        )

        if not geo_results or geo_results[0][2] == 0.0:
            return adj_ranked, adj_conf

        # Step 4: geo winner beats tied adjacency
        geo_best = geo_results[0][0]
        geo_conf = geo_results[0][2]

        if geo_conf > adj_conf:
            max_adj = max((s for _, s in adj_ranked), default=1.0)
            final = [(geo_best, max_adj + 1.0)]
            for name, score in adj_ranked:
                if name != geo_best:
                    final.append((name, score))
            return final, geo_conf

        return adj_ranked, adj_conf


# ══════════════════════════════════════════════════════════════════
# MeshtasticTool — legacy monolith kept working for backward compat
# ══════════════════════════════════════════════════════════════════

class MeshtasticTool:
    """A tool for interacting with Meshtastic devices over TCP or USB.

    This class is the original monolith from v2.x.  It now delegates
    Meshtastic-specific work to ``MeshtasticBackend`` internally while
    preserving the exact same public API so that existing code (wsgi.py,
    CLI dispatchers, etc.) continues to work unchanged.

    v3.2.0: Uses ``self.backends: list[MeshBackend]`` for multi-device.
    """

    def __init__(self, device_ip=None, serial_port=None, connection_type=None,
                 sender_filter=None, config_file=DEFAULT_CONFIG_FILE,
                 web_enabled=False, verbose=False):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Load connection type (tcp or usb)
        self.connection_type = (
            connection_type
            or os.getenv('MESHTASTIC_CONNECTION_TYPE')
            or self.config.get('Device', 'connection_type', fallback='tcp')
        )

        # Load configurations with environment variable support
        self.device_ip = (
            device_ip
            or os.getenv('MESHTASTIC_DEVICE_IP')
            or self.config.get('Device', 'ip', fallback='127.0.0.1')
        )
        self.serial_port = (
            serial_port
            or os.getenv('MESHTASTIC_SERIAL_PORT')
            or self.config.get('Device', 'serial_port', fallback=None)
        )
        if self.serial_port == '':
            self.serial_port = None
        self.sender_filter = (
            sender_filter
            or os.getenv('MESHTASTIC_SENDER_FILTER')
            or self.config.get('Filter', 'sender', fallback=None)
        )
        self.web_enabled = (
            web_enabled
            or os.getenv('MESHTASTIC_WEB_ENABLED', 'False').lower() == 'true'
        )
        self.verbose = verbose

        # Load web server config
        self.web_host = self.config.get('Web', 'host', fallback='127.0.0.1')
        self.web_port = self.config.getint('Web', 'port', fallback=5055)

        # Load database config
        self.max_packets_memory = self.config.getint('Database', 'max_packets_memory', fallback=1000)

        # Privacy config
        self.hide_dm_from_feed = self.config.getboolean('Privacy', 'hide_dm_from_feed', fallback=False)

        # Shared state
        self.latest_packets = []
        self.latest_packets_lock = threading.Lock()
        self.db_handler = DatabaseHandler()
        self.route_analyzer = RouteAnalyzer(self.db_handler)
        self._nodeinfo_cache = None   # (timestamp, rows) tuple for decode_route
        self._nodeinfo_cache_ttl = 10  # seconds
        self._nodeinfo_parsed_cache = None  # (timestamp, [{key, name}]) parsed NODEINFO entries
        self._nodeinfo_evidence_cache = None  # (timestamp, {full_key: {count, min_path_len}})
        self._decode_route_cache: dict[tuple, list[dict]] = {}
        self._decode_route_gen: int = -1  # last generation seen
        self._decode_route_lookup: tuple | None = None  # (hash_size, lookup, has_coords) for current gen
        self._sse_subscribers: list[queue.Queue] = []
        self._sse_lock = threading.Lock()
        self.traceroute_completed = False
        self.is_traceroute_mode = False
        self.traceroute_results = {}
        self.traceroute_results_lock = threading.Lock()
        self.server_start_time = datetime.now()
        self.connection_start_time = None

        # ── Multi-device backend list (v3.2.0) ──────────────────
        self.backends: list[MeshBackend] = []

        # Determine backend mode (meshtastic, meshcore, or dual)
        self.backend_mode = (
            os.getenv('MESHCONSOLE_BACKEND_MODE')
            or self.config.get('Backend', 'mode', fallback='meshtastic')
        )

        # Check for multi-device config
        device_configs = self._get_device_configs()

        if device_configs:
            # Multi-device config found — create backends from it
            self._create_backends_from_configs(device_configs)
        else:
            # Legacy single/dual device init
            # Create the Meshtastic backend only if needed
            if self.backend_mode in ('meshtastic', 'dual'):
                try:
                    from meshconsole.backend.meshtastic import MeshtasticBackend
                    backend = MeshtasticBackend(
                        device_ip=self.device_ip,
                        serial_port=self.serial_port,
                        connection_type=self.connection_type,
                        sender_filter=self.sender_filter,
                        db_handler=self.db_handler,
                        verbose=self.verbose,
                    )
                    backend.on_packet_received(self._make_packet_handler(backend))
                    self.backends.append(backend)
                except ImportError:
                    if self.backend_mode == 'meshtastic':
                        raise MeshtasticToolError(
                            "meshtastic package required. Install with: pip install meshconsole[meshtastic]"
                        )
                    logger.warning("Meshtastic unavailable; continuing with MeshCore only.")

        logger.info(f"MeshConsole initialized (backend mode: {self.backend_mode}, {len(self.backends)} backend(s)).")

    # ── Multi-device config helpers ──────────────────────────

    def _get_device_configs(self):
        """Read multi-device configs from environment or [Devices] section.

        Returns a list of dicts, or empty list if no multi-device config.
        """
        # Check env var first (set by CLI --device args)
        env_configs = os.getenv('MESHCONSOLE_DEVICE_CONFIGS', '')
        if env_configs:
            try:
                return json.loads(env_configs)
            except (json.JSONDecodeError, TypeError):
                logger.warning("Invalid MESHCONSOLE_DEVICE_CONFIGS env var, ignoring.")

        # Check INI [Devices] section
        if not self.config.has_section('Devices'):
            return []

        count = self.config.getint('Devices', 'count', fallback=0)
        if count == 0:
            return []

        configs = []
        for i in range(count):
            section = f'Device.{i}'
            if not self.config.has_section(section):
                continue
            cfg = {
                'type': self.config.get(section, 'type', fallback='meshtastic'),
                'connection_type': self.config.get(section, 'connection_type', fallback='tcp'),
                'ip': self.config.get(section, 'ip', fallback=''),
                'serial_port': self.config.get(section, 'serial_port', fallback=''),
                'ble_address': self.config.get(section, 'ble_address', fallback=''),
                'ble_pin': self.config.get(section, 'ble_pin', fallback=''),
                'tcp_host': self.config.get(section, 'tcp_host', fallback=''),
                'tcp_port': self.config.get(section, 'tcp_port', fallback=''),
                'device_id': self.config.get(section, 'device_id', fallback=''),
            }
            configs.append(cfg)

        return configs

    def _create_backends_from_configs(self, configs):
        """Create backend instances from multi-device config dicts."""
        for cfg in configs:
            btype = cfg['type']
            conn = cfg['connection_type']
            did = cfg.get('device_id', '')

            if btype == 'meshtastic':
                try:
                    from meshconsole.backend.meshtastic import MeshtasticBackend
                    ip = cfg.get('ip', '') or self.device_ip
                    sp = cfg.get('serial_port', '') or None
                    backend = MeshtasticBackend(
                        device_ip=ip,
                        serial_port=sp,
                        connection_type=conn,
                        sender_filter=self.sender_filter,
                        db_handler=self.db_handler,
                        verbose=self.verbose,
                        device_id=did,
                    )
                    backend.on_packet_received(self._make_packet_handler(backend))
                    self.backends.append(backend)
                except ImportError:
                    logger.warning("Meshtastic package unavailable, skipping device config.")
            elif btype == 'meshcore':
                try:
                    from meshconsole.backend import create_backend
                    address = ''
                    port = None
                    pin = None
                    if conn == 'ble':
                        address = cfg.get('ble_address', '')
                        pin = cfg.get('ble_pin', '') or None
                    elif conn == 'usb':
                        address = cfg.get('serial_port', '')
                    elif conn == 'tcp':
                        address = cfg.get('tcp_host', '')
                        port_str = cfg.get('tcp_port', '')
                        port = int(port_str) if port_str else 4000

                    backend = create_backend(
                        BackendType.MESHCORE,
                        connection_type=conn,
                        address=address,
                        port=port,
                        pin=pin,
                        verbose=self.verbose,
                        device_id=did,
                    )
                    backend.on_packet_received(self._make_packet_handler(backend))
                    self.backends.append(backend)
                except ImportError:
                    logger.warning("MeshCore package unavailable, skipping device config.")

    # ── Backward-compat properties: _backend / _meshcore_backend ──

    @property
    def _backend(self):
        """Return the first Meshtastic backend, or None."""
        return next((b for b in self.backends if b.backend_type == BackendType.MESHTASTIC), None)

    @_backend.setter
    def _backend(self, value):
        """Replace or remove the first Meshtastic backend (legacy compat)."""
        # Remove existing meshtastic backends
        self.backends = [b for b in self.backends if b.backend_type != BackendType.MESHTASTIC]
        if value is not None:
            self.backends.insert(0, value)

    @property
    def _meshcore_backend(self):
        """Return the first MeshCore backend, or None."""
        return next((b for b in self.backends if b.backend_type == BackendType.MESHCORE), None)

    @_meshcore_backend.setter
    def _meshcore_backend(self, value):
        """Replace or remove the first MeshCore backend (legacy compat)."""
        self.backends = [b for b in self.backends if b.backend_type != BackendType.MESHCORE]
        if value is not None:
            self.backends.append(value)

    # ── Proxy properties for backward compat ──────────────────

    @property
    def interface(self):
        mt = self._backend
        return mt.interface if mt else None

    @interface.setter
    def interface(self, value):
        mt = self._backend
        if mt:
            mt.interface = value

    @property
    def local_node_id(self):
        for b in self.backends:
            if b.local_node_id:
                return b.local_node_id
        return None

    @local_node_id.setter
    def local_node_id(self, value):
        mt = self._backend
        if mt:
            mt._local_node_id = value

    @property
    def node_name_map(self):
        """Merge node_name_map from all Meshtastic backends."""
        merged = {}
        for b in self.backends:
            if b.backend_type == BackendType.MESHTASTIC and hasattr(b, 'node_name_map'):
                merged.update(b.node_name_map)
        return merged

    @property
    def node_short_name_map(self):
        """Merge node_short_name_map from all Meshtastic backends."""
        merged = {}
        for b in self.backends:
            if b.backend_type == BackendType.MESHTASTIC and hasattr(b, 'node_short_name_map'):
                merged.update(b.node_short_name_map)
        return merged

    # ── Packet callback ───────────────────────────────────────

    def _make_packet_handler(self, backend):
        """Create a packet handler bound to a specific backend's device_id."""
        def handler(packet: UnifiedPacket):
            packet.device_id = backend.device_id
            self._handle_backend_packet(packet)
        return handler

    def _handle_backend_packet(self, packet: UnifiedPacket):
        """Handle packets produced by the backend -- log to DB and cache."""
        packet_dict = asdict(packet)
        # Ensure backend is stored as string
        if hasattr(packet_dict.get('backend'), 'value'):
            packet_dict['backend'] = packet_dict['backend'].value
        elif isinstance(packet_dict.get('backend'), BackendType):
            packet_dict['backend'] = packet_dict['backend'].value

        # Log to database
        self.db_handler.log_packet(packet_dict)

        # Also log text messages to the messages table
        if packet.port_name in ('TEXT_MESSAGE', 'TEXT_MESSAGE_APP') and packet.message:
            backend_str = packet.backend.value if isinstance(packet.backend, BackendType) else str(packet.backend)
            self.db_handler.log_message(
                timestamp=packet.timestamp,
                from_id=packet.from_id,
                to_id=packet.to_id,
                port_name=packet.port_name,
                message=packet.message,
                backend=backend_str,
            )

        # Learn route adjacency patterns from MeshCore packets with path data
        if packet.backend == BackendType.MESHCORE:
            raw = packet.raw_packet if isinstance(packet.raw_packet, dict) else {}
            path_hex = raw.get('path', '')
            if path_hex and len(path_hex) >= 4:
                hash_size = raw.get('path_hash_size', 1) or 1
                decoded_hops = self.decode_route(path_hex, hash_size)
                self.route_analyzer.learn_route(decoded_hops)

        # Handle MeshCore traceroute responses
        if packet.port_name == 'TRACEROUTE' and packet.backend == BackendType.MESHCORE:
            with self.traceroute_results_lock:
                self.traceroute_results = {
                    'success': True,
                    'destination': packet.to_id or packet.from_id,
                    'route': [{'node': packet.from_id, 'snr': packet.snr}],
                    'raw': packet.payload,
                    'backend': 'meshcore',
                }
            self.traceroute_completed = True

        # Update in-memory cache (limit scales with number of devices)
        # Optionally hide DMs from the live feed
        is_dm = (
            packet.port_name in ('TEXT_MESSAGE', 'TEXT_MESSAGE_APP')
            and packet.to_id
            and packet.to_id not in ('^all', 'broadcast', 'all')
            and not packet.to_id.startswith('channel:')
        )
        if not (is_dm and self.hide_dm_from_feed):
            with self.latest_packets_lock:
                self.latest_packets.append(packet_dict)
                effective_limit = self.max_packets_memory * max(1, len(self.backends))
                self.latest_packets = self.latest_packets[-effective_limit:]
            # Push to SSE subscribers for real-time updates
            self._publish_sse({'event': 'packet', 'port_name': packet_dict.get('port_name', '')})

    def get_local_node_ids(self) -> list[str]:
        """Return all local node IDs across all backends."""
        ids = []
        for b in self.backends:
            if b.local_node_id:
                ids.append(b.local_node_id)
        return ids

    # ── Connection ────────────────────────────────────────────

    def _connect_interface(self):
        """Connect all backends in self.backends.

        Also connects MeshCore backend if the backend mode is 'meshcore' or 'dual'
        and no MeshCore backend exists yet.
        """
        # Auto-detect if backend_mode is 'auto'
        if self.backend_mode == 'auto':
            self._auto_detect_and_connect()
            return

        # Connect all existing backends
        for b in list(self.backends):
            if b.is_connected:
                continue
            try:
                b.connect()
                if not self.connection_start_time:
                    self.connection_start_time = datetime.now()
                logger.info(f"Connected backend: {b.backend_type.value} ({b.device_id})")
            except Exception as e:
                logger.error(f"Failed to connect {b.backend_type.value} backend: {e}")
                # If this is the only meshtastic backend and mode requires it, raise
                if b.backend_type == BackendType.MESHTASTIC and self.backend_mode == 'meshtastic':
                    raise MeshtasticToolError("Connection to Meshtastic device failed.")
                elif b.backend_type == BackendType.MESHCORE and self.backend_mode == 'meshcore':
                    raise MeshtasticToolError(f"Connection to MeshCore device failed: {e}")

        # Connect MeshCore backend if mode requires it and not yet in backends
        if self.backend_mode in ('meshcore', 'dual') and not self._meshcore_backend:
            self._connect_meshcore()

    def _connect_meshcore(self):
        """Create and connect the MeshCore backend."""
        try:
            mc_conn_type = (
                os.getenv('MESHCORE_CONNECTION_TYPE')
                or self.config.get('MeshCore', 'connection_type', fallback='ble')
            )

            # Determine address based on connection type
            if mc_conn_type == 'ble':
                address = (
                    os.getenv('MESHCORE_BLE_ADDRESS')
                    or self.config.get('MeshCore', 'ble_address', fallback='')
                )
                pin = (
                    os.getenv('MESHCORE_BLE_PIN')
                    or self.config.get('MeshCore', 'ble_pin', fallback='')
                ) or None
                mc_port = None
            elif mc_conn_type == 'usb':
                address = (
                    os.getenv('MESHCORE_SERIAL_PORT')
                    or self.config.get('MeshCore', 'serial_port', fallback='')
                )
                pin = None
                mc_port = None
            elif mc_conn_type == 'tcp':
                address = (
                    os.getenv('MESHCORE_TCP_HOST')
                    or self.config.get('MeshCore', 'tcp_host', fallback='')
                )
                mc_port_str = (
                    os.getenv('MESHCORE_TCP_PORT')
                    or self.config.get('MeshCore', 'tcp_port', fallback='')
                )
                mc_port = int(mc_port_str) if mc_port_str else 4000
                pin = None
            else:
                raise MeshtasticToolError(f"Unsupported MeshCore connection type: {mc_conn_type}")

            if not address:
                raise MeshtasticToolError(
                    f"MeshCore {mc_conn_type} address not configured. "
                    f"Set it in config.ini [MeshCore] or via environment variables."
                )

            from meshconsole.backend import create_backend
            mc_backend = create_backend(
                BackendType.MESHCORE,
                connection_type=mc_conn_type,
                address=address,
                port=mc_port,
                pin=pin,
                verbose=self.verbose,
            )
            mc_backend.on_packet_received(self._make_packet_handler(mc_backend))
            mc_backend.connect()
            self.backends.append(mc_backend)
            logger.info("MeshCore backend connected successfully.")

        except ImportError as e:
            logger.error(f"MeshCore backend unavailable: {e}")
            if self.backend_mode == 'meshcore':
                raise MeshtasticToolError(str(e))
        except Exception as e:
            logger.error(f"Failed to connect MeshCore backend: {e}")
            if self.backend_mode == 'meshcore':
                raise MeshtasticToolError(f"Connection to MeshCore device failed: {e}")

    def _auto_detect_and_connect(self):
        """Auto-detect USB devices and connect to ALL of them."""
        from meshconsole.autodetect import auto_detect_devices

        devices = auto_detect_devices()
        if not devices:
            raise MeshtasticToolError("No mesh devices detected. Check USB connections.")

        for device in devices:
            if device.backend_type == BackendType.MESHTASTIC:
                try:
                    from meshconsole.backend.meshtastic import MeshtasticBackend
                    backend = MeshtasticBackend(
                        serial_port=device.port,
                        connection_type='usb',
                        db_handler=self.db_handler,
                        verbose=self.verbose,
                    )
                    backend.on_packet_received(self._make_packet_handler(backend))
                    backend.connect()
                    self.backends.append(backend)
                    if not self.connection_start_time:
                        self.connection_start_time = datetime.now()
                    logger.info(f"Auto-connected Meshtastic on {device.port}")
                except Exception as e:
                    logger.error(f"Failed to connect Meshtastic on {device.port}: {e}")

            elif device.backend_type == BackendType.MESHCORE:
                try:
                    from meshconsole.backend import create_backend
                    mc_backend = create_backend(
                        BackendType.MESHCORE,
                        connection_type='usb',
                        address=device.port,
                        verbose=self.verbose,
                    )
                    mc_backend.on_packet_received(self._make_packet_handler(mc_backend))
                    mc_backend.connect()
                    self.backends.append(mc_backend)
                    logger.info(f"Auto-connected MeshCore on {device.port}")
                except Exception as e:
                    logger.error(f"Failed to connect MeshCore on {device.port}: {e}")

        # Update backend mode based on what connected
        has_mt = any(b.backend_type == BackendType.MESHTASTIC and b.is_connected for b in self.backends)
        has_mc = any(b.backend_type == BackendType.MESHCORE and b.is_connected for b in self.backends)
        if has_mt and has_mc:
            self.backend_mode = 'dual'
        elif has_mt:
            self.backend_mode = 'meshtastic'
        elif has_mc:
            self.backend_mode = 'meshcore'
        else:
            raise MeshtasticToolError("Failed to connect to any detected devices.")

        logger.info(f"Auto-detection complete: mode={self.backend_mode}, {len(self.backends)} device(s)")

    def _sync_node_db(self):
        """Sync node database from all Meshtastic backends."""
        for b in self.backends:
            if b.backend_type == BackendType.MESHTASTIC and hasattr(b, '_sync_node_db'):
                b._sync_node_db()

    # ── Delegated methods (preserve original signatures) ──────

    def _resolve_node_name(self, node_id):
        """Resolve a node ID to a name by checking all backends."""
        if not node_id:
            return node_id

        # Route MeshCore IDs to MeshCore backends first
        if node_id.startswith('mc:'):
            for b in self.backends:
                if b.backend_type == BackendType.MESHCORE:
                    name = b.resolve_node_name(node_id)
                    if name and name != node_id and name != node_id.removeprefix('mc:'):
                        return name

        # Try all backends
        for b in self.backends:
            try:
                name = b.resolve_node_name(node_id)
                if name and name != node_id:
                    return name
            except Exception:
                continue

        return node_id

    def _get_port_name(self, portnum):
        mt = self._backend
        if mt:
            return mt._get_port_name(portnum)
        return str(portnum)

    def _json_serializer(self, obj):
        mt = self._backend
        if mt:
            return mt._json_serializer(obj)
        return str(obj)

    def on_connection(self, interface, topic=None):
        mt = self._backend
        if mt:
            mt._on_connection(interface, topic)

    def on_receive(self, packet, interface):
        mt = self._backend
        if mt:
            mt._on_receive(packet, interface)

    def process_packet(self, packet):
        mt = self._backend
        if mt:
            mt._process_packet(packet)

    def send_message(self, destination_id, message, device_id=None):
        """Send a message, optionally routing to a specific device.

        Args:
            destination_id: The target node ID.
            message: The text message to send.
            device_id: If specified, route to the backend with this device_id.
        """
        # Route by device_id if specified
        if device_id:
            for b in self.backends:
                if b.device_id == device_id:
                    b.send_message(destination_id, message)
                    return
            logger.warning(f"No backend found with device_id={device_id}")

        # Route by node ID prefix
        if destination_id.startswith('mc:'):
            for b in self.backends:
                if b.backend_type == BackendType.MESHCORE:
                    b.send_message(destination_id, message)
                    return
        else:
            for b in self.backends:
                if b.backend_type == BackendType.MESHTASTIC:
                    b.send_message(destination_id, message)
                    return

        # Fallback: send via any connected backend
        for b in self.backends:
            if b.is_connected:
                b.send_message(destination_id, message)
                return

    def send_channel_message(self, channel_idx: int, message: str, device_id: str | None = None) -> None:
        """Send a channel message via any backend that supports it."""
        for b in self.backends:
            if b.is_connected and hasattr(b, 'send_channel_message'):
                if device_id is None or b.device_id == device_id:
                    b.send_channel_message(channel_idx, message)
                    return
        raise ConnectionError("No connected device with channel support")

    def get_channels(self, device_id: str | None = None) -> list[dict]:
        """Get channels from a specific device."""
        for b in self.backends:
            if b.is_connected and hasattr(b, 'get_channels'):
                if device_id is None or b.device_id == device_id:
                    channels = b.get_channels()
                    return [{'index': ch['index'], 'name': ch['name'], 'device_id': b.device_id} for ch in channels]
        return []

    def set_channel(self, channel_idx: int, channel_name: str, device_id: str | None = None) -> bool:
        """Set a channel name on a MeshCore device."""
        for b in self.backends:
            if b.is_connected and hasattr(b, 'set_channel'):
                if device_id is None or b.device_id == device_id:
                    return b.set_channel(channel_idx, channel_name)
        raise ConnectionError("No connected MeshCore device")

    def send_traceroute(self, destination_id, hop_limit=10):
        """Route traceroute to the appropriate backend."""
        if destination_id.startswith('mc:'):
            for b in self.backends:
                if b.backend_type == BackendType.MESHCORE:
                    b.send_traceroute(destination_id, hop_limit)
                    return
        else:
            for b in self.backends:
                if b.backend_type == BackendType.MESHTASTIC:
                    b.send_traceroute(destination_id, hop_limit)
                    return

        # Fallback: any connected backend
        for b in self.backends:
            if b.is_connected:
                b.send_traceroute(destination_id, hop_limit)
                return

    def _format_node_id(self, node_num):
        mt = self._backend
        if mt:
            return mt._format_node_id(node_num)
        return str(node_num)

    def _get_node_id(self, packet, field='from'):
        mt = self._backend
        if mt:
            return mt._get_node_id(packet, field)
        return str(packet.get(field, ''))

    def _update_node_from_packet(self, packet):
        mt = self._backend
        if mt:
            mt._update_node_from_packet(packet)

    def _process_traceroute_response(self, packet):
        mt = self._backend
        if mt:
            mt._process_traceroute_response(packet)

    def _print_message_summary(self, packet):
        mt = self._backend
        if mt:
            mt._print_message_summary(packet)

    # ── Listening / reconnection ──────────────────────────────

    def start_listening(self):
        """Start listening for messages with robust reconnection logic."""

        def stop_listening(signum, frame):
            print("\nScript terminated by user.")
            logger.info("Script terminated.")
            self.cleanup()
            sys.exit(0)

        signal.signal(signal.SIGINT, stop_listening)
        signal.signal(signal.SIGTERM, stop_listening)

        if self.web_enabled:
            web_thread = threading.Thread(target=self.start_web_server)
            web_thread.daemon = True
            web_thread.start()

        logger.info("Started listening for messages. Press Ctrl+C to exit.")
        retry_delay = 1
        max_retry_delay = 30
        last_packet_time = time.time()
        connection_timeout = 60

        # Per-backend retry tracking: {device_id: {'failures': N, 'next_retry': timestamp}}
        _backend_retry_state: dict[str, dict] = {}
        _MAX_CONSECUTIVE_FAILURES = 10  # Suspend backend after this many consecutive failures

        while True:
            try:
                while True:
                    time.sleep(5)
                    current_time = time.time()

                    # ── Health check each backend independently ──
                    failed_backends = []
                    for b in list(self.backends):
                        try:
                            if b.backend_type == BackendType.MESHTASTIC:
                                iface = getattr(b, 'interface', None)
                                if not iface:
                                    failed_backends.append(b)
                                    continue

                                healthy = True

                                # Check 1: meshtastic library's own connected flag
                                if hasattr(iface, 'isConnected') and not iface.isConnected:
                                    healthy = False

                                # Check 2: TCP socket error check
                                if hasattr(iface, 'socket') and iface.socket:
                                    try:
                                        error = iface.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                                        if error != 0:
                                            healthy = False
                                    except (BrokenPipeError, OSError):
                                        healthy = False

                                # Check 3: reader thread must be alive (detects
                                # "Connection reset by peer" kills)
                                if healthy and hasattr(iface, '_readThread'):
                                    rt = iface._readThread
                                    if rt is not None and not rt.is_alive():
                                        logger.warning(
                                            f"Meshtastic reader thread dead for {b.device_id}"
                                        )
                                        healthy = False

                                # Check 4: heartbeat timer should exist and not
                                # have been cancelled/crashed
                                if healthy and hasattr(iface, 'heartbeatTimer'):
                                    ht = iface.heartbeatTimer
                                    if ht is not None and not ht.is_alive():
                                        logger.warning(
                                            f"Meshtastic heartbeat timer dead for {b.device_id}"
                                        )
                                        healthy = False

                                # Check 5: probe iface.nodes for broken pipe
                                try:
                                    if hasattr(iface, 'nodes'):
                                        _ = len(iface.nodes)
                                except (BrokenPipeError, OSError, Exception):
                                    healthy = False

                                if not healthy:
                                    failed_backends.append(b)

                            elif b.backend_type == BackendType.MESHCORE:
                                # Grace period: skip health check for 120s after
                                # connect to allow bootstrap config + contacts load
                                connect_age = time.time() - getattr(b, '_connect_time', 0)
                                if not b.is_connected and connect_age > 120:
                                    failed_backends.append(b)
                        except (BrokenPipeError, OSError) as health_err:
                            logger.debug(f"Health check error for {b.device_id}: {health_err}")
                            failed_backends.append(b)

                    if failed_backends:
                        # Reconnect failed backends without disrupting healthy ones
                        names = [b.device_id for b in failed_backends]
                        logger.error(f"Connection lost: Backend(s) failed health check: {', '.join(names)}")
                        current_time = time.time()
                        for b in failed_backends:
                            did = b.device_id
                            state = _backend_retry_state.get(did, {'failures': 0, 'next_retry': 0.0})
                            if current_time < state.get('next_retry', 0.0):
                                continue
                            if state['failures'] >= _MAX_CONSECUTIVE_FAILURES:
                                if current_time < state.get('next_retry', 0.0):
                                    continue
                                logger.info(f"Retrying suspended backend {did}...")
                            try:
                                logger.info(f"Reconnecting {did}...")
                                b.reconnect()
                                if b.backend_type == BackendType.MESHTASTIC:
                                    b._sync_node_db()
                                logger.info(f"Reconnected {did}")
                                _backend_retry_state.pop(did, None)
                            except Exception as re_err:
                                state['failures'] = state.get('failures', 0) + 1
                                delay = min(120, 2 ** state['failures'])
                                state['next_retry'] = current_time + delay
                                _backend_retry_state[did] = state
                                logger.error(f"Reconnection failed for {did}: {re_err}")

                    # ── Track packet activity ──
                    with self.latest_packets_lock:
                        if self.latest_packets:
                            latest_timestamp = self.latest_packets[-1].get('timestamp', '')
                            if latest_timestamp:
                                try:
                                    packet_time = datetime.fromisoformat(latest_timestamp).timestamp()
                                    if packet_time > last_packet_time:
                                        last_packet_time = packet_time
                                except Exception:
                                    pass

            except (ConnectionError, BrokenPipeError, OSError, socket.error, Exception) as e:
                logger.error(f"Connection lost: {e}")

                time.sleep(2)

                # ── Reconnect only failed backends, leave healthy ones alone ──
                reconnected_any = False
                for b in list(self.backends):
                    did = b.device_id

                    # Check if this backend needs reconnection
                    needs_reconnect = False
                    if b.backend_type == BackendType.MESHTASTIC:
                        iface = getattr(b, 'interface', None)
                        if not iface or not b.is_connected:
                            needs_reconnect = True
                        elif hasattr(iface, 'isConnected') and not iface.isConnected:
                            needs_reconnect = True
                        elif hasattr(iface, '_readThread'):
                            rt = iface._readThread
                            if rt is not None and not rt.is_alive():
                                needs_reconnect = True
                    elif not b.is_connected:
                        # MeshCore: require 6 consecutive failures (30s at 5s intervals)
                        # before reconnecting. Rapid reconnects corrupt device SPIFFS.
                        mc_fails = _backend_retry_state.get(did, {}).get('mc_consecutive', 0) + 1
                        if mc_fails >= 6:
                            needs_reconnect = True
                        else:
                            state = _backend_retry_state.get(did, {'failures': 0, 'next_retry': 0.0})
                            state['mc_consecutive'] = mc_fails
                            _backend_retry_state[did] = state

                    if not needs_reconnect:
                        # Backend is healthy — reset its retry state
                        _backend_retry_state.pop(did, None)
                        continue

                    # Check per-backend retry state
                    state = _backend_retry_state.get(did, {'failures': 0, 'next_retry': 0.0})

                    # Skip if suspended (too many consecutive failures)
                    if state['failures'] >= _MAX_CONSECUTIVE_FAILURES:
                        # Only retry suspended backends every 5 minutes
                        if current_time < state.get('next_retry', 0.0):
                            continue
                        logger.info(f"Retrying suspended backend {did}...")

                    # Skip if not yet time for next retry
                    if current_time < state.get('next_retry', 0.0):
                        continue

                    try:
                        logger.info(f"Reconnecting {did}...")
                        b.reconnect()
                        if b.backend_type == BackendType.MESHTASTIC:
                            b._sync_node_db()
                        reconnected_any = True
                        logger.info(f"Reconnected {did}")
                        _backend_retry_state.pop(did, None)
                    except Exception as re:
                        logger.error(f"Reconnection failed for {did}: {re}")
                        state['failures'] = state.get('failures', 0) + 1
                        # Exponential backoff per backend: 2, 4, 8, ... 30s, then 300s if suspended
                        if state['failures'] >= _MAX_CONSECUTIVE_FAILURES:
                            backoff = 300  # 5 minutes for suspended backends
                            if state['failures'] == _MAX_CONSECUTIVE_FAILURES:
                                logger.warning(
                                    f"Backend {did} suspended after {state['failures']} "
                                    f"consecutive failures (will retry every 5 min)"
                                )
                        else:
                            backoff = min(2 ** state['failures'], max_retry_delay)
                        state['next_retry'] = current_time + backoff
                        _backend_retry_state[did] = state

                if reconnected_any:
                    retry_delay = 1
                    last_packet_time = time.time()
                else:
                    retry_delay = min(retry_delay * 2, max_retry_delay)
                continue

    def _load_recent_packets_from_db(self):
        """Load recent packets from database into memory for web interface."""
        try:
            with self.latest_packets_lock:
                with self.db_handler.lock:
                    self.db_handler.cursor.execute(
                        f'SELECT * FROM packets ORDER BY timestamp DESC LIMIT {self.max_packets_memory}'
                    )
                    db_packets = self.db_handler.cursor.fetchall()

                for packet_row in reversed(db_packets):
                    try:
                        packet_data = {
                            'timestamp': packet_row[0],
                            'from_id': packet_row[1],
                            'to_id': packet_row[2],
                            'from_name': self._resolve_node_name(packet_row[1]),
                            'to_name': self._resolve_node_name(packet_row[2]),
                            'port_name': packet_row[3],
                            'payload': packet_row[4],
                            'message': '',
                            'latitude': None,
                            'longitude': None,
                            'altitude': None,
                            'position_time': None,
                            'hop_limit': None,
                            'priority': None,
                            'rssi': 'Unknown',
                            'snr': 'Unknown',
                            'battery_level': None,
                            'voltage': None,
                            'channel_util': None,
                            'air_util_tx': None,
                            'uptime_hours': None,
                            'uptime_minutes': None,
                            'raw_packet': json.loads(packet_row[5]),
                            'backend': packet_row[6] if len(packet_row) > 6 else 'meshtastic',
                            'device_id': packet_row[7] if len(packet_row) > 7 else '',
                        }

                        raw_packet = packet_data['raw_packet']
                        pkt_backend = packet_data.get('backend', 'meshtastic')

                        if pkt_backend == 'meshcore':
                            # MeshCore raw_packets store data at the top level
                            packet_data['message'] = raw_packet.get('text', '')
                            packet_data['snr'] = raw_packet.get('snr') if raw_packet.get('snr') is not None else 'N/A'
                            packet_data['rssi'] = raw_packet.get('rssi') if raw_packet.get('rssi') is not None else 'N/A'
                            packet_data['hop_limit'] = raw_packet.get('path_len')

                            # MeshCore NODEINFO / advertisement coordinates
                            lat = raw_packet.get('adv_lat') or raw_packet.get('latitude')
                            lon = raw_packet.get('adv_lon') or raw_packet.get('longitude')
                            if lat and lon:
                                packet_data['latitude'] = lat
                                packet_data['longitude'] = lon

                            # MeshCore telemetry: voltage_mv at top level
                            voltage_mv = raw_packet.get('voltage_mv', 0)
                            if voltage_mv:
                                packet_data['voltage'] = voltage_mv / 1000.0
                        else:
                            # Meshtastic raw_packets use decoded sub-structure
                            decoded = raw_packet.get('decoded', {})

                            packet_data['message'] = decoded.get('text', '')

                            position = decoded.get('position', {})
                            if position:
                                packet_data['latitude'] = position.get('latitude')
                                packet_data['longitude'] = position.get('longitude')
                                packet_data['altitude'] = position.get('altitude')
                                packet_data['position_time'] = position.get('time')

                            telemetry = decoded.get('telemetry', {})
                            if telemetry:
                                device_metrics = telemetry.get('deviceMetrics', {})
                                packet_data['battery_level'] = device_metrics.get('batteryLevel')
                                packet_data['voltage'] = device_metrics.get('voltage')
                                packet_data['channel_util'] = device_metrics.get('channelUtilization')
                                packet_data['air_util_tx'] = device_metrics.get('airUtilTx')

                                uptime_seconds = device_metrics.get('uptimeSeconds')
                                if uptime_seconds is not None:
                                    packet_data['uptime_hours'] = uptime_seconds // 3600
                                    packet_data['uptime_minutes'] = (uptime_seconds % 3600) // 60

                            packet_data['hop_limit'] = raw_packet.get('hopLimit')
                            packet_data['priority'] = raw_packet.get('priority')
                            packet_data['rssi'] = raw_packet.get('rxRssi', 'Unknown')
                            packet_data['snr'] = raw_packet.get('rxSnr', 'Unknown')

                        self.latest_packets.append(packet_data)

                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Failed to parse packet from database: {e}")
                        continue

                self.latest_packets = self.latest_packets[-self.max_packets_memory:]
                logger.info(f"Loaded {len(self.latest_packets)} packets from database for web interface")

        except Exception as e:
            logger.error(f"Failed to load packets from database: {e}")

    def start_web_server(self):
        """Start the Flask web server."""
        self._load_recent_packets_from_db()

        from meshconsole.web import create_app
        app = create_app(self)

        logger.info(f"Starting web server at http://{self.web_host}:{self.web_port}")
        app.run(host=self.web_host, port=self.web_port, debug=False, use_reloader=False)

    # ── SSE (Server-Sent Events) ────────────────────────────

    def subscribe_sse(self) -> queue.Queue:
        """Create a new SSE subscriber queue."""
        q = queue.Queue(maxsize=50)
        with self._sse_lock:
            self._sse_subscribers.append(q)
        return q

    def unsubscribe_sse(self, q: queue.Queue) -> None:
        """Remove an SSE subscriber queue."""
        with self._sse_lock:
            try:
                self._sse_subscribers.remove(q)
            except ValueError:
                pass

    def _publish_sse(self, packet_dict: dict) -> None:
        """Push a packet to all SSE subscribers."""
        with self._sse_lock:
            dead = []
            for q in self._sse_subscribers:
                try:
                    q.put_nowait(packet_dict)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                self._sse_subscribers.remove(q)

    # ── Device telemetry ─────────────────────────────────────

    def get_device_telemetry(self) -> list[dict]:
        """Return per-device stats for the enhanced Stats page."""
        devices = []
        for b in self.backends:
            # Resolve device name: MeshCore uses _device_name, Meshtastic uses node_name_map
            dev_name = getattr(b, '_device_name', '')
            if not dev_name and b.local_node_id and hasattr(b, 'node_name_map'):
                dev_name = b.node_name_map.get(b.local_node_id, '')
            entry = {
                'device_id': b.device_id,
                'name': dev_name or b.device_id,
                'type': b.backend_type.value,
                'connected': b.is_connected,
                'local_node_id': b.local_node_id,
                'stats': {},
            }
            try:
                if hasattr(b, 'get_device_stats'):
                    entry['stats'] = b.get_device_stats()
            except Exception as e:
                logger.debug(f"Error getting stats for {b.device_id}: {e}")
            devices.append(entry)

        # Per-device packet counts from DB
        try:
            with self.db_handler.lock:
                self.db_handler.cursor.execute('''
                    SELECT device_id, COUNT(*) FROM packets
                    WHERE timestamp >= datetime('now', '-24 hours')
                    GROUP BY device_id
                ''')
                counts = {row[0]: row[1] for row in self.db_handler.cursor.fetchall()}
            for d in devices:
                d['packet_count_24h'] = counts.get(d['device_id'], 0)
        except Exception:
            pass

        return devices

    # ── Orchestrator-like helpers used by web.py ──────────────

    def resolve_node_name(self, node_id):
        """Public resolve used by web routes."""
        return self._resolve_node_name(node_id)

    def resolve_node_names_bulk(self, node_ids):
        """Resolve multiple node IDs at once, returning a {id: name} dict.

        Avoids repeated backend iteration by building a single lookup map.
        """
        result = {}
        unique_ids = set(node_ids)
        if not unique_ids:
            return result

        # Build combined name map from all backends once
        name_map = {}
        for b in self.backends:
            if b.backend_type == BackendType.MESHCORE:
                for prefix, contact in getattr(b, '_contacts', {}).items():
                    mc_id = f"mc:{prefix}"
                    name = contact.get('adv_name', '') or prefix
                    if name and name != mc_id:
                        name_map[mc_id] = name
            elif b.backend_type == BackendType.MESHTASTIC:
                if hasattr(b, 'node_name_map'):
                    for nid, name in b.node_name_map.items():
                        if name and name != nid:
                            name_map[nid] = name

        for nid in unique_ids:
            if nid in name_map:
                result[nid] = name_map[nid]
            else:
                # Fallback to DB lookup only for IDs not in live caches
                result[nid] = self.db_handler.lookup_node_name(nid)

        return result

    @property
    def is_connected(self):
        return any(b.is_connected for b in self.backends)

    def get_backend_status(self):
        """Return per-backend connection status as a list (v3.2.0).

        Also returns a dict for backward compatibility with existing web UI code
        that accesses data.backends.meshtastic / data.backends.meshcore.
        """
        status_list = []
        status_dict = {}

        for b in self.backends:
            entry = {
                'device_id': b.device_id,
                'type': b.backend_type.value,
                'connected': b.is_connected,
                'local_node_id': b.local_node_id,
            }
            # Include device name
            if b.backend_type == BackendType.MESHTASTIC:
                if hasattr(b, 'node_name_map') and b.local_node_id and b.node_name_map:
                    device_name = b.node_name_map.get(b.local_node_id)
                    if device_name:
                        entry['device_name'] = device_name
            elif b.backend_type == BackendType.MESHCORE:
                device_name = getattr(b, '_device_name', None)
                if device_name:
                    entry['device_name'] = device_name

            status_list.append(entry)

            # Also populate the legacy dict (keyed by type)
            # If multiple of same type, use device_id as key
            type_key = b.backend_type.value
            if type_key in status_dict:
                # Multiple devices of same type — use device_id as key
                status_dict[b.device_id] = entry
            else:
                status_dict[type_key] = entry

        return status_dict

    def get_backend_status_list(self):
        """Return per-backend connection status as a flat list."""
        status_list = []
        for b in self.backends:
            entry = {
                'device_id': b.device_id,
                'type': b.backend_type.value,
                'connected': b.is_connected,
                'local_node_id': b.local_node_id,
            }
            if b.backend_type == BackendType.MESHTASTIC:
                if hasattr(b, 'node_name_map') and b.local_node_id and b.node_name_map:
                    device_name = b.node_name_map.get(b.local_node_id)
                    if device_name:
                        entry['device_name'] = device_name
            elif b.backend_type == BackendType.MESHCORE:
                device_name = getattr(b, '_device_name', None)
                if device_name:
                    entry['device_name'] = device_name
            status_list.append(entry)
        return status_list

    def _get_nodeinfo_entries(self):
        """Return cached, pre-parsed NODEINFO entries.

        Each entry is ``{full_key, name, path_len, has_gps}``. ``path_len`` is
        from the latest received NODEINFO (None if not present); ``has_gps``
        flags whether the latest advert carried valid (non-0,0) coords.

        Caches both the raw DB query (avoiding repeated SELECTs) and the
        parsed JSON (avoiding repeated json.loads on every call).
        """
        now = time.time()
        if self._nodeinfo_parsed_cache and (now - self._nodeinfo_parsed_cache[0]) < self._nodeinfo_cache_ttl:
            return self._nodeinfo_parsed_cache[1]

        # Get raw rows (may also refresh _nodeinfo_cache).
        # Per from_id, only the latest NODEINFO is meaningful — older receptions
        # contain stale name/key data and their raw_packet bytes vary per
        # reception (rxRssi/rxSnr/etc), so SELECT DISTINCT on raw_packet was a
        # full scan that deduped nothing. GROUP BY from_id with MAX(timestamp)
        # uses the (port_name, from_id, timestamp) index and SQLite's
        # documented bare-column-with-MAX rule to pick the newest row per node.
        if self._nodeinfo_cache and (now - self._nodeinfo_cache[0]) < self._nodeinfo_cache_ttl:
            db_rows = self._nodeinfo_cache[1]
        else:
            try:
                with self.db_handler.lock:
                    self.db_handler.cursor.execute(
                        "SELECT raw_packet, MAX(timestamp) FROM packets "
                        "WHERE port_name IN ('NODEINFO','NODEINFO_APP') "
                        "AND backend='meshcore' GROUP BY from_id"
                    )
                    db_rows = [(row[0],) for row in self.db_handler.cursor.fetchall()]
                self._nodeinfo_cache = (now, db_rows)
            except Exception:
                db_rows = []

        entries = []
        for (raw_json,) in db_rows:
            try:
                raw = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                full_key = raw.get('public_key', '') or raw.get('adv_key', '')
                name = raw.get('adv_name', '')
                if full_key and name:
                    lat = raw.get('adv_lat')
                    lon = raw.get('adv_lon')
                    has_gps = (
                        lat is not None and lon is not None
                        and abs(float(lat)) > 0.01 and abs(float(lon)) > 0.01
                    )
                    entries.append({
                        'full_key': full_key,
                        'name': name,
                        'path_len': raw.get('path_len'),
                        'has_gps': has_gps,
                    })
            except (json.JSONDecodeError, TypeError, ValueError):
                pass

        self._nodeinfo_parsed_cache = (now, entries)
        return entries

    # Eligibility thresholds for hop-resolution candidates.
    # A candidate enters the lookup only if at least one of these holds —
    # otherwise it's treated as a flood-only stranger that would poison the
    # hash bucket. See _build_route_lookup() for the rationale.
    _HOP_MIN_NODEINFO_COUNT = 3
    _HOP_MAX_PATH_LEN = 4
    _HOP_GEO_RANGE_KM = 150

    def _build_route_lookup(self, hash_size: int) -> tuple[dict, tuple]:
        """Return (lookup, local_coords_list) for ``decode_route``.

        ``lookup`` maps a hash byte (or hash_size bytes hex) to candidate node
        names eligible to be a real hop in our local mesh.

        Eligibility uses GPS as authoritative when valid, and falls back to
        reception evidence when GPS is missing:

          • Valid GPS + within HOP_GEO_RANGE_KM of any local node → accept.
          • Valid GPS + outside the range → reject. Reception count cannot
            override geography: a node 250 km away that sends many adverts
            still racks up high count via flooding, but it is not a real
            hop in our local mesh.
          • Valid GPS + no local-node reference → charitably accept.
          • No GPS (or bogus 0,0) → use path_len ≤ HOP_MAX_PATH_LEN or
            count ≥ HOP_MIN_NODEINFO_COUNT as a locality proxy.
          • No GPS + no other evidence → reject (flood-only stranger).

        This stops both single-flood strangers (andypager: 1 advert,
        16-hop path, bogus GPS) and high-volume distant flooders (BAT11:
        18 adverts but 250 km away with valid GPS) from poisoning hash
        buckets in route trails.
        """
        local_coords_list: list[tuple] = []
        for b in self.backends:
            if b.backend_type != BackendType.MESHCORE:
                continue
            dn = getattr(b, '_device_name', None)
            if dn:
                c = self.route_analyzer._geo.get_coords(dn)
                if c:
                    local_coords_list.append(c)
        haversine = self.route_analyzer._geo._haversine

        evidence = self._get_nodeinfo_evidence()

        def _eligible(full_key_lower: str, name: str, in_local_contacts: bool) -> bool:
            ev = evidence.get(full_key_lower, {})
            count = ev.get('count', 0)
            path_len = ev.get('path_len')
            has_gps = ev.get('has_gps', False)

            # GPS is authoritative when available — a far-away node can rack
            # up high reception count via flooding but is still far away.
            if has_gps:
                c = self.route_analyzer._geo.get_coords(name)
                if c and local_coords_list:
                    return any(
                        haversine(lc[0], lc[1], c[0], c[1]) <= self._HOP_GEO_RANGE_KM
                        for lc in local_coords_list
                    )
                return True  # GPS but no local reference — charitable

            # No GPS: fall back to reception evidence.
            if path_len is not None and path_len <= self._HOP_MAX_PATH_LEN:
                return True
            if count >= self._HOP_MIN_NODEINFO_COUNT:
                return True
            return False

        lookup: dict[str, list[str]] = {}
        seen_keys: set[str] = set()

        # Source 1: live MeshCore contacts (always eligible if device knows
        # them AND evidence supports — autoadd alone isn't enough since the
        # firmware adds every received advert).
        for backend in self.backends:
            if backend.backend_type != BackendType.MESHCORE:
                continue
            for prefix, contact in backend._contacts.items():
                full_key = contact.get('_full_pub_key', '') or contact.get('public_key', '')
                if not full_key or len(full_key) < hash_size * 2:
                    continue
                fk_lower = full_key.lower()
                seen_keys.add(fk_lower)
                name = contact.get('adv_name', '') or prefix
                if not _eligible(fk_lower, name, in_local_contacts=True):
                    continue
                h = full_key[:hash_size * 2].lower()
                lookup.setdefault(h, []).append(name)

        # Source 2: historical NODEINFO packets (fills gaps for nodes the
        # device hasn't autoadded — same eligibility gate applies).
        for entry in self._get_nodeinfo_entries():
            full_key = entry['full_key']
            fk_lower = full_key.lower()
            if fk_lower in seen_keys:
                continue
            if len(full_key) < hash_size * 2:
                continue
            seen_keys.add(fk_lower)
            if not _eligible(fk_lower, entry['name'], in_local_contacts=False):
                continue
            h = full_key[:hash_size * 2].lower()
            lookup.setdefault(h, []).append(entry['name'])

        return lookup, tuple(local_coords_list)

    def _get_nodeinfo_evidence(self) -> dict[str, dict]:
        """Per-node evidence used to gate route-hop candidate eligibility.

        Returns ``{full_key_lower: {'count': int, 'min_path_len': int|None}}``.

        ``count`` is the number of NODEINFO packets we've received from that
        node. ``min_path_len`` is the shortest path the advert traversed to
        reach us — large path_len means the sender is many hops away.

        Used by ``decode_route`` to suppress flood-only strangers (single
        advert, long path) from poisoning the hash-byte → node mapping.
        Cached for ``_nodeinfo_cache_ttl`` seconds.
        """
        now = time.time()
        if self._nodeinfo_evidence_cache and (now - self._nodeinfo_evidence_cache[0]) < self._nodeinfo_cache_ttl:
            return self._nodeinfo_evidence_cache[1]

        evidence: dict[str, dict] = {}
        try:
            with self.db_handler.lock:
                self.db_handler.cursor.execute(
                    "SELECT from_id, COUNT(*) FROM packets "
                    "WHERE port_name IN ('NODEINFO','NODEINFO_APP') "
                    "AND backend='meshcore' GROUP BY from_id"
                )
                count_by_from = dict(self.db_handler.cursor.fetchall())
        except Exception:
            count_by_from = {}

        for entry in self._get_nodeinfo_entries():
            full_key = (entry.get('full_key') or '').lower()
            if not full_key:
                continue
            from_id = f"mc:{full_key[:12]}"
            evidence[full_key] = {
                'count': count_by_from.get(from_id, 0),
                'path_len': entry.get('path_len'),
                'has_gps': entry.get('has_gps', False),
                'name': entry.get('name', ''),
            }

        self._nodeinfo_evidence_cache = (now, evidence)
        return evidence

    def _build_device_edge_counts(self, device_ids: list[str]) -> tuple[dict, set]:
        """Build edge counts from packets belonging to specific devices.

        Scans the packets table for MeshCore packets with path data from the
        given devices and extracts hash-pair edges.  Returns the same format
        as RouteAnalyzer's materialized graph data.
        """
        placeholders = ','.join('?' * len(device_ids))
        try:
            with self.db_handler.lock:
                self.db_handler.cursor.execute(
                    f"SELECT raw_packet FROM packets "
                    f"WHERE device_id IN ({placeholders}) AND backend='meshcore'",
                    device_ids,
                )
                rows = self.db_handler.cursor.fetchall()
        except Exception:
            return {}, set()

        edge_counts: dict[tuple[str, str], int] = {}
        node_hashes: set[str] = set()
        for (raw_json,) in rows:
            try:
                raw = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                path = raw.get('path', '')
                if not path or len(path) < 4:
                    continue
                hashes = [path[i:i + 2].lower() for i in range(0, len(path), 2)]
                for i in range(len(hashes) - 1):
                    edge_key = tuple(sorted([hashes[i], hashes[i + 1]]))
                    edge_counts[edge_key] = edge_counts.get(edge_key, 0) + 1
                for h in hashes:
                    node_hashes.add(h)
            except (json.JSONDecodeError, TypeError):
                pass

        # Filter weak edges (same threshold as RouteAnalyzer)
        edge_counts = {k: v for k, v in edge_counts.items() if v >= 2}
        node_hashes = set()
        for a, b in edge_counts:
            node_hashes.add(a)
            node_hashes.add(b)
        return edge_counts, node_hashes

    def decode_route(self, path_hex: str, hash_size: int = 1) -> list[dict]:
        """Decode a MeshCore path to a list of hop dicts.

        The path field in MeshCore packets is a hex string of 1-byte hashes
        (one per hop).  Each hash is the first byte of that node's public key.
        By matching these against known contacts we can show which nodes a
        packet travelled through.

        Phase 1: Basic hash lookup against known contacts.
        Phase 2: For ambiguous hops, use learned adjacency patterns to rank
                 candidates and assign confidence scores.

        Returns:
            [{'hash': 'c6', 'name': 'NodeName', 'ambiguous': False,
              'candidates': 1, 'confidence': 1.0, 'observations': 0,
              'candidate_names': ['NodeName']}, ...]
        """
        if not path_hex:
            return []

        # Check decode cache — invalidate when contacts or NODEINFO change
        gen = sum(
            getattr(b, '_contacts_generation', 0)
            for b in self.backends
            if b.backend_type == BackendType.MESHCORE
        )
        if self._nodeinfo_parsed_cache:
            gen += int(self._nodeinfo_parsed_cache[0])
        if gen != self._decode_route_gen:
            self._decode_route_cache.clear()
            self._decode_route_gen = gen
            self._decode_route_lookup = None
        cache_key = (path_hex, hash_size)
        cached = self._decode_route_cache.get(cache_key)
        if cached is not None:
            return cached

        # Build the hash → candidate-names lookup once per generation per
        # hash_size, gated on per-node evidence so flood-only strangers
        # don't poison the buckets.
        if self._decode_route_lookup is None or self._decode_route_lookup[0] != hash_size:
            self._decode_route_lookup = (hash_size, *self._build_route_lookup(hash_size))
        lookup: dict[str, list[str]] = self._decode_route_lookup[1]

        # Phase 1: basic hash lookup
        step = hash_size * 2
        raw_hashes = []
        for i in range(0, len(path_hex), step):
            raw_hashes.append(path_hex[i:i + step].lower())

        hops = []
        for h in raw_hashes:
            matches = lookup.get(h, [])
            hops.append({
                'hash': h,
                'name': matches[0] if len(matches) == 1 else (
                    ', '.join(matches[:2]) if matches else None
                ),
                'ambiguous': len(matches) > 1,
                'candidates': len(matches),
                'candidate_names': list(matches),
                'confidence': 1.0 if len(matches) == 1 else (
                    0.3 if matches else 0.0
                ),
                'observations': 0,
            })

        # Phase 2: use adjacency + geographic learning to resolve ambiguous hops
        analyzer = self.route_analyzer
        for idx, hop in enumerate(hops):
            if hop['candidates'] <= 1:
                # Unique or no match — confidence already set
                continue

            # Gather neighbor hashes
            neighbor_hashes = []
            if idx > 0:
                neighbor_hashes.append(hops[idx - 1]['hash'])
            if idx < len(hops) - 1:
                neighbor_hashes.append(hops[idx + 1]['hash'])

            if not neighbor_hashes:
                continue

            # Gather resolved neighbor names for geographic scoring.
            # Include uniquely-matched hops AND previously-resolved ambiguous
            # hops (confidence >= 0.5), so that earlier resolutions in the
            # left-to-right pass cascade forward to help later hops.
            resolved_neighbors = []
            for ni in (idx - 1, idx + 1):
                if 0 <= ni < len(hops):
                    nh = hops[ni]
                    if nh.get('name') and (
                        nh['candidates'] == 1
                        or nh.get('confidence', 0) >= 0.5
                    ):
                        resolved_neighbors.append(nh['name'])

            ranked, confidence = analyzer.resolve_ambiguous_hop_geo(
                hop['hash'],
                neighbor_hashes,
                hop['candidate_names'],
                resolved_neighbors=resolved_neighbors,
                all_hops=hops,
                backends=self.backends,
                db_handler=self.db_handler,
            )
            hop['confidence'] = confidence

            if ranked and ranked[0][1] > 0:
                best_name = ranked[0][0]
                hop['name'] = best_name
                hop['observations'] = int(ranked[0][1])
                if confidence >= 0.7:
                    hop['ambiguous'] = False

        # Store in decode cache (cap at 512 entries)
        if len(self._decode_route_cache) >= 512:
            self._decode_route_cache.clear()
        self._decode_route_cache[cache_key] = hops

        return hops

    def get_mesh_graph_data(self, max_nodes: int = 0, min_count: int = 2, device_ids: list[str] | None = None, focus_node: str | None = None, max_hops: int = 2) -> dict:
        """Return graph data for D3.js force-directed visualization.

        Reads from the RouteAnalyzer's adjacency cache and the node hash
        lookup to produce a nodes + links structure suitable for D3.

        Only includes nodes/edges with observation count >= 2 to reduce noise.

        Returns:
            {
                "nodes": [{"id": "c6", "name": "...", "connections": N, "confidence": F}],
                "links": [{"source": "c6", "target": "57", "count": N}]
            }
        """
        analyzer = self.route_analyzer

        # Build hash-to-name and hash-to-pubkey lookups from live contacts + DB
        hash_to_names: dict[str, list[str]] = {}
        hash_to_pubkeys: dict[str, list[str]] = {}
        seen_keys: set[str] = set()

        # Source 1: live MeshCore contacts (filtered by device_ids if specified)
        for backend in self.backends:
            if backend.backend_type == BackendType.MESHCORE:
                if device_ids and backend.device_id not in device_ids:
                    continue
                for prefix, contact in backend._contacts.items():
                    full_key = contact.get('_full_pub_key', '') or contact.get('public_key', '')
                    if full_key and len(full_key) >= 2:
                        h = full_key[:2].lower()
                        name = contact.get('adv_name', '') or prefix
                        hash_to_names.setdefault(h, []).append(name)
                        hash_to_pubkeys.setdefault(h, []).append(full_key[:12])
                        seen_keys.add(full_key.lower())

        # Source 2: historical NODEINFO packets (fills gaps after restart) — pre-parsed cache
        for entry in self._get_nodeinfo_entries():
            full_key = entry['full_key']
            if full_key.lower() in seen_keys:
                continue
            if len(full_key) >= 2:
                h = full_key[:2].lower()
                hash_to_names.setdefault(h, []).append(entry['name'])
                hash_to_pubkeys.setdefault(h, []).append(full_key[:12])
                seen_keys.add(full_key.lower())

        # Use device-specific edge counts when filtering, otherwise shared data
        if device_ids:
            edge_counts, node_hashes = self._build_device_edge_counts(device_ids)
        else:
            with analyzer._lock:
                edge_counts = dict(analyzer._graph_edge_counts)
                node_hashes = set(analyzer._graph_node_hashes)

        # Neighbourhood mode: BFS from focus node, keep only nearby hashes
        hop_distances: dict[str, int] | None = None
        focus_hash: str | None = None
        if focus_node:
            # Resolve focus_node to a 1-byte hash
            focus_hash = None
            fn = focus_node.strip().lower()
            if fn.startswith('h:') and len(fn) == 4:
                focus_hash = fn[2:]
            elif len(fn) == 2 and all(c in '0123456789abcdef' for c in fn):
                focus_hash = fn
            elif len(fn) >= 12 and all(c in '0123456789abcdef' for c in fn[:12]):
                focus_hash = fn[:2]
            else:
                # Try name match (case-insensitive)
                for h, names in hash_to_names.items():
                    if any(n.lower() == fn for n in names):
                        focus_hash = h
                        break

            if focus_hash and focus_hash in node_hashes:
                # Build adjacency from edge_counts with per-node neighbour cap
                # to prevent hash-collision explosion (top 4 strongest per hash)
                adj: dict[str, list[str]] = {}
                for (a, b), cnt in sorted(edge_counts.items(), key=lambda x: -x[1]):
                    adj.setdefault(a, [])
                    adj.setdefault(b, [])
                    if len(adj[a]) < 4:
                        adj[a].append(b)
                    if len(adj[b]) < 4:
                        adj[b].append(a)

                visited = {focus_hash: 0}
                frontier = {focus_hash}
                for hop_level in range(1, max_hops + 1):
                    next_frontier = set()
                    for h in frontier:
                        for nbr in adj.get(h, []):
                            if nbr not in visited:
                                visited[nbr] = hop_level
                                next_frontier.add(nbr)
                    frontier = next_frontier
                    if not frontier:
                        break
                node_hashes = set(visited.keys())
                edge_counts = {k: v for k, v in edge_counts.items()
                               if k[0] in node_hashes and k[1] in node_hashes}
                hop_distances = visited

        # Expand hashes into individual nodes using pubkey prefixes
        # Each real node gets its own graph entry with a unique ID (pubkey prefix)
        nodes = []
        node_ids = set()
        hash_to_node_ids: dict[str, list[str]] = {}  # hash -> list of graph node IDs

        # Ensure GeoResolver has fresh coordinates
        analyzer._geo._ensure_fresh(self.backends, self.db_handler)

        for h in sorted(node_hashes):
            pubkeys = hash_to_pubkeys.get(h, [])
            names = hash_to_names.get(h, [])
            if len(pubkeys) == 0:
                # Unknown hash — create a single node with hash as ID
                nid = f"h:{h}"
                nodes.append({
                    'id': nid, 'hash': h, 'name': h,
                    'confidence': 0.0, 'candidates': 0, 'pubkeys': [],
                })
                node_ids.add(nid)
                hash_to_node_ids.setdefault(h, []).append(nid)
            elif len(pubkeys) == 1:
                nid = pubkeys[0].lower()
                if nid not in node_ids:
                    nodes.append({
                        'id': nid, 'hash': h, 'name': names[0],
                        'confidence': 1.0, 'candidates': 1, 'pubkeys': [pubkeys[0]],
                    })
                    node_ids.add(nid)
                    hash_to_node_ids.setdefault(h, []).append(nid)
            else:
                # Ambiguous hash — use GeoResolver to rank candidates
                # Gather neighbor names from adjacency cache
                neighbor_names = []
                with analyzer._lock:
                    for (nh, nbr), cands in analyzer._cache.items():
                        if nh == h and cands:
                            # Find resolved neighbor name (unique hash)
                            nbr_names = hash_to_names.get(nbr, [])
                            if len(nbr_names) == 1:
                                neighbor_names.append(nbr_names[0])

                geo_results = analyzer._geo.score_candidates(
                    names, neighbor_names
                )
                # Build a geo-confidence lookup
                geo_conf = {r[0]: r[2] for r in geo_results}
                geo_best = geo_results[0][0] if geo_results and geo_results[0][2] > 0 else None

                for pk, name in zip(pubkeys, names):
                    nid = pk.lower()
                    if nid not in node_ids:
                        conf = geo_conf.get(name, 0.0)
                        if conf == 0.0:
                            conf = max(0.3, min(0.85, 1.0 / len(pubkeys)))
                        nodes.append({
                            'id': nid, 'hash': h, 'name': name,
                            'confidence': round(conf, 2),
                            'candidates': len(pubkeys), 'pubkeys': [pk],
                        })
                        node_ids.add(nid)
                        hash_to_node_ids.setdefault(h, []).append(nid)

        # Build links: expand hash-level edges into node-level edges.
        # When a hash has a confident geo-resolved winner, only that
        # node gets the edges — low-confidence candidates are excluded
        # so they don't cluster near nodes they aren't actually next to.
        node_conf = {n['id']: n.get('confidence', 0) for n in nodes}

        # Build nid->name lookup for _pick_representatives
        nid_to_name = {n['id']: n['name'] for n in nodes}

        def _pick_representatives(nids, near_coords=None):
            """From a set of node IDs sharing a hash, return the best
            candidate to receive edges.  Uses confidence first, then
            geo proximity as tiebreaker.

            Args:
                nids: list of candidate node IDs
                near_coords: optional (lat, lon) reference point — when set,
                    geography is the PRIMARY signal (overrides confidence),
                    because we know exactly where the local node is.
            """
            if len(nids) <= 1:
                return nids

            # When we have a known reference point (local node), geography
            # is the best signal — a 0.7 confidence node 500km away is less
            # likely our neighbor than a 0.3 confidence node 5km away.
            if near_coords and analyzer._geo.coord_count > 0:
                best_nid = None
                best_dist = float('inf')
                for nid in nids:
                    name = nid_to_name.get(nid, '')
                    c = analyzer._geo.get_coords(name)
                    if not c:
                        continue
                    d = analyzer._geo._haversine(near_coords[0], near_coords[1], c[0], c[1])
                    if d < best_dist:
                        best_dist = d
                        best_nid = nid
                if best_nid:
                    return [best_nid]

            # No reference point — use confidence, then neighbor-geo tiebreak
            high = [nid for nid in nids if node_conf.get(nid, 0) >= 0.7]
            if high:
                return high

            # Tiebreak: pick candidate geographically closest to resolved neighbors
            sample_hash = next((n['hash'] for n in nodes if n['id'] == nids[0]), None)
            if sample_hash and analyzer._geo.coord_count > 0:
                nbr_coords = []
                # Use secondary index for O(K) neighbor lookup instead of full cache scan
                for nbr in analyzer._hash_neighbors.get(sample_hash, set()):
                    nbr_names = hash_to_names.get(nbr, [])
                    if len(nbr_names) == 1:
                        c = analyzer._geo.get_coords(nbr_names[0])
                        if c:
                            nbr_coords.append(c)
                if nbr_coords:
                    best_nid = None
                    best_dist = float('inf')
                    for nid in nids:
                        name = nid_to_name.get(nid, '')
                        c = analyzer._geo.get_coords(name)
                        if not c:
                            continue
                        avg = sum(analyzer._geo._haversine(c[0], c[1], nc[0], nc[1])
                                  for nc in nbr_coords) / len(nbr_coords)
                        if avg < best_dist:
                            best_dist = avg
                            best_nid = nid
                    if best_nid:
                        return [best_nid]
            best = max(nids, key=lambda nid: node_conf.get(nid, 0))
            return [best]

        # Build links using candidate-level adjacency scores instead of raw
        # hash-level edges.  For each hash pair (a, b), the RouteAnalyzer's
        # cache has per-candidate observation counts.  We use the winning
        # candidate for each side (the one with the most observations for
        # this specific neighbor hash) to build accurate node-to-node edges.
        links = []
        link_set = set()

        # Targeted candidate lookups: only fetch adjacency scores for edges
        # we actually need (O(edges) not O(44k full cache))
        candidate_wins: dict[tuple[str, str], dict[str, int]] = {}
        with analyzer._lock:
            for (ha, hb) in edge_counts:
                key_ab = (ha, hb)
                key_ba = (hb, ha)
                if key_ab in analyzer._cache:
                    candidate_wins[key_ab] = dict(analyzer._cache[key_ab])
                if key_ba in analyzer._cache:
                    candidate_wins[key_ba] = dict(analyzer._cache[key_ba])

        # Reverse map: candidate name -> node ID (pubkey prefix) in the graph
        name_to_nid: dict[str, str] = {}
        for n in nodes:
            if n['name'] and n['name'] != n['id']:
                name_to_nid[n['name']] = n['id']

        for (ha, hb), count in sorted(edge_counts.items(), key=lambda x: -x[1]):
            a_candidates = candidate_wins.get((ha, hb), {})
            b_candidates = candidate_wins.get((hb, ha), {})

            # Pick the top candidate that exists in our graph
            def _best_candidate(cands, hash_key):
                for cand_name, cand_cnt in sorted(cands.items(), key=lambda x: -x[1]):
                    nid = name_to_nid.get(cand_name)
                    if nid and nid in node_ids:
                        return nid, cand_cnt
                # Fallback: use _pick_representatives
                reps = _pick_representatives(hash_to_node_ids.get(hash_key, []))
                return (reps[0] if reps else None), count

            a_id, a_cnt = _best_candidate(a_candidates, ha)
            b_id, b_cnt = _best_candidate(b_candidates, hb)

            if a_id and b_id and a_id != b_id:
                link_key = tuple(sorted([a_id, b_id]))
                if link_key not in link_set:
                    link_set.add(link_key)
                    # Use the smaller side's count as the edge weight
                    # (represents the bottleneck of the connection)
                    edge_count = min(a_cnt, b_cnt) if isinstance(a_cnt, int) and isinstance(b_cnt, int) else count
                    links.append({'source': a_id, 'target': b_id, 'count': edge_count})

        # Count connections per node using pre-computed dict
        def _count_connections(nodes, links):
            counts = {}
            for l in links:
                counts[l['source']] = counts.get(l['source'], 0) + 1
                counts[l['target']] = counts.get(l['target'], 0) + 1
            for n in nodes:
                n['connections'] = counts.get(n['id'], 0)

        _count_connections(nodes, links)

        # Boost confidence for ambiguous nodes that were actively selected
        # as link endpoints.  The link-building logic already used adjacency
        # scores and geo to pick the best candidate per hash — if a node has
        # connections, it was chosen over its competitors, so its displayed
        # confidence should reflect that rather than the raw 1/N hash fallback.
        for n in nodes:
            if n.get('candidates', 0) > 1 and n.get('connections', 0) > 0:
                conns = n['connections']
                # Scale: 1 connection → 0.75, 3+ → 0.85 (cap)
                boosted = min(0.85, 0.7 + 0.05 * min(conns, 3))
                if boosted > n.get('confidence', 0):
                    n['confidence'] = round(boosted, 2)

        # Neighbourhood cleanup: remove unconnected ghost candidates.
        # In neighbourhood mode, each hash expands to all its candidates but
        # only the winner gets links.  Strip the rest to keep the graph clean.
        # Only preserve the actual focus node, not all candidates sharing its hash.
        if focus_node and hop_distances:
            # Resolve which specific node ID is the focus
            fn_lower = focus_node.strip().lower()
            if len(fn_lower) >= 12 and all(c in '0123456789abcdef' for c in fn_lower[:12]):
                focus_keep = {fn_lower[:12]}
            else:
                focus_keep = set(hash_to_node_ids.get(focus_hash, []))
            connected_ids = set()
            for l in links:
                connected_ids.add(l['source'])
                connected_ids.add(l['target'])
            nodes = [n for n in nodes if n['id'] in connected_ids or n['id'] in focus_keep]
            node_ids = {n['id'] for n in nodes}

        # Add local node(s) — connected to their nearest repeaters
        # Build per-device last_hop_counts so devices on different frequencies
        # don't get phantom cross-links
        per_device_hops: dict[str, dict[str, int]] = {}
        with self.latest_packets_lock:
            for pkt in self.latest_packets:
                if pkt.get('backend') == 'meshcore':
                    raw = pkt.get('raw_packet', {})
                    path = raw.get('path', '') if isinstance(raw, dict) else ''
                    did = pkt.get('device_id', '')
                    if path and len(path) >= 4 and did:
                        last_hash = path[-2:].lower()
                        per_device_hops.setdefault(did, {})
                        per_device_hops[did][last_hash] = per_device_hops[did].get(last_hash, 0) + 1

        for backend in self.backends:
            if backend.backend_type == BackendType.MESHCORE:
                if device_ids and backend.device_id not in device_ids:
                    continue
                local_key = getattr(backend, '_local_pub_key', '') or ''
                local_name = getattr(backend, '_device_name', '') or 'Local'
                if local_key and len(local_key) >= 12:
                    local_id = local_key[:12].lower()
                    if local_id not in node_ids:
                        nodes.append({
                            'id': local_id, 'hash': local_key[:2].lower(),
                            'name': local_name, 'confidence': 1.0,
                            'candidates': 1, 'pubkeys': [local_id],
                            'is_local': True, 'connections': 0,
                        })
                        node_ids.add(local_id)
                    else:
                        for n in nodes:
                            if n['id'] == local_id:
                                n['is_local'] = True
                                n['name'] = local_name
                                break

                    # Connect to nearest repeaters — only from THIS device's packets
                    # Use local node's coordinates to pick geo-nearest candidate
                    # from ALL known candidates, not just the filtered graph set.
                    local_coords = analyzer._geo.get_coords(local_name) if analyzer._geo.coord_count > 0 else None
                    device_hops = per_device_hops.get(backend.device_id, {})
                    top_neighbors = sorted(device_hops.items(), key=lambda x: -x[1])[:5]
                    for neighbor_hash, count in top_neighbors:
                        # Build full candidate list from hash_to_pubkeys (not the filtered graph)
                        full_pks = hash_to_pubkeys.get(neighbor_hash, [])
                        full_names = hash_to_names.get(neighbor_hash, [])

                        # Pick the geo-nearest candidate to our local node
                        best_nid = None
                        best_dist = float('inf')
                        if local_coords and len(full_pks) > 1:
                            for i, pk in enumerate(full_pks):
                                nid = pk.lower()
                                name = full_names[i] if i < len(full_names) else ''
                                c = analyzer._geo.get_coords(name)
                                if c:
                                    d = analyzer._geo._haversine(
                                        local_coords[0], local_coords[1], c[0], c[1])
                                    if d < best_dist:
                                        best_dist = d
                                        best_nid = nid
                        # Skip if nearest GPS candidate is >150km — likely a collision
                        if best_nid and best_dist > 150:
                            best_nid = None

                        # Second chance: check non-GPS candidates that are anchored
                        # as local by strong adjacency to verified-local nodes
                        if not best_nid and local_coords and len(full_pks) > 1:
                            # Hashes of nodes confirmed within 150km of local
                            local_hashes = set()
                            for l in links:
                                for lid in (l['source'], l['target']):
                                    ln = next((n for n in nodes if n['id'] == lid and n.get('is_local')), None)
                                    if ln:
                                        local_hashes.add(ln.get('hash', ''))
                            # Also include hashes of 1st-hop neighbors already accepted
                            for l in links:
                                if l['source'] == local_id or l['target'] == local_id:
                                    nbr = l['target'] if l['source'] == local_id else l['source']
                                    nbr_n = next((n for n in nodes if n['id'] == nbr), None)
                                    if nbr_n:
                                        local_hashes.add(nbr_n.get('hash', ''))

                            for i, pk in enumerate(full_pks):
                                nid = pk.lower()
                                name = full_names[i] if i < len(full_names) else ''
                                # Skip candidates we already checked (have GPS)
                                if analyzer._geo.get_coords(name):
                                    continue
                                # Check if this candidate co-occurs with local hashes
                                co_count = 0
                                for lh in local_hashes:
                                    cands = analyzer._cache.get((neighbor_hash, lh), {})
                                    co_count += cands.get(name, 0)
                                if co_count >= 3:
                                    best_nid = nid
                                    break

                        if not best_nid and full_pks:
                            if len(full_pks) == 1:
                                nid = full_pks[0].lower()
                                name = full_names[0] if full_names else ''
                                c = analyzer._geo.get_coords(name) if local_coords else None
                                if c and analyzer._geo._haversine(
                                        local_coords[0], local_coords[1], c[0], c[1]) > 150:
                                    continue
                                best_nid = nid
                            elif not local_coords:
                                graph_nids = hash_to_node_ids.get(neighbor_hash, [])
                                best_nid = graph_nids[0] if graph_nids else full_pks[0].lower()

                        if best_nid and best_nid != local_id:
                            # Ensure node exists in graph (add if missing)
                            if best_nid not in node_ids:
                                idx = next((i for i, pk in enumerate(full_pks)
                                            if pk.lower() == best_nid), 0)
                                name = full_names[idx] if idx < len(full_names) else best_nid
                                nodes.append({
                                    'id': best_nid, 'hash': neighbor_hash,
                                    'name': name, 'confidence': 0.5,
                                    'candidates': len(full_pks),
                                    'pubkeys': [best_nid], 'connections': 0,
                                })
                                node_ids.add(best_nid)
                            lk = tuple(sorted([local_id, best_nid]))
                            if lk not in link_set:
                                link_set.add(lk)
                                links.append({'source': local_id, 'target': best_nid, 'count': count})

        # Remove implausible local connections: if a link touches a local
        # node and the other end is >150km away, it's a hash collision artifact.
        local_id_set = {n['id'] for n in nodes if n.get('is_local')}
        if local_id_set and analyzer._geo.coord_count > 0:
            local_coord_map = {}
            for n in nodes:
                if n.get('is_local'):
                    c = analyzer._geo.get_coords(n['name'])
                    if c:
                        local_coord_map[n['id']] = c

            if local_coord_map:
                filtered_links = []
                for l in links:
                    s, t = l['source'], l['target']
                    drop = False
                    if s in local_coord_map:
                        tc = analyzer._geo.get_coords(
                            next((n['name'] for n in nodes if n['id'] == t), ''))
                        if tc and analyzer._geo._haversine(
                                local_coord_map[s][0], local_coord_map[s][1],
                                tc[0], tc[1]) > 150:
                            drop = True
                    elif t in local_coord_map:
                        sc = analyzer._geo.get_coords(
                            next((n['name'] for n in nodes if n['id'] == s), ''))
                        if sc and analyzer._geo._haversine(
                                local_coord_map[t][0], local_coord_map[t][1],
                                sc[0], sc[1]) > 150:
                            drop = True
                    if not drop:
                        filtered_links.append(l)
                links = filtered_links

        # Recount connections
        _count_connections(nodes, links)

        total_nodes = len(nodes)
        total_links = len(links)

        # ── Server-side filtering for performance ──
        if max_nodes > 0 and len(nodes) > max_nodes and not focus_node:
            # Always keep: local nodes + direct neighbors + their direct
            # connections (2nd hop) so the path into the main mesh is preserved.
            keep_ids: set[str] = set()
            local_ids = {n['id'] for n in nodes if n.get('is_local')}
            keep_ids.update(local_ids)
            # 1st hop: direct neighbors of local nodes
            hop1 = set()
            for l in links:
                if l['source'] in local_ids:
                    hop1.add(l['target'])
                if l['target'] in local_ids:
                    hop1.add(l['source'])
            keep_ids.update(hop1)
            # 2nd hop: top 3 strongest connections per hop-1 node only
            # — keeps gateway nodes like Exmouth~West without cascading
            hop1_nbrs: dict[str, list] = {nid: [] for nid in hop1}
            for l in links:
                if l['source'] in hop1 and l['target'] not in keep_ids:
                    hop1_nbrs[l['source']].append((l['target'], l['count']))
                if l['target'] in hop1 and l['source'] not in keep_ids:
                    hop1_nbrs[l['target']].append((l['source'], l['count']))
            for nid in hop1:
                top = sorted(hop1_nbrs[nid], key=lambda x: -x[1])[:3]
                for nbr_id, _ in top:
                    keep_ids.add(nbr_id)

            # Fill remaining slots by importance
            remaining = max_nodes - len(keep_ids)
            if remaining > 0:
                scored = sorted(
                    (n for n in nodes if n['id'] not in keep_ids and n['connections'] > 0),
                    key=lambda n: n['connections'] * (1 + n.get('confidence', 0)),
                    reverse=True,
                )
                for n in scored[:remaining]:
                    keep_ids.add(n['id'])

            nodes = [n for n in nodes if n['id'] in keep_ids]
            links = [l for l in links if l['source'] in keep_ids and l['target'] in keep_ids]

            # Recount after filtering
            _count_connections(nodes, links)

        # Cap links for D3 performance: keep top 4 strongest per node, max 300 total.
        # Always preserve local node links and bridge links to maintain connectivity.
        if len(links) > 300 and not focus_node:
            local_node_ids = {n['id'] for n in nodes if n.get('is_local')}
            # Identify links that touch local nodes or their direct neighbors
            local_nbrs = set()
            for l in links:
                if l['source'] in local_node_ids:
                    local_nbrs.add(l['target'])
                elif l['target'] in local_node_ids:
                    local_nbrs.add(l['source'])
            protected_ids = local_node_ids | local_nbrs

            # Split into protected (always keep) and cappable
            protected = [l for l in links
                         if l['source'] in protected_ids or l['target'] in protected_ids]
            cappable = [l for l in links
                        if l['source'] not in protected_ids and l['target'] not in protected_ids]
            cappable.sort(key=lambda l: l['count'], reverse=True)

            budget = 300 - len(protected)
            kept = []
            per_node = {}
            for l in cappable:
                sc = per_node.get(l['source'], 0)
                tc = per_node.get(l['target'], 0)
                if sc < 4 or tc < 4:
                    kept.append(l)
                    per_node[l['source']] = sc + 1
                    per_node[l['target']] = tc + 1
                    if len(kept) >= budget:
                        break
            links = protected + kept
            _count_connections(nodes, links)

        result = {
            'nodes': nodes,
            'links': links,
            'total_nodes': total_nodes,
            'total_links': total_links,
        }
        if hop_distances:
            # Resolve the actual focus node ID (not all candidates on the same hash)
            fn_lower = focus_node.strip().lower() if focus_node else ''
            if len(fn_lower) >= 12 and all(c in '0123456789abcdef' for c in fn_lower[:12]):
                actual_focus_id = fn_lower[:12]
            else:
                focus_nids = hash_to_node_ids.get(focus_hash, [])
                actual_focus_id = focus_nids[0] if focus_nids else None

            node_hop_map = {}
            for n in nodes:
                if n['id'] == actual_focus_id:
                    node_hop_map[n['id']] = 0  # only the actual clicked node is hop 0
                else:
                    dist = hop_distances.get(n.get('hash', ''), -1)
                    # Other nodes sharing the focus hash get hop 1, not hop 0
                    node_hop_map[n['id']] = max(dist, 1) if dist == 0 else dist
            result['hop_distances'] = node_hop_map
            result['focus_hash'] = focus_hash
            result['focus_node_id'] = actual_focus_id
        return result

    # ── Flood advertisement ────────────────────────────────────

    def get_meshcore_devices(self) -> list[dict]:
        """Return a list of connected MeshCore backends (for UI device picker)."""
        devices = []
        for b in self.backends:
            if b.backend_type == BackendType.MESHCORE and b.is_connected:
                devices.append({
                    'device_id': b.device_id,
                    'device_name': getattr(b, '_device_name', '') or b.device_id,
                    'local_node_id': b.local_node_id,
                    'pub_key': getattr(b, '_local_pub_key', '') or '',
                })
        return devices

    def send_flood_advertisement(self, device_id: str | None = None) -> dict:
        """Send a flooded advertisement from a MeshCore device.

        Returns a dict with the flood start time and device info so the
        caller can begin polling for heard-back results.
        """
        backend = None
        for b in self.backends:
            if b.backend_type == BackendType.MESHCORE and b.is_connected:
                if device_id is None or b.device_id == device_id:
                    backend = b
                    break

        if backend is None:
            return {'success': False, 'error': 'No connected MeshCore device found'}

        result = backend.send_advertisement(flood=True)
        if result != 'ok':
            return {'success': False, 'error': result}

        flood_time = datetime.now().isoformat()
        pub_key = getattr(backend, '_local_pub_key', '') or ''

        return {
            'success': True,
            'flood_time': flood_time,
            'device_id': backend.device_id,
            'device_name': getattr(backend, '_device_name', '') or backend.device_id,
            'pub_key': pub_key,
        }

    def clear_traceroute_results(self):
        # Clear orchestrator-level traceroute state
        with self.traceroute_results_lock:
            self.traceroute_results = {}
        self.traceroute_completed = False
        # Also clear per-backend traceroute state
        for b in self.backends:
            if hasattr(b, 'traceroute_results_lock'):
                with b.traceroute_results_lock:
                    b.traceroute_results = {}
                b.traceroute_completed = False

    def get_traceroute_results(self):
        # Check orchestrator-level results first (MeshCore)
        with self.traceroute_results_lock:
            if self.traceroute_results:
                return dict(self.traceroute_results)
        # Fall back to per-backend results
        for b in self.backends:
            if hasattr(b, 'traceroute_results_lock'):
                with b.traceroute_results_lock:
                    if b.traceroute_results:
                        return dict(b.traceroute_results)
        return {}

    # ── CLI commands ──────────────────────────────────────────

    def list_nodes(self):
        """List all known nodes."""
        print("\nKnown Nodes:")
        for node_id, name in self.node_name_map.items():
            print(f"{node_id}: {name}")
        print()

    def export_data(self, export_format='json'):
        """Export data to a file (last 48 hours)."""
        filename = f"meshtastic_data.{export_format}"
        packets = self.db_handler.fetch_packets(hours=48)

        if export_format == 'json':
            data = []
            for packet in packets:
                data.append({
                    'timestamp': packet[0],
                    'from_id': packet[1],
                    'to_id': packet[2],
                    'port_name': packet[3],
                    'payload': packet[4],
                    'raw_packet': json.loads(packet[5])
                })
            with open(filename, 'w') as f:
                json.dump(data, f, default=self._json_serializer, indent=2)
            logger.info(f"Data exported to {filename}")
        elif export_format == 'csv':
            import csv
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'from_id', 'to_id', 'port_name', 'payload', 'raw_packet'])
                for packet in packets:
                    writer.writerow(packet)
            logger.info(f"Data exported to {filename}")
        else:
            logger.error(f"Unsupported export format: {export_format}")

    def display_stats(self):
        """Display statistics about the network or messages received."""
        packet_count, node_count, port_usage = self.db_handler.fetch_packet_stats()

        print("\nNetwork Statistics:")
        print(f"Total Packets Received: {packet_count}")
        print(f"Total Nodes Communicated: {node_count}")
        print("Port Usage:")
        for port, count in port_usage:
            print(f"  {port}: {count} packets")
        print()

    def cleanup(self):
        """Clean up resources."""
        try:
            self.route_analyzer.flush()
        except Exception as e:
            logger.error(f"Error flushing route analyzer: {e}")
        try:
            self.db_handler.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        for b in list(self.backends):
            try:
                b.disconnect()
            except Exception as e:
                logger.error(f"Error closing {b.backend_type.value} interface: {e}")


# ══════════════════════════════════════════════════════════════════
# MeshConsole -- new-style orchestrator (aliased for convenience)
# ══════════════════════════════════════════════════════════════════

MeshConsole = MeshtasticTool  # Will diverge in future phases


# ══════════════════════════════════════════════════════════════════
# Logging configuration
# ══════════════════════════════════════════════════════════════════

def configure_logging(config_file=DEFAULT_CONFIG_FILE):
    """Configure logging settings with rotation support."""
    config = configparser.ConfigParser()
    config.read(config_file)

    log_level = config.get('Logging', 'level', fallback='INFO')
    log_file = config.get('Logging', 'file', fallback='meshtastic_tool.log')
    max_size = config.getint('Logging', 'max_size', fallback=10) * 1024 * 1024
    backup_count = config.getint('Logging', 'backup_count', fallback=5)

    from logging.handlers import RotatingFileHandler

    log_format = '%(asctime)s %(levelname)s [%(name)s]: %(message)s'

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_size,
        backupCount=backup_count
    )
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        handlers=[file_handler, console_handler]
    )

    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)


# ══════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════

def main():
    """Main function -- delegates to the CLI module's parser and dispatch."""
    from meshconsole.cli import cli_main
    cli_main()


if __name__ == '__main__':
    main()
