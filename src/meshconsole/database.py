"""
MeshConsole Database Handler
----------------------------
Thread-safe SQLite database operations for packet and message storage.

Extracted from core.py MeshtasticTool monolith.

Author: M9WAV
License: MIT
"""

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


# zstd frame magic — first 4 bytes of every zstd-compressed blob.
# Used to distinguish compressed BLOB raw_packet rows from legacy JSON TEXT.
_ZSTD_MAGIC = b'\x28\xb5\x2f\xfd'

try:
    import zstandard as _zstd
    _ZSTD_COMPRESSOR = _zstd.ZstdCompressor(level=3)
    _ZSTD_DECOMPRESSOR = _zstd.ZstdDecompressor()
    ZSTD_AVAILABLE = True
except ImportError:
    _ZSTD_COMPRESSOR = None
    _ZSTD_DECOMPRESSOR = None
    ZSTD_AVAILABLE = False
    logger.info("zstandard not installed — raw_packet storage stays uncompressed")


def encode_raw_packet(raw_packet) -> 'str | bytes':
    """Serialise a raw_packet dict for DB storage.

    Returns zstd-compressed BLOB when zstandard is available, else JSON TEXT
    so old deployments without the dependency keep working. Compressed blobs
    start with the zstd magic ``28 b5 2f fd`` so readers can detect them.
    """
    payload = json.dumps(raw_packet, default=str)
    if _ZSTD_COMPRESSOR is not None:
        return _ZSTD_COMPRESSOR.compress(payload.encode('utf-8'))
    return payload


def decode_raw_packet(stored) -> dict:
    """Reverse ``encode_raw_packet`` — works on TEXT, BLOB, or pre-decoded dict.

    Detects compressed blobs by the zstd magic bytes so legacy JSON-TEXT rows
    still decode without a migration step.
    """
    if stored is None or stored == '':
        return {}
    if isinstance(stored, dict):
        return stored
    if isinstance(stored, (bytes, bytearray, memoryview)):
        b = bytes(stored)
        if b[:4] == _ZSTD_MAGIC and _ZSTD_DECOMPRESSOR is not None:
            try:
                b = _ZSTD_DECOMPRESSOR.decompress(b)
            except Exception:
                pass  # corrupted frame — fall through to JSON parse
        try:
            return json.loads(b.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
    if isinstance(stored, str):
        try:
            return json.loads(stored)
        except json.JSONDecodeError:
            return {}
    return {}


class DatabaseHandler:
    """Handles database operations in a thread-safe manner."""

    def __init__(self, db_file='meshtastic_messages.db'):
        self.db_file = db_file
        self.lock = threading.Lock()
        self._setup_database()
        self._migrate_backend_column()
        self._migrate_device_id_column()
        self._setup_nodes_live()
        self._prune_thread = None
        self._prune_stop = threading.Event()

    def _setup_database(self):
        """Set up SQLite database for message and packet logging."""
        try:
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA synchronous=NORMAL')
            self.cursor = self.conn.cursor()

            # Create messages table if not exists
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    timestamp TEXT,
                    from_id TEXT,
                    to_id TEXT,
                    port_name TEXT,
                    message TEXT
                )
            ''')

            # Create packets table for storing all packets
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS packets (
                    timestamp TEXT,
                    from_id TEXT,
                    to_id TEXT,
                    port_name TEXT,
                    payload TEXT,
                    raw_packet TEXT
                )
            ''')

            # Create indexes for faster filtering
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_packets_from_id ON packets(from_id)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_packets_to_id ON packets(to_id)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_packets_port_name ON packets(port_name)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_packets_timestamp ON packets(timestamp DESC)')

            # Route adjacency learning table (v3.3.0)
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS route_adjacency (
                    node_hash TEXT NOT NULL,
                    neighbor_hash TEXT NOT NULL,
                    node_candidate TEXT NOT NULL,
                    count INTEGER DEFAULT 1,
                    last_seen TEXT,
                    PRIMARY KEY (node_hash, neighbor_hash, node_candidate)
                )
            ''')
            self.cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_route_adj_lookup '
                'ON route_adjacency(node_hash, neighbor_hash)'
            )

            # Message indexes for conversation queries (v3.8.0)
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(from_id, to_id, timestamp DESC)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(timestamp DESC)')

            self.conn.commit()
            logger.info("Database initialized.")
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise RuntimeError("Failed to initialize the database.")

    def _migrate_backend_column(self):
        """Add backend column to tables if missing (v3.0 migration)."""
        try:
            # Check if backend column exists in packets table
            self.cursor.execute("PRAGMA table_info(packets)")
            columns = [row[1] for row in self.cursor.fetchall()]
            if 'backend' not in columns:
                logger.info("Migrating database: adding backend column to packets table")
                self.cursor.execute("ALTER TABLE packets ADD COLUMN backend TEXT DEFAULT 'meshtastic'")
                self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_packets_backend ON packets(backend)')
                self.conn.commit()

            # Check if backend column exists in messages table
            self.cursor.execute("PRAGMA table_info(messages)")
            columns = [row[1] for row in self.cursor.fetchall()]
            if 'backend' not in columns:
                logger.info("Migrating database: adding backend column to messages table")
                self.cursor.execute("ALTER TABLE messages ADD COLUMN backend TEXT DEFAULT 'meshtastic'")
                self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_backend ON messages(backend)')
                self.conn.commit()

            # Composite indexes for common query patterns (v3.9.0 perf)
            # These depend on backend column existing, so they live here after migration
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_packets_port_from_ts ON packets(port_name, from_id, timestamp DESC)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_packets_backend_ts ON packets(backend, timestamp DESC)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_to_id ON messages(to_id)')
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database migration error: {e}")

    def _migrate_device_id_column(self):
        """Add device_id column to tables if missing (v3.2.0 migration)."""
        try:
            # Check if device_id column exists in packets table
            self.cursor.execute("PRAGMA table_info(packets)")
            columns = [row[1] for row in self.cursor.fetchall()]
            if 'device_id' not in columns:
                logger.info("Migrating database: adding device_id column to packets table")
                self.cursor.execute("ALTER TABLE packets ADD COLUMN device_id TEXT DEFAULT ''")
                self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_packets_device_id ON packets(device_id)')
                self.conn.commit()

            # Check if device_id column exists in messages table
            self.cursor.execute("PRAGMA table_info(messages)")
            columns = [row[1] for row in self.cursor.fetchall()]
            if 'device_id' not in columns:
                logger.info("Migrating database: adding device_id column to messages table")
                self.cursor.execute("ALTER TABLE messages ADD COLUMN device_id TEXT DEFAULT ''")
                self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_device_id ON messages(device_id)')
                self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database device_id migration error: {e}")

    def _setup_nodes_live(self):
        """Materialised per-node summary table (v3.15.0).

        ``_get_nodeinfo_entries`` and friends used to scan the entire packets
        table on every cache miss. With ~3.5 k known nodes that's a lot of
        repeated JSON parsing. This table holds one row per (full_key,
        backend) pair and is updated incrementally by the orchestrator on
        every NODEINFO arrival, so the hot path becomes O(nodes) instead of
        O(packets).
        """
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS nodes_live (
                    full_key TEXT NOT NULL,
                    backend TEXT NOT NULL,
                    name TEXT,
                    hash_byte TEXT,
                    lat REAL,
                    lon REAL,
                    has_gps INTEGER DEFAULT 0,
                    nodeinfo_count INTEGER DEFAULT 0,
                    min_path_len INTEGER,
                    last_seen TEXT,
                    PRIMARY KEY (full_key, backend)
                )
            ''')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_nodes_live_hash ON nodes_live(hash_byte, backend)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_nodes_live_last_seen ON nodes_live(last_seen DESC)')
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"nodes_live setup error: {e}")

    def upsert_node(self, full_key: str, backend: str, name: str = '',
                    lat: float | None = None, lon: float | None = None,
                    timestamp: str | None = None, path_len: int | None = None):
        """Record/update a NODEINFO observation in nodes_live.

        Increments ``nodeinfo_count`` on each call. ``min_path_len`` only
        decreases — the closest the node has ever been heard from. ``has_gps``
        flips to 1 the first time we see plausible non-zero coords; it doesn't
        revert on a later 0,0 broadcast (which we treat as missing GPS, not
        deliberate teleportation to the Atlantic).
        """
        if not full_key:
            return
        full_key = full_key.lower()
        hash_byte = full_key[:2]
        ts = timestamp or datetime.now().isoformat()
        has_gps = int(
            lat is not None and lon is not None
            and abs(float(lat)) > 0.01 and abs(float(lon)) > 0.01
        )
        with self.lock:
            try:
                self.cursor.execute(
                    'SELECT name, lat, lon, has_gps, nodeinfo_count, min_path_len FROM nodes_live WHERE full_key=? AND backend=?',
                    (full_key, backend),
                )
                row = self.cursor.fetchone()
                if row:
                    new_name = name or row[0] or ''
                    new_lat = lat if has_gps else row[1]
                    new_lon = lon if has_gps else row[2]
                    new_has_gps = max(int(row[3] or 0), has_gps)
                    new_count = (row[4] or 0) + 1
                    new_min_path = (
                        path_len if row[5] is None
                        else min(row[5], path_len) if path_len is not None
                        else row[5]
                    )
                    self.cursor.execute(
                        'UPDATE nodes_live SET name=?, lat=?, lon=?, has_gps=?, '
                        'nodeinfo_count=?, min_path_len=?, last_seen=? '
                        'WHERE full_key=? AND backend=?',
                        (new_name, new_lat, new_lon, new_has_gps,
                         new_count, new_min_path, ts, full_key, backend),
                    )
                else:
                    self.cursor.execute(
                        'INSERT INTO nodes_live VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (full_key, backend, name, hash_byte,
                         lat if has_gps else None, lon if has_gps else None,
                         has_gps, 1, path_len, ts),
                    )
                self.conn.commit()
            except sqlite3.Error as e:
                logger.error(f"upsert_node error: {e}")

    def fetch_nodes_live(self, backend: str | None = None) -> list[dict]:
        """Return all rows from nodes_live, optionally filtered by backend."""
        with self.lock:
            try:
                if backend:
                    self.cursor.execute(
                        'SELECT full_key, backend, name, hash_byte, lat, lon, has_gps, '
                        'nodeinfo_count, min_path_len, last_seen FROM nodes_live WHERE backend=?',
                        (backend,),
                    )
                else:
                    self.cursor.execute(
                        'SELECT full_key, backend, name, hash_byte, lat, lon, has_gps, '
                        'nodeinfo_count, min_path_len, last_seen FROM nodes_live'
                    )
                rows = self.cursor.fetchall()
            except sqlite3.Error as e:
                logger.error(f"fetch_nodes_live error: {e}")
                return []
        return [
            {
                'full_key': r[0], 'backend': r[1], 'name': r[2] or '',
                'hash_byte': r[3] or '', 'lat': r[4], 'lon': r[5],
                'has_gps': bool(r[6]), 'nodeinfo_count': r[7] or 0,
                'min_path_len': r[8], 'last_seen': r[9] or '',
            }
            for r in rows
        ]

    def backfill_nodes_live(self) -> int:
        """One-shot backfill from existing packets — runs once at startup if
        the table is empty so existing deployments don't have to wait for
        new NODEINFO arrivals to populate it.

        Returns the number of distinct nodes seeded.
        """
        with self.lock:
            try:
                self.cursor.execute('SELECT COUNT(*) FROM nodes_live')
                if (self.cursor.fetchone()[0] or 0) > 0:
                    return 0
                self.cursor.execute(
                    "SELECT raw_packet, MAX(timestamp), backend, COUNT(*) "
                    "FROM packets WHERE port_name IN ('NODEINFO','NODEINFO_APP') "
                    "GROUP BY from_id, backend"
                )
                rows = self.cursor.fetchall()
            except sqlite3.Error as e:
                logger.error(f"backfill_nodes_live: query failed: {e}")
                return 0

        seeded = 0
        for raw, latest_ts, backend, count in rows:
            try:
                rp = decode_raw_packet(raw)
                full_key = (rp.get('public_key') or rp.get('adv_key') or '').lower()
                if not full_key:
                    continue
                lat = rp.get('adv_lat') or rp.get('latitude')
                lon = rp.get('adv_lon') or rp.get('longitude')
                self.upsert_node(
                    full_key=full_key,
                    backend=backend or 'meshcore',
                    name=rp.get('adv_name') or rp.get('name') or '',
                    lat=lat, lon=lon,
                    timestamp=latest_ts,
                    path_len=rp.get('path_len'),
                )
                # backfill records each row as if it were a single observation;
                # bump count to reflect aggregate
                if count and count > 1:
                    with self.lock:
                        try:
                            self.cursor.execute(
                                'UPDATE nodes_live SET nodeinfo_count=? '
                                'WHERE full_key=? AND backend=?',
                                (count, full_key, backend or 'meshcore'),
                            )
                            self.conn.commit()
                        except sqlite3.Error:
                            pass
                seeded += 1
            except Exception:
                continue
        if seeded:
            logger.info(f"backfill_nodes_live: seeded {seeded} nodes from packets history")
        return seeded

    def prune_old_packets(self, max_age_days: int) -> dict:
        """Delete packets and messages older than ``max_age_days``.

        Returns a dict with row counts and elapsed seconds. Runs VACUUM
        INCREMENTAL after deletion to reclaim space without locking the DB.
        """
        if not max_age_days or max_age_days <= 0:
            return {'packets_deleted': 0, 'messages_deleted': 0, 'elapsed': 0.0}
        cutoff = (datetime.now() - timedelta(days=max_age_days)).isoformat()
        t0 = time.time()
        with self.lock:
            try:
                self.cursor.execute('DELETE FROM packets WHERE timestamp < ?', (cutoff,))
                pkts = self.cursor.rowcount
                self.cursor.execute('DELETE FROM messages WHERE timestamp < ?', (cutoff,))
                msgs = self.cursor.rowcount
                self.conn.commit()
                # WAL checkpoint then incremental vacuum (best-effort)
                try:
                    self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                    self.conn.execute('PRAGMA incremental_vacuum')
                except sqlite3.Error:
                    pass
            except sqlite3.Error as e:
                logger.error(f"prune_old_packets failed: {e}")
                return {'packets_deleted': 0, 'messages_deleted': 0, 'elapsed': time.time() - t0, 'error': str(e)}
        return {'packets_deleted': pkts, 'messages_deleted': msgs, 'elapsed': time.time() - t0}

    def start_prune_thread(self, max_age_days: int, interval_seconds: int = 3600):
        """Start a daemon thread that prunes older-than-max_age_days rows
        every ``interval_seconds``. No-op if max_age_days is falsy.
        """
        if not max_age_days or max_age_days <= 0 or self._prune_thread is not None:
            return

        def _loop():
            # Stagger first run so service startup isn't slowed
            if self._prune_stop.wait(60):
                return
            while not self._prune_stop.is_set():
                try:
                    result = self.prune_old_packets(max_age_days)
                    if result.get('packets_deleted') or result.get('messages_deleted'):
                        logger.info(
                            f"Auto-prune: removed {result['packets_deleted']} packets, "
                            f"{result['messages_deleted']} messages older than {max_age_days}d "
                            f"in {result['elapsed']:.1f}s"
                        )
                except Exception as e:
                    logger.warning(f"Auto-prune iteration failed: {e}")
                if self._prune_stop.wait(interval_seconds):
                    return

        self._prune_thread = threading.Thread(target=_loop, daemon=True, name='db-prune')
        self._prune_thread.start()
        logger.info(f"Auto-prune thread started (max_age_days={max_age_days}, interval={interval_seconds}s)")

    def stop_prune_thread(self):
        """Signal the prune thread to exit (called during shutdown)."""
        self._prune_stop.set()

    def log_message(self, timestamp, from_id, to_id, port_name, message, backend='meshtastic', device_id=''):
        """Log the message to the SQLite database, skipping duplicates.

        Deduplicates messages with the same from_id, to_id, and message text
        within a 10-second window — handles the same broadcast being received
        by multiple radios on different frequencies.
        """
        with self.lock:
            try:
                # Check for duplicate within 10s window
                cutoff = (datetime.now() - timedelta(seconds=10)).isoformat()
                self.cursor.execute(
                    'SELECT 1 FROM messages WHERE from_id = ? AND to_id = ? AND message = ? AND timestamp >= ? LIMIT 1',
                    (from_id, to_id, message, cutoff)
                )
                if self.cursor.fetchone():
                    logger.debug("Duplicate message suppressed: %s from %s", message[:30], from_id)
                    return
                self.cursor.execute(
                    'INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (timestamp, from_id, to_id, port_name, message, backend, device_id)
                )
                self.conn.commit()
                logger.debug("Message logged to database.")
            except sqlite3.Error as e:
                logger.error(f"Failed to log message to database: {e}")

    def log_packet(self, packet_data):
        """Log the packet to the SQLite database.

        Args:
            packet_data: Either a dict (legacy) or a UnifiedPacket-like object with to_dict().
        """
        with self.lock:
            try:
                # Support both dict and dataclass-like objects
                if hasattr(packet_data, 'to_dict'):
                    d = packet_data.to_dict()
                elif isinstance(packet_data, dict):
                    d = packet_data
                else:
                    d = dict(packet_data)

                backend = d.get('backend', 'meshtastic')
                # If backend is an enum value, get the string
                if hasattr(backend, 'value'):
                    backend = backend.value

                device_id = d.get('device_id', '')

                self.cursor.execute(
                    'INSERT INTO packets VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (
                        d['timestamp'],
                        d['from_id'],
                        d['to_id'],
                        d['port_name'],
                        d.get('payload', ''),
                        encode_raw_packet(d.get('raw_packet', {})),
                        backend,
                        device_id,
                    )
                )
                self.conn.commit()
                logger.debug("Packet logged to database.")
            except sqlite3.Error as e:
                logger.error(f"Failed to log packet to database: {e}")

    def fetch_packets(self, hours=None, backend=None):
        """Fetch packets from the database, optionally filtered by time and backend.

        Args:
            hours: If specified, only return packets from the last N hours.
            backend: If specified, only return packets from this backend.
        """
        with self.lock:
            conditions = []
            params = []

            if hours:
                conditions.append('timestamp >= datetime("now", ? || " hours")')
                params.append(f'-{hours}')

            if backend:
                conditions.append('backend = ?')
                params.append(backend)

            where_clause = ' AND '.join(conditions) if conditions else '1=1'

            self.cursor.execute(
                f'SELECT * FROM packets WHERE {where_clause} ORDER BY timestamp DESC',
                params
            )
            return self.cursor.fetchall()

    def fetch_packets_filtered(self, node_filter=None, port_filter=None, limit=1000, backend=None, device_id=None):
        """Fetch packets from the database with optional filters.

        Args:
            node_filter: If specified, only return packets where from_id or to_id matches.
            port_filter: If specified, only return packets with matching port_name.
            limit: Maximum number of packets to return.
            backend: If specified, only return packets from this backend.
            device_id: If specified, only return packets from this device.

        Returns:
            List of packet dictionaries.
        """
        with self.lock:
            conditions = []
            params = []

            if node_filter:
                conditions.append('(from_id = ? OR to_id = ?)')
                params.extend([node_filter, node_filter])

            if port_filter:
                port_values = [p.strip() for p in port_filter.split(',')]
                if len(port_values) == 1:
                    conditions.append('port_name = ?')
                    params.append(port_values[0])
                else:
                    placeholders = ','.join('?' * len(port_values))
                    conditions.append(f'port_name IN ({placeholders})')
                    params.extend(port_values)

            if backend and device_id:
                # Match by device_id OR by backend (for old packets without device_id)
                conditions.append('(device_id = ? OR (device_id = "" AND backend = ?))')
                params.extend([device_id, backend])
            elif backend:
                conditions.append('backend = ?')
                params.append(backend)
            elif device_id:
                conditions.append('device_id = ?')
                params.append(device_id)

            where_clause = ' AND '.join(conditions) if conditions else '1=1'
            params.append(limit)

            self.cursor.execute(
                f'SELECT * FROM packets WHERE {where_clause} ORDER BY timestamp DESC LIMIT ?',
                params
            )
            rows = self.cursor.fetchall()

            # Convert to packet dictionaries
            packets = []
            for row in rows:
                try:
                    raw_packet = decode_raw_packet(row[5])
                    pkt_backend = row[6] if len(row) > 6 else 'meshtastic'

                    # Resolve from_name / to_name based on backend
                    if pkt_backend == 'meshcore':
                        from_name = (
                            raw_packet.get('adv_name', '')
                            or raw_packet.get('name', '')
                            or row[1]
                        )
                        to_name = row[2]
                        pkt_snr = raw_packet.get('snr') if raw_packet.get('snr') is not None else 'N/A'
                        pkt_rssi = raw_packet.get('rssi') if raw_packet.get('rssi') is not None else 'N/A'
                        pkt_hop_limit = raw_packet.get('path_len', 'N/A')
                    else:
                        from_name = raw_packet.get('fromId', row[1])
                        to_name = raw_packet.get('toId', row[2])
                        pkt_snr = raw_packet.get('rxSnr', 'N/A')
                        pkt_rssi = raw_packet.get('rxRssi', 'N/A')
                        pkt_hop_limit = raw_packet.get('hopLimit', 'N/A')

                    packet = {
                        'timestamp': row[0],
                        'from_id': row[1],
                        'to_id': row[2],
                        'port_name': row[3],
                        'payload': row[4],
                        'raw_packet': raw_packet,
                        'from_name': from_name,
                        'to_name': to_name,
                        'rssi': pkt_rssi,
                        'snr': pkt_snr,
                        'hop_limit': pkt_hop_limit,
                        'backend': pkt_backend,
                    }
                    # Extract additional fields based on port type
                    decoded = raw_packet.get('decoded', {})

                    if row[3] == 'TEXT_MESSAGE_APP':
                        packet['message'] = decoded.get('text', '')
                    elif row[3] == 'TEXT_MESSAGE':
                        # MeshCore text messages store text directly in raw_packet
                        packet['message'] = raw_packet.get('text', '') or decoded.get('text', '')
                    elif row[3] in ('POSITION_APP', 'POSITION'):
                        pos = decoded.get('position', {})
                        packet['latitude'] = pos.get('latitude', pos.get('latitudeI', 0) / 1e7 if 'latitudeI' in pos else None)
                        packet['longitude'] = pos.get('longitude', pos.get('longitudeI', 0) / 1e7 if 'longitudeI' in pos else None)
                        packet['altitude'] = pos.get('altitude', 0)
                    elif row[3] in ('NODEINFO', 'NODEINFO_APP') and pkt_backend == 'meshcore':
                        # MeshCore NODEINFO may carry coordinates
                        lat = raw_packet.get('adv_lat') or raw_packet.get('latitude')
                        lon = raw_packet.get('adv_lon') or raw_packet.get('longitude')
                        if lat and lon:
                            packet['latitude'] = lat
                            packet['longitude'] = lon
                    elif row[3] in ('TELEMETRY_APP', 'TELEMETRY'):
                        if pkt_backend == 'meshcore':
                            # MeshCore telemetry: voltage_mv directly in raw_packet
                            voltage_mv = raw_packet.get('voltage_mv', 0)
                            packet['voltage'] = voltage_mv / 1000.0 if voltage_mv else None
                        else:
                            metrics = decoded.get('telemetry', {}).get('deviceMetrics', {})
                            packet['battery_level'] = metrics.get('batteryLevel')
                            packet['voltage'] = metrics.get('voltage')
                            packet['channel_util'] = metrics.get('channelUtilization')
                            uptime = metrics.get('uptimeSeconds', 0)
                            packet['uptime_hours'] = uptime // 3600
                            packet['uptime_minutes'] = (uptime % 3600) // 60
                    packets.append(packet)
                except Exception as e:
                    logger.error(f"Error processing packet row: {e}")
                    continue

            return packets

    def lookup_node_name(self, node_id):
        """Look up a node's long name from NODEINFO packets in the database.

        Args:
            node_id: The node ID to look up (e.g., '!da567ab8' or 'mc:a1b2c3d4e5f6')

        Returns:
            The node's long name if found, otherwise the original node_id.
        """
        with self.lock:
            # Find most recent NODEINFO packet from this node (both Meshtastic and MeshCore port names)
            self.cursor.execute(
                '''SELECT raw_packet, backend FROM packets
                   WHERE from_id = ? AND port_name IN ('NODEINFO_APP', 'NODEINFO')
                   ORDER BY timestamp DESC LIMIT 1''',
                (node_id,)
            )
            row = self.cursor.fetchone()
            if row and row[0]:
                try:
                    raw_packet = decode_raw_packet(row[0])
                    pkt_backend = row[1] if len(row) > 1 else 'meshtastic'
                    if pkt_backend == 'meshcore' or node_id.startswith('mc:'):
                        # MeshCore: name stored at top level of raw_packet
                        name = raw_packet.get('adv_name', '') or raw_packet.get('name', '')
                        if name:
                            return name
                    else:
                        # Meshtastic: name stored in decoded.user.longName
                        long_name = raw_packet.get('decoded', {}).get('user', {}).get('longName')
                        if long_name:
                            return long_name
                except Exception:
                    pass
            return node_id

    def fetch_packet_stats(self, backend=None):
        """Fetch packet statistics from the database.

        Args:
            backend: If specified, only count packets from this backend.
        """
        with self.lock:
            if backend:
                self.cursor.execute('SELECT COUNT(*) FROM packets WHERE backend = ?', (backend,))
                packet_count = self.cursor.fetchone()[0]

                self.cursor.execute('SELECT COUNT(DISTINCT from_id) FROM packets WHERE backend = ?', (backend,))
                node_count = self.cursor.fetchone()[0]

                self.cursor.execute('SELECT port_name, COUNT(*) FROM packets WHERE backend = ? GROUP BY port_name', (backend,))
                port_usage = self.cursor.fetchall()
            else:
                self.cursor.execute('SELECT COUNT(*) FROM packets')
                packet_count = self.cursor.fetchone()[0]

                self.cursor.execute('SELECT COUNT(DISTINCT from_id) FROM packets')
                node_count = self.cursor.fetchone()[0]

                self.cursor.execute('SELECT port_name, COUNT(*) FROM packets GROUP BY port_name')
                port_usage = self.cursor.fetchall()

            return packet_count, node_count, port_usage

    def fetch_hourly_stats(self, backend=None):
        """Fetch hourly packet and message counts for the last 24 hours.

        Args:
            backend: If specified, only count packets from this backend.
        """
        with self.lock:
            # Initialize all 24 hours with zeros
            hourly_data = {}
            now = datetime.now()
            for i in range(24):
                hour_dt = now - timedelta(hours=i)
                hour_key = hour_dt.strftime('%Y-%m-%d %H')
                hourly_data[hour_key] = {'packets': 0, 'messages': 0}

            # Query packets grouped by hour
            if backend:
                self.cursor.execute('''
                    SELECT strftime('%Y-%m-%d %H', timestamp) as hour,
                           COUNT(*) as packet_count,
                           SUM(CASE WHEN port_name IN ('TEXT_MESSAGE_APP', 'TEXT_MESSAGE') THEN 1 ELSE 0 END) as message_count
                    FROM packets
                    WHERE timestamp >= datetime('now', '-24 hours') AND backend = ?
                    GROUP BY hour
                    ORDER BY hour DESC
                ''', (backend,))
            else:
                self.cursor.execute('''
                    SELECT strftime('%Y-%m-%d %H', timestamp) as hour,
                           COUNT(*) as packet_count,
                           SUM(CASE WHEN port_name IN ('TEXT_MESSAGE_APP', 'TEXT_MESSAGE') THEN 1 ELSE 0 END) as message_count
                    FROM packets
                    WHERE timestamp >= datetime('now', '-24 hours')
                    GROUP BY hour
                    ORDER BY hour DESC
                ''')

            for row in self.cursor.fetchall():
                hour_key = row[0]
                if hour_key in hourly_data:
                    hourly_data[hour_key] = {
                        'packets': row[1],
                        'messages': row[2]
                    }

            # Convert to ordered lists (oldest to newest)
            hours = []
            packets = []
            messages = []

            for i in range(23, -1, -1):
                hour_dt = now - timedelta(hours=i)
                hour_key = hour_dt.strftime('%Y-%m-%d %H')
                hour_label = hour_dt.strftime('%H:00')

                hours.append(hour_label)
                packets.append(hourly_data.get(hour_key, {}).get('packets', 0))
                messages.append(hourly_data.get(hour_key, {}).get('messages', 0))

            return hours, packets, messages

    # ── Route adjacency learning (v3.3.0) ─────────────────────────

    def batch_upsert_adjacency(self, rows: list[tuple[str, str, str, str]]) -> None:
        """Batch upsert adjacency observations.

        Args:
            rows: List of (node_hash, neighbor_hash, node_candidate, timestamp) tuples.
        """
        if not rows:
            return
        with self.lock:
            try:
                self.cursor.executemany(
                    '''INSERT INTO route_adjacency (node_hash, neighbor_hash, node_candidate, count, last_seen)
                       VALUES (?, ?, ?, 1, ?)
                       ON CONFLICT(node_hash, neighbor_hash, node_candidate)
                       DO UPDATE SET count = count + 1, last_seen = excluded.last_seen''',
                    rows,
                )
                self.conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Failed to upsert route adjacency: {e}")

    def load_adjacency_all(self) -> list[tuple[str, str, str, int]]:
        """Load all adjacency records for building the in-memory cache.

        Returns:
            List of (node_hash, neighbor_hash, node_candidate, count) tuples.
        """
        with self.lock:
            try:
                self.cursor.execute(
                    'SELECT node_hash, neighbor_hash, node_candidate, count '
                    'FROM route_adjacency'
                )
                return self.cursor.fetchall()
            except sqlite3.Error as e:
                logger.error(f"Failed to load route adjacency: {e}")
                return []

    # ── Network health queries (v3.11.0) ─────────────────────

    def fetch_network_health(self, backend=None):
        """Return network health metrics for the last hour."""
        with self.lock:
            try:
                where = "WHERE timestamp >= datetime('now', '-1 hours')"
                params = []
                if backend:
                    where += ' AND backend = ?'
                    params.append(backend)

                self.cursor.execute(f'SELECT COUNT(DISTINCT from_id) FROM packets {where}', params)
                nodes_last_hour = self.cursor.fetchone()[0]

                self.cursor.execute(f'SELECT COUNT(*) FROM packets {where}', params)
                packets_last_hour = self.cursor.fetchone()[0]

                self.cursor.execute(f'''
                    SELECT from_id, COUNT(*) as cnt FROM packets {where}
                    GROUP BY from_id ORDER BY cnt DESC LIMIT 1
                ''', params)
                row = self.cursor.fetchone()
                busiest_node = row[0] if row else None
                busiest_count = row[1] if row else 0

                return {
                    'nodes_last_hour': nodes_last_hour,
                    'packets_last_hour': packets_last_hour,
                    'packet_rate': round(packets_last_hour / 60, 1),
                    'busiest_node': busiest_node,
                    'busiest_count': busiest_count,
                }
            except sqlite3.Error as e:
                logger.error(f"Error fetching network health: {e}")
                return {}

    # ── Conversation queries (v3.8.0) ────────────────────────

    def fetch_conversations(self, local_node_ids: list[str] | None = None):
        """Return unique conversation threads with last message.

        A conversation is any exchange between a local node and another
        node.  Groups by the OTHER node, sorted by most recent message.
        """
        if not local_node_ids:
            return []

        with self.lock:
            try:
                placeholders = ','.join('?' * len(local_node_ids))
                # Find all messages involving our nodes as DMs (not broadcasts)
                self.cursor.execute(f'''
                    SELECT
                        CASE WHEN from_id IN ({placeholders}) THEN to_id ELSE from_id END AS other_id,
                        message,
                        timestamp,
                        backend,
                        MAX(timestamp) AS last_ts
                    FROM messages
                    WHERE (from_id IN ({placeholders}) OR to_id IN ({placeholders}))
                      AND to_id NOT IN ('^all', 'broadcast', 'all')
                      AND to_id NOT LIKE 'channel:%'
                    GROUP BY other_id
                    ORDER BY last_ts DESC
                ''', local_node_ids * 3)

                rows = self.cursor.fetchall()
                conversations = []
                for row in rows:
                    other_id = row[0]
                    # Skip if other_id is one of our own nodes
                    if other_id in local_node_ids:
                        continue
                    conversations.append({
                        'node_id': other_id,
                        'last_message': row[1] or '',
                        'timestamp': row[2] or '',
                        'backend': row[3] or '',
                    })
                return conversations
            except sqlite3.Error as e:
                logger.error(f"Error fetching conversations: {e}")
                return []

    def fetch_thread(self, node_id: str, local_node_ids: list[str] | None = None, limit: int = 50):
        """Return messages between us and a specific node, newest first."""
        if not local_node_ids:
            return []

        with self.lock:
            try:
                placeholders = ','.join('?' * len(local_node_ids))
                self.cursor.execute(f'''
                    SELECT timestamp, from_id, to_id, message, backend, device_id
                    FROM messages
                    WHERE ((from_id IN ({placeholders}) AND to_id = ?)
                        OR (from_id = ? AND to_id IN ({placeholders})))
                    ORDER BY timestamp DESC
                    LIMIT ?
                ''', [*local_node_ids, node_id, node_id, *local_node_ids, limit])

                rows = self.cursor.fetchall()
                messages = []
                for row in rows:
                    messages.append({
                        'timestamp': row[0],
                        'from_id': row[1],
                        'to_id': row[2],
                        'message': row[3],
                        'backend': row[4] if len(row) > 4 else '',
                        'device_id': row[5] if len(row) > 5 else '',
                        'is_self': row[1] in local_node_ids,
                    })
                return list(reversed(messages))  # chronological order
            except sqlite3.Error as e:
                logger.error(f"Error fetching thread: {e}")
                return []

    def fetch_channel_conversations(self):
        """Return channel threads split by backend, with count (48h) and last sender."""
        with self.lock:
            try:
                cutoff = (datetime.now() - timedelta(hours=48)).isoformat()
                self.cursor.execute('''
                    SELECT to_id, backend, COUNT(*) AS msg_count, MAX(timestamp) AS last_ts
                    FROM messages
                    WHERE to_id LIKE 'channel:%'
                      AND to_id != 'channel:'
                      AND to_id NOT GLOB 'channel:ch[0-9]'
                      AND timestamp >= ?
                    GROUP BY to_id, backend
                    ORDER BY last_ts DESC
                ''', (cutoff,))
                summary_rows = self.cursor.fetchall()
                results = []
                for row in summary_rows:
                    to_id = row[0]
                    backend = row[1] or ''
                    self.cursor.execute(
                        'SELECT message, from_id FROM messages WHERE to_id = ? AND backend = ? ORDER BY timestamp DESC LIMIT 1',
                        (to_id, backend)
                    )
                    last = self.cursor.fetchone()
                    results.append({
                        'channel_name': to_id.replace('channel:', '', 1),
                        'channel_key': to_id,
                        'backend': backend,
                        'last_message': last[0] if last else '',
                        'last_timestamp': row[3] or '',
                        'last_sender_id': last[1] if last else '',
                        'message_count': row[2],
                    })
                return results
            except sqlite3.Error as e:
                logger.error(f"Error fetching channel conversations: {e}")
                return []

    def fetch_channel_messages(self, channel_name: str, limit: int = 1000, hours: int = 48, search: str | None = None, backend: str | None = None):
        """Return messages for a specific channel, chronological order."""
        with self.lock:
            try:
                channel_key = f'channel:{channel_name}'
                cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
                params: list = [channel_key, cutoff]
                search_clause = ''
                if search:
                    search_clause = ' AND message LIKE ?'
                    params.append(f'%{search}%')
                if backend:
                    search_clause += ' AND backend = ?'
                    params.append(backend)
                params.append(limit)
                self.cursor.execute(f'''
                    SELECT timestamp, from_id, to_id, message, backend, device_id
                    FROM messages
                    WHERE to_id = ? AND timestamp >= ?{search_clause}
                    ORDER BY timestamp DESC
                    LIMIT ?
                ''', params)
                rows = self.cursor.fetchall()
                return list(reversed([{
                    'timestamp': row[0],
                    'from_id': row[1],
                    'to_id': row[2],
                    'message': row[3],
                    'backend': row[4] if len(row) > 4 else '',
                    'device_id': row[5] if len(row) > 5 else '',
                } for row in rows]))
            except sqlite3.Error as e:
                logger.error(f"Error fetching channel messages: {e}")
                return []

    def close(self):
        """Close the database connection."""
        try:
            self.conn.close()
        except sqlite3.Error as e:
            logger.error(f"Error closing database connection: {e}")
