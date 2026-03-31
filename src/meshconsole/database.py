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
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


class DatabaseHandler:
    """Handles database operations in a thread-safe manner."""

    def __init__(self, db_file='meshtastic_messages.db'):
        self.db_file = db_file
        self.lock = threading.Lock()
        self._setup_database()
        self._migrate_backend_column()
        self._migrate_device_id_column()

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

    def log_message(self, timestamp, from_id, to_id, port_name, message, backend='meshtastic', device_id=''):
        """Log the message to the SQLite database."""
        with self.lock:
            try:
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
                        json.dumps(d.get('raw_packet', {})),
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
                    raw_packet = json.loads(row[5]) if row[5] else {}
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
                    raw_packet = json.loads(row[0])
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

    def close(self):
        """Close the database connection."""
        try:
            self.conn.close()
        except sqlite3.Error as e:
            logger.error(f"Error closing database connection: {e}")
