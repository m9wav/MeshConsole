#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Meshtastic Tool
---------------
A tool for interacting with Meshtastic devices over TCP or USB.

Author: M9WAV
License: MIT
Version: 2.2.0
"""

import argparse
import configparser
import json
import logging
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

import meshtastic
import meshtastic.tcp_interface
import meshtastic.serial_interface
from meshtastic import portnums_pb2 as portnums
from pubsub import pub

# Imports for Flask
from flask import Flask, render_template, jsonify, Response, request, session
from flask_cors import CORS
from functools import wraps

# Imports for Traceroute
from meshtastic.protobuf import mesh_pb2
from google.protobuf.json_format import MessageToDict

# Set up module-level logger
logger = logging.getLogger(__name__)

# Define constants
DEFAULT_CONFIG_FILE = 'config.ini'

# Custom exceptions
class MeshtasticToolError(Exception):
    """Custom exception class for Meshtastic Tool errors."""
    pass

# Authentication helper functions
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
        # Check if authentication is enabled
        config = configparser.ConfigParser()
        config.read(DEFAULT_CONFIG_FILE)
        auth_password = config.get('Security', 'auth_password', fallback='')
        
        # If no password is set, allow access
        if not auth_password:
            return f(*args, **kwargs)
        
        # Check if user is authenticated
        if not session.get('authenticated'):
            return jsonify({'success': False, 'error': 'Authentication required', 'auth_required': True}), 401
        
        # Check if session is expired
        auth_timeout = config.getint('Security', 'auth_timeout', fallback=60)
        if 'auth_time' in session:
            auth_time = datetime.fromisoformat(session['auth_time'])
            if datetime.now() - auth_time > timedelta(minutes=auth_timeout):
                session.clear()
                return jsonify({'success': False, 'error': 'Session expired', 'auth_required': True}), 401
        
        return f(*args, **kwargs)
    return decorated_function

@dataclass
class PacketSummary:
    timestamp: str
    from_id: str
    to_id: str
    from_name: str
    to_name: str
    port_name: str
    payload: str
    message: str
    latitude: float
    longitude: float
    altitude: float
    position_time: float
    hop_limit: int
    priority: int
    rssi: float
    snr: float
    battery_level: float
    voltage: float
    channel_util: float
    air_util_tx: float
    uptime_hours: int
    uptime_minutes: int
    raw_packet: dict

class DatabaseHandler:
    """Handles database operations in a thread-safe manner."""

    def __init__(self, db_file='meshtastic_messages.db'):
        self.db_file = db_file
        self.lock = threading.Lock()
        self._setup_database()

    def _setup_database(self):
        """Set up SQLite database for message and packet logging."""
        try:
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
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

            self.conn.commit()
            logger.info("Database initialized.")
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise MeshtasticToolError("Failed to initialize the database.")

    def log_message(self, timestamp, from_id, to_id, port_name, message):
        """Log the message to the SQLite database."""
        with self.lock:
            try:
                self.cursor.execute(
                    'INSERT INTO messages VALUES (?, ?, ?, ?, ?)',
                    (timestamp, from_id, to_id, port_name, message)
                )
                self.conn.commit()
                logger.debug("Message logged to database.")
            except sqlite3.Error as e:
                logger.error(f"Failed to log message to database: {e}")

    def log_packet(self, packet_data):
        """Log the packet to the SQLite database."""
        with self.lock:
            try:
                self.cursor.execute(
                    'INSERT INTO packets VALUES (?, ?, ?, ?, ?, ?)',
                    (
                        packet_data['timestamp'],
                        packet_data['from_id'],
                        packet_data['to_id'],
                        packet_data['port_name'],
                        packet_data['payload'],
                        json.dumps(packet_data['raw_packet'])
                    )
                )
                self.conn.commit()
                logger.debug("Packet logged to database.")
            except sqlite3.Error as e:
                logger.error(f"Failed to log packet to database: {e}")

    def fetch_packets(self, hours=None):
        """Fetch packets from the database, optionally filtered by time.

        Args:
            hours: If specified, only return packets from the last N hours.
        """
        with self.lock:
            if hours:
                self.cursor.execute(
                    'SELECT * FROM packets WHERE timestamp >= datetime("now", ? || " hours") ORDER BY timestamp DESC',
                    (f'-{hours}',)
                )
            else:
                self.cursor.execute('SELECT * FROM packets ORDER BY timestamp DESC')
            return self.cursor.fetchall()

    def fetch_packets_filtered(self, node_filter=None, port_filter=None, limit=1000):
        """Fetch packets from the database with optional node and port filters.

        Args:
            node_filter: If specified, only return packets where from_id or to_id matches.
            port_filter: If specified, only return packets with matching port_name.
            limit: Maximum number of packets to return.

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
                conditions.append('port_name = ?')
                params.append(port_filter)

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
                    packet = {
                        'timestamp': row[0],
                        'from_id': row[1],
                        'to_id': row[2],
                        'port_name': row[3],
                        'payload': row[4],
                        'raw_packet': raw_packet,
                        'from_name': raw_packet.get('fromId', row[1]),
                        'to_name': raw_packet.get('toId', row[2]),
                        'rssi': raw_packet.get('rxRssi', 'N/A'),
                        'snr': raw_packet.get('rxSnr', 'N/A'),
                        'hop_limit': raw_packet.get('hopLimit', 'N/A'),
                    }
                    # Extract additional fields based on port type
                    decoded = raw_packet.get('decoded', {})
                    if row[3] == 'TEXT_MESSAGE_APP':
                        packet['message'] = decoded.get('text', '')
                    elif row[3] == 'POSITION_APP':
                        pos = decoded.get('position', {})
                        packet['latitude'] = pos.get('latitude', pos.get('latitudeI', 0) / 1e7 if 'latitudeI' in pos else None)
                        packet['longitude'] = pos.get('longitude', pos.get('longitudeI', 0) / 1e7 if 'longitudeI' in pos else None)
                        packet['altitude'] = pos.get('altitude', 0)
                    elif row[3] == 'TELEMETRY_APP':
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
            node_id: The node ID to look up (e.g., '!da567ab8')

        Returns:
            The node's long name if found, otherwise the original node_id.
        """
        with self.lock:
            # Find most recent NODEINFO packet from this node
            self.cursor.execute(
                '''SELECT raw_packet FROM packets
                   WHERE from_id = ? AND port_name = 'NODEINFO_APP'
                   ORDER BY timestamp DESC LIMIT 1''',
                (node_id,)
            )
            row = self.cursor.fetchone()
            if row and row[0]:
                try:
                    raw_packet = json.loads(row[0])
                    long_name = raw_packet.get('decoded', {}).get('user', {}).get('longName')
                    if long_name:
                        return long_name
                except Exception:
                    pass
            return node_id

    def fetch_packet_stats(self):
        """Fetch packet statistics from the database."""
        with self.lock:
            self.cursor.execute('SELECT COUNT(*) FROM packets')
            packet_count = self.cursor.fetchone()[0]

            self.cursor.execute('SELECT COUNT(DISTINCT from_id) FROM packets')
            node_count = self.cursor.fetchone()[0]

            self.cursor.execute('SELECT port_name, COUNT(*) FROM packets GROUP BY port_name')
            port_usage = self.cursor.fetchall()

            return packet_count, node_count, port_usage

    def fetch_hourly_stats(self):
        """Fetch hourly packet and message counts for the last 24 hours."""
        with self.lock:
            # Get counts per hour for last 24 hours
            # SQLite datetime format: YYYY-MM-DD HH:MM:SS
            hourly_data = {}

            # Initialize all 24 hours with zeros
            now = datetime.now()
            for i in range(24):
                hour_dt = now - timedelta(hours=i)
                hour_key = hour_dt.strftime('%Y-%m-%d %H')
                hourly_data[hour_key] = {'packets': 0, 'messages': 0}

            # Query packets grouped by hour
            self.cursor.execute('''
                SELECT strftime('%Y-%m-%d %H', timestamp) as hour,
                       COUNT(*) as packet_count,
                       SUM(CASE WHEN port_name = 'TEXT_MESSAGE_APP' THEN 1 ELSE 0 END) as message_count
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

    def close(self):
        """Close the database connection."""
        try:
            self.conn.close()
        except sqlite3.Error as e:
            logger.error(f"Error closing database connection: {e}")

class MeshtasticTool:
    """A tool for interacting with Meshtastic devices over TCP or USB."""

    def __init__(self, device_ip=None, serial_port=None, connection_type=None, sender_filter=None, config_file=DEFAULT_CONFIG_FILE, web_enabled=False, verbose=False):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Load connection type (tcp or usb)
        self.connection_type = connection_type or os.getenv('MESHTASTIC_CONNECTION_TYPE') or self.config.get('Device', 'connection_type', fallback='tcp')

        # Load configurations with environment variable support
        self.device_ip = device_ip or os.getenv('MESHTASTIC_DEVICE_IP') or self.config.get('Device', 'ip', fallback='127.0.0.1')
        self.serial_port = serial_port or os.getenv('MESHTASTIC_SERIAL_PORT') or self.config.get('Device', 'serial_port', fallback=None)
        # Handle empty string as None for serial_port
        if self.serial_port == '':
            self.serial_port = None
        self.sender_filter = sender_filter or os.getenv('MESHTASTIC_SENDER_FILTER') or self.config.get('Filter', 'sender', fallback=None)
        self.web_enabled = web_enabled or os.getenv('MESHTASTIC_WEB_ENABLED', 'False').lower() == 'true'
        self.verbose = verbose

        # Load web server config
        self.web_host = self.config.get('Web', 'host', fallback='127.0.0.1')
        self.web_port = self.config.getint('Web', 'port', fallback=5055)

        # Load database config
        self.max_packets_memory = self.config.getint('Database', 'max_packets_memory', fallback=1000)

        self.interface = None
        self.node_name_map = {}
        self.node_short_name_map = {}
        self.latest_packets = []  # Shared data for web server
        self.latest_packets_lock = threading.Lock()
        self.db_handler = DatabaseHandler()
        self.traceroute_completed = False
        self.is_traceroute_mode = False  # Flag to indicate traceroute mode
        self.local_node_id = None  # Store the local node ID for filtering
        self.traceroute_results = {}  # Store traceroute results for web interface
        self.traceroute_results_lock = threading.Lock()
        self.server_start_time = datetime.now()  # Track when the tool was started
        self.connection_start_time = None  # Track when connection was established

        # Subscribe to Meshtastic events
        pub.subscribe(self.on_receive, 'meshtastic.receive')
        pub.subscribe(self.on_connection, 'meshtastic.connection.established')
        logger.info("Meshtastic Tool initialized.")

    def _connect_interface(self):
        """Establish connection to the Meshtastic device via TCP or USB."""
        try:
            if self.connection_type.lower() == 'usb':
                if self.serial_port:
                    logger.info(f"Connecting via USB to {self.serial_port}...")
                    self.interface = meshtastic.serial_interface.SerialInterface(devPath=self.serial_port)
                else:
                    logger.info("Connecting via USB (auto-detect)...")
                    self.interface = meshtastic.serial_interface.SerialInterface()
            else:
                logger.info(f"Connecting via TCP to {self.device_ip}...")
                self.interface = meshtastic.tcp_interface.TCPInterface(hostname=self.device_ip)
            self._sync_node_db()  # Sync node database upon connection
            self.connection_start_time = datetime.now()  # Track successful connection time
        except Exception as e:
            conn_target = self.serial_port or "auto-detect" if self.connection_type.lower() == 'usb' else self.device_ip
            logger.error(f"Failed to connect to the Meshtastic device ({self.connection_type}) at {conn_target}: {e}")
            raise MeshtasticToolError("Connection to Meshtastic device failed.")

    def _sync_node_db(self):
        """Sync node database from the Meshtastic device to the local dictionary."""
        logger.info("Syncing node database from device...")
        try:
            nodes = self.interface.nodes
            # Get the local node ID from the interface
            if hasattr(self.interface, 'myInfo') and self.interface.myInfo:
                self.local_node_id = self.interface.myInfo.my_node_num
                if self.local_node_id:
                    # Convert to hex format
                    self.local_node_id = f"!{self.local_node_id:08x}"
                    logger.info(f"Detected local node ID: {self.local_node_id}")
            
            for node_id, node_info in nodes.items():
                user = node_info.get('user', {})
                long_name = user.get('longName', 'Unknown')
                short_name = user.get('shortName', '')
                self.node_name_map[node_id] = long_name
                if short_name:
                    self.node_short_name_map[node_id] = short_name
                logger.debug(f"Node {node_id} is mapped to {long_name} ({short_name})")
                
                # Alternative method to detect local node if myInfo didn't work
                if not self.local_node_id and node_info.get('num'):
                    # Check if this node has the same IP as our connection
                    # This is a fallback method
                    node_num = node_info.get('num')
                    formatted_id = f"!{node_num:08x}"
                    # We'll mark the first node we find as potentially local
                    # A more sophisticated approach would check device info
                    if not hasattr(self, '_potential_local_node'):
                        self._potential_local_node = formatted_id
                        
        except Exception as e:
            logger.error(f"Failed to sync node database: {e}")
            
        # If we still don't have local_node_id, use the potential one
        if not self.local_node_id and hasattr(self, '_potential_local_node'):
            self.local_node_id = self._potential_local_node
            logger.warning(f"Using fallback method for local node detection: {self.local_node_id}")

    def _resolve_node_name(self, node_id):
        """Resolve node ID to a friendly name if possible."""
        # First check in-memory map
        if node_id in self.node_name_map:
            return self.node_name_map[node_id]

        # Fallback: check database for NODEINFO packets from this node
        try:
            name = self.db_handler.lookup_node_name(node_id)
            if name and name != node_id:
                # Cache it for future lookups
                self.node_name_map[node_id] = name
                return name
        except Exception as e:
            logger.debug(f"Error looking up node name from DB: {e}")

        return node_id

    def _get_port_name(self, portnum):
        """Get the port name from the port number."""
        port_name = 'Unknown'
        if portnum is not None:
            if isinstance(portnum, int):
                try:
                    port_name = portnums.PortNum.Name(portnum)
                except ValueError:
                    port_name = 'Unknown'
            elif isinstance(portnum, str):
                port_name = portnum
        return port_name

    def _json_serializer(self, obj):
        """JSON serializer for objects not serializable by default."""
        if isinstance(obj, bytes):
            import base64
            return base64.b64encode(obj).decode('utf-8')
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return vars(obj)
        else:
            return str(obj)

    def on_connection(self, interface, topic=pub.AUTO_TOPIC):
        """Handle connection establishment."""
        if self.connection_type.lower() == 'usb':
            conn_info = self.serial_port or "auto-detected USB"
        else:
            conn_info = self.device_ip
        logger.info(f"Connected to {conn_info}")

    def on_receive(self, packet, interface):
        """Callback function to handle received packets."""
        from_id = self._get_node_id(packet, 'from')
        to_id = self._get_node_id(packet, 'to')

        # If this is a NODEINFO packet, update our node map directly from the packet
        decoded = packet.get('decoded', {})
        portnum = decoded.get('portnum')
        if portnum == 'NODEINFO_APP' or portnum == 4:  # 4 is the numeric value
            self._update_node_from_packet(packet)

        # Sync node database if we encounter unknown nodes
        nodes_to_check = [from_id, to_id] if to_id != 'Unknown' else [from_id]
        needs_sync = False

        for node_id in nodes_to_check:
            if node_id != 'Unknown' and node_id not in self.node_name_map:
                needs_sync = True
                break

        if needs_sync:
            logger.debug(f"Syncing node database for new nodes: {[n for n in nodes_to_check if n not in self.node_name_map]}")
            self._sync_node_db()

        # Filter out messages from our own node (automatic filtering)
        if self.local_node_id and from_id == self.local_node_id:
            logger.debug(f"Filtering out packet from local node: {from_id}")
            return

        # Filter messages if sender_filter is set (manual filtering)
        if self.sender_filter and from_id != self.sender_filter:
            return

        # Process the packet
        self.process_packet(packet)

    def process_packet(self, packet):
        """Process a received packet."""
        try:
            from_id = self._get_node_id(packet, 'from')
            to_id = self._get_node_id(packet, 'to')

            decoded = packet.get('decoded', {})
            portnum = decoded.get('portnum', None)
            port_name = self._get_port_name(portnum)
            message = decoded.get('text', '')
            payload = decoded.get('payload', '')

            # Log message if available
            timestamp = datetime.now().isoformat()
            if message:
                self.db_handler.log_message(timestamp, from_id, to_id, port_name, message)

            # Resolve node names, with fallback to packet data for NODEINFO
            from_name = self._resolve_node_name(from_id)
            to_name = self._resolve_node_name(to_id)

            # For NODEINFO packets, extract name from packet if not resolved
            if (port_name == 'NODEINFO_APP' or portnum == 4) and from_name == from_id:
                user_data = decoded.get('user', {})
                if user_data.get('longName'):
                    from_name = user_data['longName']

            # Log packet
            raw_packet_serialized = json.loads(json.dumps(packet, default=self._json_serializer))

            packet_summary = PacketSummary(
                timestamp=timestamp,
                from_id=from_id,
                to_id=to_id,
                from_name=from_name,
                to_name=to_name,
                port_name=port_name,
                payload=payload,
                message=message,
                latitude=decoded.get('position', {}).get('latitude', None),
                longitude=decoded.get('position', {}).get('longitude', None),
                altitude=decoded.get('position', {}).get('altitude', None),
                position_time=decoded.get('position', {}).get('time', None),
                hop_limit=packet.get('hopLimit', None),
                priority=packet.get('priority', None),
                rssi=packet.get('rxRssi', 'Unknown'),
                snr=packet.get('rxSnr', 'Unknown'),
                battery_level=decoded.get('telemetry', {}).get('deviceMetrics', {}).get('batteryLevel', None),
                voltage=decoded.get('telemetry', {}).get('deviceMetrics', {}).get('voltage', None),
                channel_util=decoded.get('telemetry', {}).get('deviceMetrics', {}).get('channelUtilization', None),
                air_util_tx=decoded.get('telemetry', {}).get('deviceMetrics', {}).get('airUtilTx', None),
                uptime_hours=None,
                uptime_minutes=None,
                raw_packet=raw_packet_serialized
            )

            # Calculate uptime if available
            uptime_seconds = decoded.get('telemetry', {}).get('deviceMetrics', {}).get('uptimeSeconds', None)
            if uptime_seconds is not None:
                packet_summary.uptime_hours = uptime_seconds // 3600
                packet_summary.uptime_minutes = (uptime_seconds % 3600) // 60

            # Log packet to database
            self.db_handler.log_packet(asdict(packet_summary))

            # Update latest packets for web interface
            with self.latest_packets_lock:
                self.latest_packets.append(asdict(packet_summary))
                self.latest_packets = self.latest_packets[-self.max_packets_memory:]

            # If not in traceroute mode, process traceroute response
            if not self.is_traceroute_mode and port_name == 'TRACEROUTE_APP':
                self._process_traceroute_response(packet)

            # Pretty-print the packet if verbose mode is enabled
            if self.verbose:
                self._print_message_summary(packet)
            if not self.is_traceroute_mode:
                logger.info(f"Processed packet from {from_id} to {to_id} on port {port_name}")

        except Exception as e:
            logger.error(f"Error processing packet: {e}")

    def _print_message_summary(self, packet):
        """Helper function to display packet info more clearly."""

        decoded = packet.get('decoded', {})
        portnum = decoded.get('portnum', None)
        port_name = self._get_port_name(portnum)

        print("\n" + "=" * 40)
        print("📦 New Packet:")

        from_id = self._get_node_id(packet, 'from')
        to_id = self._get_node_id(packet, 'to')

        # Resolve node IDs to names
        from_name = self._resolve_node_name(from_id)
        to_name = self._resolve_node_name(to_id)

        print(f"🔗 From: {from_name} ({from_id}) --> To: {to_name} ({to_id})")

        print(f"📬 Port: {port_name}")

        # Handle different port types
        if port_name == 'TEXT_MESSAGE_APP':
            message = decoded.get('text', '(No Text)')
            print(f"💬 Message: {message}")

        elif port_name == 'POSITION_APP':
            position = decoded.get('position', {})
            print("📍 Position Data:")
            self._print_position_info(position)

        elif port_name == 'NODEINFO_APP':
            user = decoded.get('user', {})
            print("ℹ️ Node Information:")
            self._print_node_info(user)

        elif port_name == 'TELEMETRY_APP':
            print("📊 Telemetry Data:")
            telemetry = decoded.get('telemetry', {})
            self._print_telemetry_info(telemetry)

        elif port_name == 'ENVIRONMENTAL_MEASUREMENT_APP':
            env = decoded.get('environmentalMeasurement', {})
            print("🌐 Environmental Measurements:")
            self._print_environmental_info(env)

        elif port_name == 'TRACEROUTE_APP':
            # Handle traceroute responses
            print("🛰 Traceroute Data:")
            # Traceroute data is processed elsewhere
        else:
            # For unknown or unhandled port types
            print("❓ Unknown or unhandled port type.")
            print("Decoded Data:")
            print(decoded)

        # Additional context: Hops and signal quality
        hop_limit = packet.get('hopLimit', None)
        priority = packet.get('priority', None)

        rx_metadata = packet.get('rxMetadata', {})
        rx_time = rx_metadata.get('receivedTime', None)

        if rx_time is not None:
            # If rx_time seems too large, it's likely in milliseconds
            if rx_time > 1e10:  # Arbitrary threshold for seconds vs. milliseconds
                rx_datetime = datetime.fromtimestamp(rx_time / 1000)
            else:
                rx_datetime = datetime.fromtimestamp(rx_time)
            print(f"⏲ Received Time: {rx_datetime}")
        else:
            print("⏲ Received Time: Unknown")

        if hop_limit is not None:
            print(f"🔢 Hop Limit: {hop_limit}")

        rssi = packet.get('rxRssi', 'Unknown')
        snr = packet.get('rxSnr', 'Unknown')
        print(f"📡 RSSI: {rssi} dBm | SNR: {snr} dB")

        print("=" * 40 + "\n")

    def _print_telemetry_info(self, telemetry):
        """Display telemetry information in a user-friendly way."""
        metrics = telemetry.get('deviceMetrics', {})
        battery_level = metrics.get('batteryLevel', 'Unknown')
        voltage = metrics.get('voltage', 'Unknown')
        channel_util = metrics.get('channelUtilization', 'Unknown')
        air_util_tx = metrics.get('airUtilTx', 'Unknown')
        uptime_seconds = metrics.get('uptimeSeconds', 'Unknown')

        if uptime_seconds != 'Unknown':
            uptime_hours = uptime_seconds // 3600
            uptime_minutes = (uptime_seconds % 3600) // 60
        else:
            uptime_hours = uptime_minutes = 'Unknown'

        print(f"🔋 Battery Level: {battery_level}%")
        print(f"🔌 Voltage: {voltage}V")
        print(f"📶 Channel Utilization: {channel_util}%")
        print(f"📡 Air Utilization Tx: {air_util_tx}%")
        print(f"⏲ Uptime: {uptime_hours} hours, {uptime_minutes} minutes")

    def _print_position_info(self, position):
        """Display position information."""
        latitude = position.get('latitude', 'Unknown')
        longitude = position.get('longitude', 'Unknown')
        altitude = position.get('altitude', 'Unknown')
        time_value = position.get('time', 'Unknown')

        if time_value != 'Unknown':
            time_dt = datetime.fromtimestamp(time_value)
            time_str = time_dt.isoformat()
        else:
            time_str = 'Unknown'

        print(f"🌍 Latitude: {latitude}")
        print(f"🌍 Longitude: {longitude}")
        print(f"⛰ Altitude: {altitude} meters")
        print(f"⏰ Time: {time_str}")

    def _print_node_info(self, user):
        """Display node information."""
        long_name = user.get('longName', 'Unknown')
        short_name = user.get('shortName', 'Unknown')
        macaddr = user.get('macaddr', 'Unknown')
        hw_model = user.get('hwModel', 'Unknown')

        print(f"👤 Long Name: {long_name}")
        print(f"👥 Short Name: {short_name}")
        print(f"🆔 MAC Address: {macaddr}")
        print(f"🛠 Hardware Model: {hw_model}")

    def _print_environmental_info(self, env):
        """Display environmental measurements."""
        temperature = env.get('temperature', 'Unknown')
        relative_humidity = env.get('relativeHumidity', 'Unknown')
        pressure = env.get('pressure', 'Unknown')

        print(f"🌡 Temperature: {temperature}°C")
        print(f"💧 Humidity: {relative_humidity}%")
        print(f"🔴 Pressure: {pressure} hPa")

    def send_message(self, destination_id, message):
        """Send a text message to a destination node."""
        try:
            self.interface.sendText(
                text=message,
                destinationId=destination_id,
                wantAck=True
            )
            logger.info(f"Sent message to {destination_id}: {message}")
        except Exception as e:
            logger.error(f"Failed to send message to {destination_id}: {e}")

    def send_traceroute(self, destination_id, hop_limit=10):
        """Send a traceroute request to the destination node."""
        try:
            logger.info(f"Sending traceroute request to {destination_id} with hop limit {hop_limit}")
            route_request = mesh_pb2.RouteDiscovery()
            self.interface.sendData(
                route_request,
                destinationId=destination_id,
                portNum=portnums.PortNum.TRACEROUTE_APP,
                wantResponse=True,
                hopLimit=hop_limit,
                onResponse=self._process_traceroute_response
            )
        except Exception as e:
            logger.error(f"Failed to send traceroute request: {e}")

    def _format_node_id(self, node_num: int) -> str:
        """Convert node number to the format !xxxxxxxx"""
        if node_num == 4294967295:
            return "Unknown"  # Handle broadcast address as 'Unknown'
        return f"!{node_num:08x}"

    def _get_node_id(self, packet, field='from'):
        """Get node ID from packet, handling both string and numeric formats."""
        # Try the string format first (fromId/toId)
        str_field = f"{field}Id"
        node_id = packet.get(str_field)
        if node_id and node_id != 'Unknown':
            return node_id
        # Fall back to numeric format and convert
        num_id = packet.get(field)
        if num_id is not None:
            return self._format_node_id(num_id)
        return 'Unknown'

    def _update_node_from_packet(self, packet):
        """Extract and update node info directly from NODEINFO_APP packets."""
        decoded = packet.get('decoded', {})
        user = decoded.get('user', {})

        if not user:
            return

        # Get the node ID from the packet
        from_id = self._get_node_id(packet, 'from')
        if from_id == 'Unknown':
            return

        # Extract the long name
        long_name = user.get('longName')
        if long_name:
            if from_id not in self.node_name_map or self.node_name_map[from_id] != long_name:
                self.node_name_map[from_id] = long_name
                logger.info(f"Updated node {from_id} -> {long_name}")

    def _process_traceroute_response(self, packet):
        """Process traceroute responses and display route with node IDs and SNR values."""
        try:
            decoded = packet.get('decoded', {})
            payload = decoded.get('payload', None)
            if not payload:
                logger.error("No payload found in traceroute response.")
                self.traceroute_completed = True
                return

            route_info = mesh_pb2.RouteDiscovery()
            route_info.ParseFromString(payload)
            route_dict = MessageToDict(route_info)

            snr_towards = route_dict.get("snrTowards", [])
            snr_back = route_dict.get("snrBack", [])
            route = route_dict.get("route", [])
            route_back = route_dict.get("routeBack", [])

            # Debugging Information
            logger.debug(f"Route: {route}, RouteBack: {route_back}")
            logger.debug(f"SNR Towards: {snr_towards}, SNR Back: {snr_back}")

            # Store results for web interface
            hops_towards = []
            hops_back = []
            
            # Build structured data for web interface
            if route:
                for idx, node_num in enumerate(route):
                    node_id = self._format_node_id(node_num)
                    node_name = self._resolve_node_name(node_id)
                    snr_value = round(snr_towards[idx] / 4, 2) if idx < len(snr_towards) else "N/A"
                    hops_towards.append({
                        'hop': idx + 1,
                        'id': node_id,
                        'name': node_name,
                        'snr': snr_value
                    })
            
            if route_back:
                for idx, node_num in enumerate(route_back):
                    node_id = self._format_node_id(node_num)
                    node_name = self._resolve_node_name(node_id)
                    snr_value = round(snr_back[idx] / 4, 2) if idx < len(snr_back) else "N/A"
                    hops_back.append({
                        'hop': idx + 1,
                        'id': node_id,
                        'name': node_name,
                        'snr': snr_value
                    })
            
            # Handle direct connection case
            is_direct = not route and not route_back and snr_towards and snr_back
            if is_direct:
                snr_towards_db = round(snr_towards[0] / 4, 2) if snr_towards else 'N/A'
                snr_back_db = round(snr_back[0] / 4, 2) if snr_back else 'N/A'
                
            # Store results for web interface access
            with self.traceroute_results_lock:
                self.traceroute_results = {
                    'success': True,
                    'timestamp': datetime.now().isoformat(),
                    'is_direct': is_direct,
                    'snr_towards_direct': round(snr_towards[0] / 4, 2) if is_direct and snr_towards else None,
                    'snr_back_direct': round(snr_back[0] / 4, 2) if is_direct and snr_back else None,
                    'hops_towards': hops_towards,
                    'hops_back': hops_back,
                    'total_hops': len(hops_towards) if hops_towards else (1 if is_direct else 0)
                }

            print("Traceroute result:")

            # Handle Direct Connection Case
            if is_direct:
                snr_towards_db = round(snr_towards[0] / 4, 2) if snr_towards else 'N/A'
                snr_back_db = round(snr_back[0] / 4, 2) if snr_back else 'N/A'
                print(f"Direct connection! SNR towards: {snr_towards_db} dB, SNR back: {snr_back_db} dB")
            else:
                # Print details for each hop towards the destination
                if route:
                    print("Hops towards destination:")
                    for hop_data in hops_towards:
                        print(f"  Hop {hop_data['hop']}: Node ID {hop_data['id']} ({hop_data['name']}), SNR towards {hop_data['snr']} dB")
                else:
                    print("No hops towards destination.")

                # Print details for each hop back to the origin
                if route_back:
                    print("Hops back to origin:")
                    for hop_data in hops_back:
                        print(f"  Hop {hop_data['hop']}: Node ID {hop_data['id']} ({hop_data['name']}), SNR back {hop_data['snr']} dB")
                else:
                    print("No data for hops back to origin.")

            print("Traceroute completed!")
            self.traceroute_completed = True

        except Exception as e:
            logger.error(f"Error processing traceroute: {e}")
            # Store error result for web interface
            with self.traceroute_results_lock:
                self.traceroute_results = {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
            self.traceroute_completed = True  # Ensure the flag is set even if there's an error

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
            # Start the web server in a separate thread
            web_thread = threading.Thread(target=self.start_web_server)
            web_thread.daemon = True
            web_thread.start()

        logger.info("Started listening for messages. Press Ctrl+C to exit.")
        retry_delay = 1  # Start with 1 second
        max_retry_delay = 30  # Maximum retry delay
        last_packet_time = time.time()
        connection_timeout = 60  # Consider connection dead if no packets for 60 seconds
        
        while True:
            try:
                # Main listening loop with enhanced monitoring
                while True:
                    time.sleep(1)
                    current_time = time.time()
                    
                    # Check if interface exists and is connected
                    if not self.interface:
                        raise ConnectionError("Interface is None")
                    
                    # Check for connection health using multiple indicators
                    connection_healthy = True
                    
                    # Method 1: Check isConnected if available
                    if hasattr(self.interface, 'isConnected'):
                        if not self.interface.isConnected:
                            connection_healthy = False
                            logger.warning("Interface reports disconnected")
                    
                    # Method 2: Check socket state if it's a TCP interface
                    if hasattr(self.interface, 'socket') and self.interface.socket:
                        try:
                            # Try to get socket error status
                            error = self.interface.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                            if error != 0:
                                connection_healthy = False
                                logger.warning(f"Socket error detected: {error}")
                        except Exception as e:
                            connection_healthy = False
                            logger.warning(f"Socket check failed: {e}")
                    
                    # Method 3: Check for packet timeout (no packets received recently)
                    if current_time - last_packet_time > connection_timeout:
                        # Only consider this a problem if we've been connected for a while
                        if current_time - last_packet_time > connection_timeout * 2:
                            connection_healthy = False
                            logger.warning(f"No packets received for {current_time - last_packet_time:.1f} seconds")
                    
                    # Method 4: Try a simple operation to test the connection
                    try:
                        if hasattr(self.interface, 'nodes'):
                            # This should be a lightweight operation
                            _ = len(self.interface.nodes)
                    except Exception as e:
                        connection_healthy = False
                        logger.warning(f"Interface operation failed: {e}")
                    
                    if not connection_healthy:
                        raise ConnectionError("Interface health check failed")
                    
                    # Update last packet time if we have recent packets
                    with self.latest_packets_lock:
                        if self.latest_packets:
                            latest_timestamp = self.latest_packets[-1].get('timestamp', '')
                            if latest_timestamp:
                                try:
                                    packet_time = datetime.fromisoformat(latest_timestamp).timestamp()
                                    if packet_time > last_packet_time:
                                        last_packet_time = packet_time
                                except:
                                    pass
                    
            except (ConnectionError, BrokenPipeError, OSError, socket.error, Exception) as e:
                logger.error(f"Connection lost: {e}")
                
                # Force cleanup of the broken interface and any background threads
                try:
                    if self.interface:
                        # Try to close gracefully first
                        if hasattr(self.interface, 'close'):
                            self.interface.close()
                        # Force close socket if it exists
                        if hasattr(self.interface, 'socket') and self.interface.socket:
                            try:
                                self.interface.socket.shutdown(socket.SHUT_RDWR)
                                self.interface.socket.close()
                            except:
                                pass
                except Exception as cleanup_error:
                    logger.debug(f"Error during interface cleanup: {cleanup_error}")
                
                self.interface = None
                
                # Wait a bit for background threads to die
                time.sleep(2)
                
                logger.info(f"Attempting to reconnect in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
                try:
                    # Create a completely new interface
                    logger.info("Creating new interface connection...")
                    self._connect_interface()
                    self._sync_node_db()
                    retry_delay = 1  # Reset delay after successful reconnection
                    last_packet_time = time.time()  # Reset packet timer
                    logger.info("Successfully reconnected to the device.")
                    
                except Exception as reconnect_error:
                    logger.error(f"Reconnection attempt failed: {reconnect_error}")
                    retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff
                    continue

    def _load_recent_packets_from_db(self):
        """Load recent packets from database into memory for web interface."""
        try:
            with self.latest_packets_lock:
                # Get the most recent 100 packets from database
                with self.db_handler.lock:
                    self.db_handler.cursor.execute(
                        f'SELECT * FROM packets ORDER BY timestamp DESC LIMIT {self.max_packets_memory}'
                    )
                    db_packets = self.db_handler.cursor.fetchall()
                
                # Convert database rows to packet format
                for packet_row in reversed(db_packets):  # Reverse to get chronological order
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
                            'raw_packet': json.loads(packet_row[5])
                        }
                        
                        # Extract additional data from raw packet if available
                        raw_packet = packet_data['raw_packet']
                        decoded = raw_packet.get('decoded', {})
                        
                        # Extract message
                        packet_data['message'] = decoded.get('text', '')
                        
                        # Extract position data
                        position = decoded.get('position', {})
                        if position:
                            packet_data['latitude'] = position.get('latitude')
                            packet_data['longitude'] = position.get('longitude')
                            packet_data['altitude'] = position.get('altitude')
                            packet_data['position_time'] = position.get('time')
                        
                        # Extract telemetry data
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
                        
                        # Extract signal data
                        packet_data['hop_limit'] = raw_packet.get('hopLimit')
                        packet_data['priority'] = raw_packet.get('priority')
                        packet_data['rssi'] = raw_packet.get('rxRssi', 'Unknown')
                        packet_data['snr'] = raw_packet.get('rxSnr', 'Unknown')
                        
                        self.latest_packets.append(packet_data)
                        
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Failed to parse packet from database: {e}")
                        continue
                
                # Keep only the most recent packets (based on config)
                self.latest_packets = self.latest_packets[-self.max_packets_memory:]
                logger.info(f"Loaded {len(self.latest_packets)} packets from database for web interface")
                
        except Exception as e:
            logger.error(f"Failed to load packets from database: {e}")

    def start_web_server(self):
        """Start the Flask web server."""
        # Load recent packets from database on startup
        self._load_recent_packets_from_db()
        
        # Get template directory relative to this module
        import importlib.resources
        try:
            # Python 3.9+
            template_dir = importlib.resources.files('meshconsole') / 'templates'
            app = Flask(__name__, template_folder=str(template_dir))
        except (TypeError, AttributeError):
            # Fallback for older Python or local development
            import os
            template_dir = os.path.join(os.path.dirname(__file__), 'templates')
            app = Flask(__name__, template_folder=template_dir)
        # Set secret key for sessions
        app.secret_key = secrets.token_hex(32)
        
        # Configure CORS with specific settings
        cors_enabled = self.config.getboolean('Security', 'cors_enabled', fallback=True)
        if cors_enabled:
            cors_origins = self.config.get('Security', 'cors_origins', fallback='http://localhost,http://127.0.0.1').split(',')
            CORS(app, resources={
                r"/packets": {"origins": cors_origins},
                r"/send-message": {"origins": cors_origins},
                r"/traceroute": {"origins": cors_origins},
                r"/stats": {"origins": cors_origins},
                r"/export": {"origins": cors_origins},
                r"/auth/*": {"origins": cors_origins}
            })

        @app.route('/')
        def index():
            return render_template('index.html')

        @app.route('/auth/login', methods=['POST'])
        def login():
            """Authenticate user for sensitive features."""
            try:
                data = request.get_json()
                password = data.get('password', '')
                
                auth_password = self.config.get('Security', 'auth_password', fallback='')
                
                # If no password is configured, deny access
                if not auth_password:
                    return jsonify({'success': False, 'error': 'Authentication not configured'}), 400
                
                # Check password
                if password == auth_password:
                    session['authenticated'] = True
                    session['auth_time'] = datetime.now().isoformat()
                    return jsonify({'success': True, 'message': 'Authentication successful'})
                else:
                    return jsonify({'success': False, 'error': 'Invalid password'}), 401
                    
            except Exception as e:
                logger.error(f"Authentication error: {e}")
                return jsonify({'success': False, 'error': 'Authentication failed'}), 500

        @app.route('/auth/logout', methods=['POST'])
        def logout():
            """Log out user."""
            session.clear()
            return jsonify({'success': True, 'message': 'Logged out successfully'})

        @app.route('/auth/status')
        def auth_status():
            """Check authentication status."""
            auth_password = self.config.get('Security', 'auth_password', fallback='')
            auth_required = bool(auth_password)
            
            if not auth_required:
                return jsonify({'auth_required': False, 'authenticated': True})
            
            authenticated = session.get('authenticated', False)
            
            # Check if session is expired
            if authenticated and 'auth_time' in session:
                auth_timeout = self.config.getint('Security', 'auth_timeout', fallback=60)
                auth_time = datetime.fromisoformat(session['auth_time'])
                if datetime.now() - auth_time > timedelta(minutes=auth_timeout):
                    session.clear()
                    authenticated = False
            
            return jsonify({
                'auth_required': auth_required,
                'authenticated': authenticated
            })

        @app.route('/packets')
        def get_packets():
            limit = int(request.args.get('limit', self.max_packets_memory))
            offset = int(request.args.get('offset', 0))
            port_filter = request.args.get('port_filter', '')
            node_filter = request.args.get('node_filter', '')
            unique_locations = request.args.get('unique_locations', '') == '1'

            # If filtering by node or port or unique locations, query database for more complete results
            if node_filter or port_filter or unique_locations:
                # For unique locations, force POSITION_APP filter and fetch more for deduplication
                effective_port_filter = 'POSITION_APP' if unique_locations else (port_filter or None)
                # For unique locations, fetch from config limit to find unique positions
                # For regular filtering, just fetch what we need for the current page
                db_limit = self.max_packets_memory if unique_locations else (offset + limit)
                packets = self.db_handler.fetch_packets_filtered(
                    node_filter=node_filter or None,
                    port_filter=effective_port_filter,
                    limit=db_limit
                )

                # If unique_locations, deduplicate by coordinates FIRST (before expensive name resolution)
                if unique_locations:
                    seen_locations = {}
                    for packet in packets:
                        lat = packet.get('latitude')
                        lon = packet.get('longitude')
                        if lat is not None and lon is not None:
                            # Round to 5 decimal places (~1m precision) to group nearby positions
                            location_key = (round(lat, 5), round(lon, 5))
                            if location_key not in seen_locations:
                                seen_locations[location_key] = packet
                    packets = list(seen_locations.values())

                # Resolve node names using current node database (now only for deduplicated results)
                for packet in packets:
                    packet['from_name'] = self._resolve_node_name(packet.get('from_id', ''))
                    packet['to_name'] = self._resolve_node_name(packet.get('to_id', ''))

                total_packets = len(packets)
                # Already sorted by timestamp DESC from database
                paginated_packets = packets[offset:offset + limit]
            else:
                # No filters - use in-memory cache for speed
                with self.latest_packets_lock:
                    packets = list(self.latest_packets)

                total_packets = len(packets)
                packets = packets[::-1]  # Reverse for newest first
                paginated_packets = packets[offset:offset + limit]

            try:
                response_data = {
                    'packets': paginated_packets,
                    'total': total_packets,
                    'filtered': bool(port_filter or node_filter or unique_locations)
                }
                packets_json = json.dumps(response_data, default=self._json_serializer)
            except TypeError as e:
                logger.error(f"Failed to serialize packets: {e}")
                return jsonify({'error': 'Failed to serialize packets'}), 500
            return Response(packets_json, mimetype='application/json')

        @app.route('/nodes')
        def get_nodes():
            """Get all known nodes with their info from NODEINFO packets."""
            try:
                # Query all NODEINFO packets, get latest per node
                with self.db_handler.lock:
                    # Use subquery to ensure we get raw_packet from the actual latest row
                    self.db_handler.cursor.execute('''
                        SELECT p.from_id, p.raw_packet, p.timestamp
                        FROM packets p
                        INNER JOIN (
                            SELECT from_id, MAX(timestamp) as max_ts
                            FROM packets
                            WHERE port_name = 'NODEINFO_APP'
                            GROUP BY from_id
                        ) latest ON p.from_id = latest.from_id AND p.timestamp = latest.max_ts
                        WHERE p.port_name = 'NODEINFO_APP'
                        ORDER BY p.timestamp DESC
                    ''')
                    rows = self.db_handler.cursor.fetchall()

                nodes = []
                for row in rows:
                    try:
                        node_id = row[0]
                        raw_packet = json.loads(row[1]) if row[1] else {}
                        user = raw_packet.get('decoded', {}).get('user', {})
                        db_name = user.get('longName', node_id)
                        db_short = user.get('shortName', '')
                        # Prefer in-memory names from device (more up-to-date) over DB
                        live_name = self.node_name_map.get(node_id)
                        live_short = self.node_short_name_map.get(node_id)
                        nodes.append({
                            'id': node_id,
                            'longName': live_name if live_name and live_name != node_id else db_name,
                            'shortName': live_short if live_short else db_short,
                            'hwModel': user.get('hwModel', ''),
                            'lastSeen': row[2]
                        })
                    except Exception:
                        continue

                return jsonify({'nodes': nodes})
            except Exception as e:
                logger.error(f"Error fetching nodes: {e}")
                return jsonify({'error': str(e)}), 500

        @app.route('/send-message', methods=['POST'])
        @require_auth
        def send_message_api():
            try:
                data = request.get_json()
                destination = data.get('destination')
                message = data.get('message')
                want_ack = data.get('wantAck', True)
                
                if not destination or not message:
                    return jsonify({'success': False, 'error': 'Missing destination or message'}), 400
                
                # Send the message using the existing method
                self.send_message(destination, message)
                
                return jsonify({'success': True, 'message': 'Message sent successfully'})
                
            except Exception as e:
                logger.error(f"Error sending message via API: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @app.route('/traceroute', methods=['POST'])
        @require_auth
        def traceroute_api():
            try:
                data = request.get_json()
                destination = data.get('destination')
                hop_limit = data.get('hopLimit', 10)
                
                if not destination:
                    return jsonify({'success': False, 'error': 'Missing destination'}), 400
                
                # Clear previous results
                with self.traceroute_results_lock:
                    self.traceroute_results = {}
                
                # Reset completion flag
                self.traceroute_completed = False
                
                # Start traceroute
                self.send_traceroute(destination, hop_limit)
                
                # Wait for results with timeout
                timeout = 30  # 30 seconds timeout
                start_time = time.time()
                
                while not self.traceroute_completed and (time.time() - start_time) < timeout:
                    time.sleep(0.1)  # Check every 100ms
                
                # Get results
                with self.traceroute_results_lock:
                    if self.traceroute_results:
                        return jsonify(self.traceroute_results)
                    else:
                        return jsonify({
                            'success': False,
                            'error': 'Traceroute timed out or no response received',
                            'timeout': True
                        })
                
            except Exception as e:
                logger.error(f"Error running traceroute via API: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @app.route('/traceroute-results')
        def get_traceroute_results():
            """Get the latest traceroute results."""
            try:
                with self.traceroute_results_lock:
                    if self.traceroute_results:
                        return jsonify(self.traceroute_results)
                    else:
                        return jsonify({'success': False, 'error': 'No traceroute results available'})
            except Exception as e:
                logger.error(f"Error fetching traceroute results: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @app.route('/status')
        def get_status():
            """Return server status including uptime and connection info."""
            try:
                now = datetime.now()
                server_uptime = int((now - self.server_start_time).total_seconds()) if self.server_start_time else 0
                connection_uptime = int((now - self.connection_start_time).total_seconds()) if self.connection_start_time else 0
                connected = self.interface is not None

                return jsonify({
                    'connected': connected,
                    'server_start': self.server_start_time.isoformat() if self.server_start_time else None,
                    'connection_start': self.connection_start_time.isoformat() if self.connection_start_time else None,
                    'server_uptime_seconds': server_uptime,
                    'connection_uptime_seconds': connection_uptime,
                    'local_node_id': self.local_node_id
                })
            except Exception as e:
                logger.error(f"Error fetching status: {e}")
                return jsonify({'error': str(e), 'connected': False}), 500

        @app.route('/stats')
        def get_stats():
            try:
                packet_count, node_count, port_usage = self.db_handler.fetch_packet_stats()
                hours, hourly_packets, hourly_messages = self.db_handler.fetch_hourly_stats()

                # Calculate messages today (simplified)
                today = datetime.now().date()
                messages_today = 0
                with self.latest_packets_lock:
                    for packet in self.latest_packets:
                        packet_date = datetime.fromisoformat(packet['timestamp']).date()
                        if packet_date == today and packet['port_name'] == 'TEXT_MESSAGE_APP':
                            messages_today += 1

                # Convert port usage to dictionary
                port_usage_dict = {port: count for port, count in port_usage}

                return jsonify({
                    'totalPackets': packet_count,
                    'totalNodes': node_count,
                    'messagesToday': messages_today,
                    'portUsage': port_usage_dict,
                    'hourlyData': {
                        'hours': hours,
                        'packets': hourly_packets,
                        'messages': hourly_messages
                    }
                })

            except Exception as e:
                logger.error(f"Error fetching stats via API: {e}")
                return jsonify({'error': str(e)}), 500

        @app.route('/export')
        def export_data_api():
            try:
                export_format = request.args.get('format', 'json')
                
                if export_format not in ['json', 'csv']:
                    return jsonify({'error': 'Invalid format. Use json or csv.'}), 400
                
                # Only export last 48 hours of data
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
                    
                    response_data = json.dumps(data, default=self._json_serializer, indent=2)
                    response = Response(response_data, mimetype='application/json')
                    response.headers['Content-Disposition'] = 'attachment; filename=meshtastic-data.json'
                    
                elif export_format == 'csv':
                    import io
                    import csv
                    
                    output = io.StringIO()
                    writer = csv.writer(output)
                    writer.writerow(['timestamp', 'from_id', 'to_id', 'port_name', 'payload', 'raw_packet'])
                    
                    for packet in packets:
                        writer.writerow(packet)
                    
                    response_data = output.getvalue()
                    response = Response(response_data, mimetype='text/csv')
                    response.headers['Content-Disposition'] = 'attachment; filename=meshtastic-data.csv'
                
                return response
                
            except Exception as e:
                logger.error(f"Error exporting data via API: {e}")
                return jsonify({'error': str(e)}), 500

        # Run the Flask app
        logger.info(f"Starting web server at http://{self.web_host}:{self.web_port}")
        app.run(host=self.web_host, port=self.web_port, debug=False, use_reloader=False)

    def list_nodes(self):
        """List all known nodes."""
        print("\nKnown Nodes:")
        for node_id, name in self.node_name_map.items():
            print(f"{node_id}: {name}")
        print()

    def export_data(self, export_format='json'):
        """Export data to a file (last 48 hours)."""
        filename = f"meshtastic_data.{export_format}"
        # Only export last 48 hours of data
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
            self.db_handler.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        try:
            if self.interface and self.interface.isConnected:
                self.interface.close()
        except Exception as e:
            logger.error(f"Error closing Meshtastic interface: {e}")

def configure_logging(config_file=DEFAULT_CONFIG_FILE):
    """Configure logging settings with rotation support."""
    config = configparser.ConfigParser()
    config.read(config_file)
    
    # Get logging configuration
    log_level = config.get('Logging', 'level', fallback='INFO')
    log_file = config.get('Logging', 'file', fallback='meshtastic_tool.log')
    max_size = config.getint('Logging', 'max_size', fallback=10) * 1024 * 1024  # Convert MB to bytes
    backup_count = config.getint('Logging', 'backup_count', fallback=5)
    
    # Set up rotating file handler
    from logging.handlers import RotatingFileHandler
    
    log_format = '%(asctime)s %(levelname)s [%(name)s]: %(message)s'
    
    # Create handlers
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=max_size, 
        backupCount=backup_count
    )
    console_handler = logging.StreamHandler()
    
    # Set format
    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        handlers=[file_handler, console_handler]
    )
    
    # Reduce noise from some libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)

def main():
    """Main function to parse arguments and run the Meshtastic Tool."""
    configure_logging()
    parser = argparse.ArgumentParser(
        description="Meshtastic Tool - Send and receive messages over Meshtastic devices.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--version', action='version', version='Meshtastic Tool 2.2.0')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Subparser for the 'send' command
    send_parser = subparsers.add_parser('send', help='Send a message to a node')
    send_parser.add_argument('--ip', type=str, required=False, help="IP address of the Meshtastic device (uses config.ini if not specified)")
    send_parser.add_argument('--usb', action='store_true', help="Connect via USB instead of TCP")
    send_parser.add_argument('--port', type=str, required=False, help="Serial port for USB connection (e.g., /dev/cu.usbserial-0001)")
    send_parser.add_argument('--dest', type=str, required=True, help="Destination node ID to send the message to")
    send_parser.add_argument('--message', type=str, required=True, help="Message to send")
    send_parser.add_argument('--verbose', action='store_true', help="Enable verbose output")

    # Subparser for the 'listen' command
    listen_parser = subparsers.add_parser('listen', help='Listen for incoming messages')
    listen_parser.add_argument('--ip', type=str, required=False, help="IP address of the Meshtastic device (uses config.ini if not specified)")
    listen_parser.add_argument('--usb', action='store_true', help="Connect via USB instead of TCP")
    listen_parser.add_argument('--port', type=str, required=False, help="Serial port for USB connection (e.g., /dev/cu.usbserial-0001)")
    listen_parser.add_argument('--sender', type=str, required=False, help="Sender ID to filter messages")
    listen_parser.add_argument('--web', action='store_true', help="Enable the web server")
    listen_parser.add_argument('--verbose', action='store_true', help="Enable verbose output")

    # Subparser for the 'nodes' command
    nodes_parser = subparsers.add_parser('nodes', help='List all known nodes')
    nodes_parser.add_argument('--ip', type=str, required=False, help="IP address of the Meshtastic device (uses config.ini if not specified)")
    nodes_parser.add_argument('--usb', action='store_true', help="Connect via USB instead of TCP")
    nodes_parser.add_argument('--port', type=str, required=False, help="Serial port for USB connection (e.g., /dev/cu.usbserial-0001)")

    # Subparser for the 'export' command
    export_parser = subparsers.add_parser('export', help='Export data to a file')
    export_parser.add_argument('--format', choices=['json', 'csv'], default='json', help='Export format')

    # Subparser for the 'stats' command
    stats_parser = subparsers.add_parser('stats', help='Display network statistics')

    # Subparser for the 'traceroute' command
    traceroute_parser = subparsers.add_parser('traceroute', help='Perform a traceroute to a node')
    traceroute_parser.add_argument('--ip', type=str, required=False, help="IP address of the Meshtastic device (uses config.ini if not specified)")
    traceroute_parser.add_argument('--usb', action='store_true', help="Connect via USB instead of TCP")
    traceroute_parser.add_argument('--port', type=str, required=False, help="Serial port for USB connection (e.g., /dev/cu.usbserial-0001)")
    traceroute_parser.add_argument('--dest', type=str, required=True, help="Destination node ID for traceroute")
    traceroute_parser.add_argument('--hop-limit', type=int, default=10, help="Maximum hop limit for traceroute")
    traceroute_parser.add_argument('--verbose', action='store_true', help="Enable verbose output")

    args = parser.parse_args()

    try:
        if args.command == 'send':
            conn_type = 'usb' if args.usb else None
            meshtastic_tool = MeshtasticTool(device_ip=args.ip, serial_port=args.port, connection_type=conn_type, verbose=args.verbose)
            meshtastic_tool._connect_interface()
            meshtastic_tool.send_message(destination_id=args.dest, message=args.message)
            meshtastic_tool.cleanup()
        elif args.command == 'listen':
            conn_type = 'usb' if args.usb else None
            meshtastic_tool = MeshtasticTool(device_ip=args.ip, serial_port=args.port, connection_type=conn_type, sender_filter=args.sender, web_enabled=args.web, verbose=args.verbose)
            meshtastic_tool._connect_interface()
            meshtastic_tool.start_listening()
        elif args.command == 'nodes':
            conn_type = 'usb' if args.usb else None
            meshtastic_tool = MeshtasticTool(device_ip=args.ip, serial_port=args.port, connection_type=conn_type)
            meshtastic_tool._connect_interface()
            meshtastic_tool.list_nodes()
            meshtastic_tool.cleanup()
        elif args.command == 'export':
            meshtastic_tool = MeshtasticTool()
            meshtastic_tool.export_data(export_format=args.format)
            meshtastic_tool.cleanup()
        elif args.command == 'stats':
            meshtastic_tool = MeshtasticTool()
            meshtastic_tool.display_stats()
            meshtastic_tool.cleanup()
        elif args.command == 'traceroute':
            conn_type = 'usb' if args.usb else None
            meshtastic_tool = MeshtasticTool(device_ip=args.ip, serial_port=args.port, connection_type=conn_type, verbose=args.verbose)
            meshtastic_tool.is_traceroute_mode = True  # Set traceroute mode
            meshtastic_tool._connect_interface()
            meshtastic_tool.send_traceroute(destination_id=args.dest, hop_limit=args.hop_limit)
            try:
                # Wait for a maximum of 30 seconds
                timeout = 30
                start_time = time.time()
                while True:
                    time.sleep(1)
                    if meshtastic_tool.traceroute_completed:
                        break
                    if time.time() - start_time > timeout:
                        print("Traceroute timed out.")
                        break
            except KeyboardInterrupt:
                print("Traceroute interrupted by user.")
            finally:
                meshtastic_tool.cleanup()
        else:
            parser.print_help()
    except MeshtasticToolError as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Program interrupted by user.")
        sys.exit(0)

if __name__ == '__main__':
    main()
