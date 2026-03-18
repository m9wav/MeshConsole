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
Version: 3.2.0
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

        # Shared state
        self.latest_packets = []
        self.latest_packets_lock = threading.Lock()
        self.db_handler = DatabaseHandler()
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
                    backend.on_packet_received(self._handle_backend_packet)
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
                    backend.on_packet_received(self._handle_backend_packet)
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
                    backend.on_packet_received(self._handle_backend_packet)
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

        # Update in-memory cache
        with self.latest_packets_lock:
            self.latest_packets.append(packet_dict)
            self.latest_packets = self.latest_packets[-self.max_packets_memory:]

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
            mc_backend.on_packet_received(self._handle_backend_packet)
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
                    backend.on_packet_received(self._handle_backend_packet)
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
                    mc_backend.on_packet_received(self._handle_backend_packet)
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

        while True:
            try:
                while True:
                    time.sleep(1)
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
                                if hasattr(iface, 'isConnected') and not iface.isConnected:
                                    healthy = False

                                if hasattr(iface, 'socket') and iface.socket:
                                    try:
                                        error = iface.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                                        if error != 0:
                                            healthy = False
                                    except (BrokenPipeError, OSError):
                                        healthy = False

                                try:
                                    if hasattr(iface, 'nodes'):
                                        _ = len(iface.nodes)
                                except (BrokenPipeError, OSError, Exception):
                                    healthy = False

                                if not healthy:
                                    failed_backends.append(b)

                            elif b.backend_type == BackendType.MESHCORE:
                                if not b.is_connected:
                                    failed_backends.append(b)
                        except (BrokenPipeError, OSError) as health_err:
                            logger.debug(f"Health check error for {b.device_id}: {health_err}")
                            failed_backends.append(b)

                    if failed_backends:
                        # Only raise if at least one backend was supposed to be connected
                        names = [b.device_id for b in failed_backends]
                        raise ConnectionError(
                            f"Backend(s) failed health check: {', '.join(names)}"
                        )

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
                logger.info(f"Attempting to reconnect in {retry_delay} seconds...")
                time.sleep(retry_delay)

                # ── Reconnect only failed backends, leave healthy ones alone ──
                reconnected_any = False
                for b in list(self.backends):
                    # Check if this backend needs reconnection
                    needs_reconnect = False
                    if b.backend_type == BackendType.MESHTASTIC:
                        iface = getattr(b, 'interface', None)
                        if not iface or not b.is_connected:
                            needs_reconnect = True
                        elif hasattr(iface, 'isConnected') and not iface.isConnected:
                            needs_reconnect = True
                    elif not b.is_connected:
                        needs_reconnect = True

                    if not needs_reconnect:
                        continue

                    try:
                        logger.info(f"Reconnecting {b.device_id}...")
                        b.reconnect()
                        if b.backend_type == BackendType.MESHTASTIC:
                            b._sync_node_db()
                        reconnected_any = True
                        logger.info(f"Reconnected {b.device_id}")
                    except Exception as re:
                        logger.error(f"Reconnection failed for {b.device_id}: {re}")

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

    # ── Orchestrator-like helpers used by web.py ──────────────

    def resolve_node_name(self, node_id):
        """Public resolve used by web routes."""
        return self._resolve_node_name(node_id)

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
