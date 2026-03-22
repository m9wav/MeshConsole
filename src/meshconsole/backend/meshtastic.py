"""
MeshConsole Meshtastic Backend
-------------------------------
Backend implementation for Meshtastic devices, extracted from the monolithic
MeshtasticTool class in core.py.

Handles connection management, packet processing, sending, traceroute,
and node resolution for Meshtastic hardware.

Author: M9WAV
License: MIT
"""

import json
import logging
import time
import threading
from datetime import datetime
from dataclasses import asdict
from typing import Optional, Callable

from meshconsole.models import BackendType, UnifiedPacket, UnifiedNode
from meshconsole.backend.base import MeshBackend

logger = logging.getLogger(__name__)

# ── Import guard ──────────────────────────────────────────────────
try:
    import meshtastic
    import meshtastic.tcp_interface
    import meshtastic.serial_interface
    from meshtastic import portnums_pb2 as portnums
    from meshtastic.protobuf import mesh_pb2
    from google.protobuf.json_format import MessageToDict
    from pubsub import pub
    MESHTASTIC_AVAILABLE = True
except ImportError:
    MESHTASTIC_AVAILABLE = False
    pub = None


class MeshtasticBackend(MeshBackend):
    """Meshtastic device backend using the meshtastic Python library.

    Wraps connection management, event handling (via pypubsub), packet
    processing, sending, and traceroute for Meshtastic radios.
    """

    def __init__(
        self,
        device_ip: str = '127.0.0.1',
        serial_port: str | None = None,
        connection_type: str = 'tcp',
        sender_filter: str | None = None,
        db_handler=None,
        verbose: bool = False,
        device_id: str = "",
    ):
        super().__init__()
        if not MESHTASTIC_AVAILABLE:
            raise ImportError(
                "meshtastic is required for Meshtastic support. "
                "Install it with: pip install meshconsole[meshtastic]"
            )

        if device_id:
            self._device_id = device_id

        self._device_ip = device_ip
        self._serial_port = serial_port
        self._connection_type = connection_type
        self._sender_filter = sender_filter
        self._db_handler = db_handler
        self._verbose = verbose

        self._interface = None
        self._connected = False
        self._local_node_id: str | None = None
        self._connection_start_time: datetime | None = None

        # Node name caches
        self.node_name_map: dict[str, str] = {}
        self.node_short_name_map: dict[str, str] = {}

        # Traceroute state
        self.traceroute_completed = False
        self.is_traceroute_mode = False
        self.traceroute_results: dict = {}
        self.traceroute_results_lock = threading.Lock()

        # Callbacks
        self._packet_callback: Callable | None = None
        self._connection_callback: Callable | None = None
        self._disconnection_callback: Callable | None = None

        # pubsub subscriptions are deferred to connect() so that multiple
        # MeshtasticBackend instances don't cross-fire events.
        self._subscribed = False
        logger.info("MeshtasticBackend initialized.")

    # ── MeshBackend interface ─────────────────────────────────────

    @property
    def backend_type(self) -> BackendType:
        return BackendType.MESHTASTIC

    @property
    def is_connected(self) -> bool:
        return self._connected and self._interface is not None

    @property
    def local_node_id(self) -> Optional[str]:
        return self._local_node_id

    @property
    def interface(self):
        """Direct access to the underlying meshtastic interface (for legacy compat)."""
        return self._interface

    @interface.setter
    def interface(self, value):
        self._interface = value
        self._connected = value is not None

    def connect(self) -> None:
        """Establish connection to the Meshtastic device via TCP or USB."""
        try:
            # Subscribe to pypubsub events before connecting so we capture
            # the initial connection established event.
            if not self._subscribed:
                pub.subscribe(self._on_receive, 'meshtastic.receive')
                pub.subscribe(self._on_connection, 'meshtastic.connection.established')
                self._subscribed = True

            if self._connection_type.lower() == 'usb':
                if self._serial_port:
                    logger.info(f"Connecting via USB to {self._serial_port}...")
                    self._interface = meshtastic.serial_interface.SerialInterface(devPath=self._serial_port)
                else:
                    logger.info("Connecting via USB (auto-detect)...")
                    self._interface = meshtastic.serial_interface.SerialInterface()
            else:
                logger.info(f"Connecting via TCP to {self._device_ip}...")
                self._interface = meshtastic.tcp_interface.TCPInterface(hostname=self._device_ip)

            self._connected = True
            self._sync_node_db()
            self._connection_start_time = datetime.now()

            if self._connection_callback:
                self._connection_callback()

        except Exception as e:
            conn_target = (self._serial_port or "auto-detect") if self._connection_type.lower() == 'usb' else self._device_ip
            logger.error(f"Failed to connect to the Meshtastic device ({self._connection_type}) at {conn_target}: {e}")
            raise

    def disconnect(self) -> None:
        """Cleanly disconnect from the device."""
        self._connected = False
        # Unsubscribe from pypubsub to prevent stale callbacks
        if self._subscribed:
            try:
                pub.unsubscribe(self._on_receive, 'meshtastic.receive')
                pub.unsubscribe(self._on_connection, 'meshtastic.connection.established')
            except Exception:
                pass
            self._subscribed = False
        # Close the interface, suppressing any broken pipe errors
        iface = self._interface
        self._interface = None
        if iface:
            try:
                # Kill the heartbeat timer to prevent BrokenPipeError
                if hasattr(iface, 'heartbeatTimer') and iface.heartbeatTimer:
                    iface.heartbeatTimer.cancel()
                    iface.heartbeatTimer = None
                if hasattr(iface, 'close'):
                    iface.close()
            except (BrokenPipeError, OSError, Exception) as e:
                logger.debug(f"Expected error closing Meshtastic interface: {e}")

    def reconnect(self) -> None:
        """Disconnect and reconnect to the same device."""
        logger.info(f"Reconnecting Meshtastic ({self._connection_type})...")
        self.disconnect()
        time.sleep(1)
        self.connect()

    def get_nodes(self) -> dict[str, UnifiedNode]:
        """Return all known nodes, keyed by canonical node_id."""
        nodes = {}
        for node_id, long_name in self.node_name_map.items():
            short_name = self.node_short_name_map.get(node_id, '')
            nodes[node_id] = UnifiedNode(
                node_id=node_id,
                display_name=long_name,
                short_name=short_name,
                backend=BackendType.MESHTASTIC,
                raw_data={},
            )
        return nodes

    def resolve_node_name(self, node_id: str) -> str:
        """Resolve node ID to a friendly name if possible."""
        # First check in-memory map
        if node_id in self.node_name_map:
            return self.node_name_map[node_id]

        # Fallback: check database for NODEINFO packets from this node
        if self._db_handler:
            try:
                name = self._db_handler.lookup_node_name(node_id)
                if name and name != node_id:
                    # Cache it for future lookups
                    self.node_name_map[node_id] = name
                    return name
            except Exception as e:
                logger.debug(f"Error looking up node name from DB: {e}")

        return node_id

    def send_message(self, destination: str, message: str) -> None:
        """Send a text message to a destination node."""
        try:
            self._interface.sendText(
                text=message,
                destinationId=destination,
                wantAck=True
            )
            logger.info(f"Sent message to {destination}: {message}")
        except Exception as e:
            logger.error(f"Failed to send message to {destination}: {e}")

    def send_channel_message(self, channel_idx: int, message: str) -> None:
        """Send a text message to a Meshtastic channel."""
        try:
            self._interface.sendText(
                text=message,
                channelIndex=channel_idx,
                wantAck=True
            )
            logger.info(f"Sent channel {channel_idx} message: {message}")
        except Exception as e:
            logger.error(f"Failed to send to channel {channel_idx}: {e}")
            raise

    def get_channels(self) -> list[dict]:
        """Return Meshtastic channel list."""
        channels = []
        if not self._interface:
            return channels
        try:
            node = self._interface.getNode('^local')
            if node and hasattr(node, 'channels'):
                for ch in node.channels:
                    if ch.role != 0:  # 0 = DISABLED
                        name = ch.settings.name if ch.settings.name else f'Channel {ch.index}'
                        if ch.index == 0 and not ch.settings.name:
                            name = 'Primary'
                        channels.append({'index': ch.index, 'name': name})
        except Exception as e:
            logger.debug(f"Error getting channels: {e}")
        return channels

    def send_traceroute(self, destination: str, hop_limit: int = 10) -> None:
        """Send a traceroute request to the destination node."""
        try:
            logger.info(f"Sending traceroute request to {destination} with hop limit {hop_limit}")
            route_request = mesh_pb2.RouteDiscovery()
            self._interface.sendData(
                route_request,
                destinationId=destination,
                portNum=portnums.PortNum.TRACEROUTE_APP,
                wantResponse=True,
                hopLimit=hop_limit,
                onResponse=self._process_traceroute_response
            )
        except Exception as e:
            logger.error(f"Failed to send traceroute request: {e}")

    def on_packet_received(self, callback: Callable[[UnifiedPacket], None]) -> None:
        """Register a callback for incoming packets."""
        self._packet_callback = callback

    def on_connection_established(self, callback: Callable[[], None]) -> None:
        """Register a callback for connection establishment."""
        self._connection_callback = callback

    def on_connection_lost(self, callback: Callable[[], None]) -> None:
        """Register a callback for connection loss."""
        self._disconnection_callback = callback

    # ── Internal event handlers ───────────────────────────────────

    def _on_connection(self, interface, topic=None):
        """Handle connection establishment."""
        # Guard: only process events for our own interface instance
        if interface is not self._interface:
            return
        if self._connection_type.lower() == 'usb':
            conn_info = self._serial_port or "auto-detected USB"
        else:
            conn_info = self._device_ip
        logger.info(f"Connected to {conn_info}")

    def _on_receive(self, packet, interface):
        """Callback function to handle received packets from pypubsub."""
        # Guard: only process events for our own interface instance
        if interface is not self._interface:
            return
        from_id = self._get_node_id(packet, 'from')
        to_id = self._get_node_id(packet, 'to')

        # If this is a NODEINFO packet, update our node map directly from the packet
        decoded = packet.get('decoded', {})
        portnum = decoded.get('portnum')
        if portnum == 'NODEINFO_APP' or portnum == 4:
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
        if self._local_node_id and from_id == self._local_node_id:
            logger.debug(f"Filtering out packet from local node: {from_id}")
            return

        # Filter messages if sender_filter is set (manual filtering)
        if self._sender_filter and from_id != self._sender_filter:
            return

        # Process the packet
        self._process_packet(packet)

    def _process_packet(self, packet):
        """Process a received packet and produce a UnifiedPacket."""
        try:
            from_id = self._get_node_id(packet, 'from')
            to_id = self._get_node_id(packet, 'to')

            decoded = packet.get('decoded', {})
            portnum = decoded.get('portnum', None)
            port_name = self._get_port_name(portnum)
            message = decoded.get('text', '')
            payload = decoded.get('payload', '')

            timestamp = datetime.now().isoformat()

            # Log message if available
            if message and self._db_handler:
                self._db_handler.log_message(
                    timestamp, from_id, to_id, port_name, message,
                    backend='meshtastic'
                )

            # Resolve node names
            from_name = self.resolve_node_name(from_id)
            to_name = self.resolve_node_name(to_id)

            # For NODEINFO packets, extract name from packet if not resolved
            if (port_name == 'NODEINFO_APP' or portnum == 4) and from_name == from_id:
                user_data = decoded.get('user', {})
                if user_data.get('longName'):
                    from_name = user_data['longName']

            # Serialize the raw packet
            raw_packet_serialized = json.loads(json.dumps(packet, default=self._json_serializer))

            # Build UnifiedPacket
            uptime_seconds = decoded.get('telemetry', {}).get('deviceMetrics', {}).get('uptimeSeconds', None)
            uptime_hours = None
            uptime_minutes = None
            if uptime_seconds is not None:
                uptime_hours = uptime_seconds // 3600
                uptime_minutes = (uptime_seconds % 3600) // 60

            unified = UnifiedPacket(
                timestamp=timestamp,
                from_id=from_id,
                to_id=to_id,
                from_name=from_name,
                to_name=to_name,
                port_name=port_name,
                backend=BackendType.MESHTASTIC,
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
                uptime_hours=uptime_hours,
                uptime_minutes=uptime_minutes,
                raw_packet=raw_packet_serialized,
            )

            # Fire the callback (orchestrator will handle DB logging and caching)
            if self._packet_callback:
                self._packet_callback(unified)

            # If not in traceroute mode, process traceroute response
            if not self.is_traceroute_mode and port_name == 'TRACEROUTE_APP':
                self._process_traceroute_response(packet)

            # Pretty-print the packet if verbose mode is enabled
            if self._verbose:
                self._print_message_summary(packet)
            if not self.is_traceroute_mode:
                logger.info(f"Processed packet from {from_id} to {to_id} on port {port_name}")

        except Exception as e:
            logger.error(f"Error processing packet: {e}")

    # ── Node management ───────────────────────────────────────────

    def _sync_node_db(self):
        """Sync node database from the Meshtastic device to the local dictionary."""
        if not self._interface:
            return
        logger.info("Syncing node database from device...")
        try:
            nodes = self._interface.nodes
            # Get the local node ID from the interface
            if hasattr(self._interface, 'myInfo') and self._interface.myInfo:
                self._local_node_id = self._interface.myInfo.my_node_num
                if self._local_node_id:
                    self._local_node_id = f"!{self._local_node_id:08x}"
                    logger.info(f"Detected local node ID: {self._local_node_id}")

            for node_id, node_info in nodes.items():
                user = node_info.get('user', {})
                long_name = user.get('longName', 'Unknown')
                short_name = user.get('shortName', '')
                self.node_name_map[node_id] = long_name
                if short_name:
                    self.node_short_name_map[node_id] = short_name
                logger.debug(f"Node {node_id} is mapped to {long_name} ({short_name})")

                # Alternative method to detect local node if myInfo didn't work
                if not self._local_node_id and node_info.get('num'):
                    node_num = node_info.get('num')
                    formatted_id = f"!{node_num:08x}"
                    if not hasattr(self, '_potential_local_node'):
                        self._potential_local_node = formatted_id

        except Exception as e:
            logger.error(f"Failed to sync node database: {e}")

        # If we still don't have local_node_id, use the potential one
        if not self._local_node_id and hasattr(self, '_potential_local_node'):
            self._local_node_id = self._potential_local_node
            logger.warning(f"Using fallback method for local node detection: {self._local_node_id}")

    def _update_node_from_packet(self, packet):
        """Extract and update node info directly from NODEINFO_APP packets."""
        decoded = packet.get('decoded', {})
        user = decoded.get('user', {})

        if not user:
            return

        from_id = self._get_node_id(packet, 'from')
        if from_id == 'Unknown':
            return

        long_name = user.get('longName')
        if long_name:
            if from_id not in self.node_name_map or self.node_name_map[from_id] != long_name:
                self.node_name_map[from_id] = long_name
                logger.info(f"Updated node {from_id} -> {long_name}")

    # ── Port name mapping ─────────────────────────────────────────

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

    # ── Node ID helpers ───────────────────────────────────────────

    def _format_node_id(self, node_num: int) -> str:
        """Convert node number to the format !xxxxxxxx."""
        if node_num == 4294967295:
            return "Unknown"
        return f"!{node_num:08x}"

    def _get_node_id(self, packet, field='from'):
        """Get node ID from packet, handling both string and numeric formats."""
        str_field = f"{field}Id"
        node_id = packet.get(str_field)
        if node_id and node_id != 'Unknown':
            return node_id
        num_id = packet.get(field)
        if num_id is not None:
            return self._format_node_id(num_id)
        return 'Unknown'

    # ── JSON serialization ────────────────────────────────────────

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

    # ── Traceroute processing ─────────────────────────────────────

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

            logger.debug(f"Route: {route}, RouteBack: {route_back}")
            logger.debug(f"SNR Towards: {snr_towards}, SNR Back: {snr_back}")

            hops_towards = []
            hops_back = []

            if route:
                for idx, node_num in enumerate(route):
                    node_id = self._format_node_id(node_num)
                    node_name = self.resolve_node_name(node_id)
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
                    node_name = self.resolve_node_name(node_id)
                    snr_value = round(snr_back[idx] / 4, 2) if idx < len(snr_back) else "N/A"
                    hops_back.append({
                        'hop': idx + 1,
                        'id': node_id,
                        'name': node_name,
                        'snr': snr_value
                    })

            is_direct = not route and not route_back and snr_towards and snr_back

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
            if is_direct:
                snr_towards_db = round(snr_towards[0] / 4, 2) if snr_towards else 'N/A'
                snr_back_db = round(snr_back[0] / 4, 2) if snr_back else 'N/A'
                print(f"Direct connection! SNR towards: {snr_towards_db} dB, SNR back: {snr_back_db} dB")
            else:
                if route:
                    print("Hops towards destination:")
                    for hop_data in hops_towards:
                        print(f"  Hop {hop_data['hop']}: Node ID {hop_data['id']} ({hop_data['name']}), SNR towards {hop_data['snr']} dB")
                else:
                    print("No hops towards destination.")

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
            with self.traceroute_results_lock:
                self.traceroute_results = {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
            self.traceroute_completed = True

    # ── Verbose printing ──────────────────────────────────────────

    def _print_message_summary(self, packet):
        """Helper function to display packet info more clearly."""
        decoded = packet.get('decoded', {})
        portnum = decoded.get('portnum', None)
        port_name = self._get_port_name(portnum)

        print("\n" + "=" * 40)
        print("New Packet:")

        from_id = self._get_node_id(packet, 'from')
        to_id = self._get_node_id(packet, 'to')

        from_name = self.resolve_node_name(from_id)
        to_name = self.resolve_node_name(to_id)

        print(f"From: {from_name} ({from_id}) --> To: {to_name} ({to_id})")
        print(f"Port: {port_name}")

        if port_name == 'TEXT_MESSAGE_APP':
            message = decoded.get('text', '(No Text)')
            print(f"Message: {message}")
        elif port_name == 'POSITION_APP':
            position = decoded.get('position', {})
            print("Position Data:")
            self._print_position_info(position)
        elif port_name == 'NODEINFO_APP':
            user = decoded.get('user', {})
            print("Node Information:")
            self._print_node_info(user)
        elif port_name == 'TELEMETRY_APP':
            print("Telemetry Data:")
            telemetry = decoded.get('telemetry', {})
            self._print_telemetry_info(telemetry)
        elif port_name == 'ENVIRONMENTAL_MEASUREMENT_APP':
            env = decoded.get('environmentalMeasurement', {})
            print("Environmental Measurements:")
            self._print_environmental_info(env)
        elif port_name == 'TRACEROUTE_APP':
            print("Traceroute Data:")
        else:
            print("Unknown or unhandled port type.")
            print("Decoded Data:")
            print(decoded)

        hop_limit = packet.get('hopLimit', None)
        rx_metadata = packet.get('rxMetadata', {})
        rx_time = rx_metadata.get('receivedTime', None)

        if rx_time is not None:
            if rx_time > 1e10:
                rx_datetime = datetime.fromtimestamp(rx_time / 1000)
            else:
                rx_datetime = datetime.fromtimestamp(rx_time)
            print(f"Received Time: {rx_datetime}")
        else:
            print("Received Time: Unknown")

        if hop_limit is not None:
            print(f"Hop Limit: {hop_limit}")

        rssi = packet.get('rxRssi', 'Unknown')
        snr = packet.get('rxSnr', 'Unknown')
        print(f"RSSI: {rssi} dBm | SNR: {snr} dB")
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

        print(f"Battery Level: {battery_level}%")
        print(f"Voltage: {voltage}V")
        print(f"Channel Utilization: {channel_util}%")
        print(f"Air Utilization Tx: {air_util_tx}%")
        print(f"Uptime: {uptime_hours} hours, {uptime_minutes} minutes")

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

        print(f"Latitude: {latitude}")
        print(f"Longitude: {longitude}")
        print(f"Altitude: {altitude} meters")
        print(f"Time: {time_str}")

    def _print_node_info(self, user):
        """Display node information."""
        long_name = user.get('longName', 'Unknown')
        short_name = user.get('shortName', 'Unknown')
        macaddr = user.get('macaddr', 'Unknown')
        hw_model = user.get('hwModel', 'Unknown')

        print(f"Long Name: {long_name}")
        print(f"Short Name: {short_name}")
        print(f"MAC Address: {macaddr}")
        print(f"Hardware Model: {hw_model}")

    def _print_environmental_info(self, env):
        """Display environmental measurements."""
        temperature = env.get('temperature', 'Unknown')
        relative_humidity = env.get('relativeHumidity', 'Unknown')
        pressure = env.get('pressure', 'Unknown')

        print(f"Temperature: {temperature}C")
        print(f"Humidity: {relative_humidity}%")
        print(f"Pressure: {pressure} hPa")
