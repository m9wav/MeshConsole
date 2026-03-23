"""
MeshConsole MeshCore Backend
-----------------------------
Backend implementation for MeshCore devices using the meshcore Python library.

Handles connection management (BLE, Serial, TCP), event-to-UnifiedPacket
conversion, contact/node resolution, and async-to-sync bridging for
MeshCore mesh radios.

Author: M9WAV
License: MIT
"""

import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Optional, Callable

from meshconsole.models import BackendType, ConnectionType, UnifiedPacket, UnifiedNode
from meshconsole.backend.base import MeshBackend

logger = logging.getLogger(__name__)

# ── Import guard ──────────────────────────────────────────────────
try:
    from meshcore import MeshCore, EventType
    MESHCORE_AVAILABLE = True
except ImportError:
    MESHCORE_AVAILABLE = False
    MeshCore = None
    EventType = None


class MeshCoreBackend(MeshBackend):
    """MeshCore device backend using the meshcore Python library.

    Wraps connection management, event handling, packet conversion,
    sending, and path discovery for MeshCore radios.  Owns a dedicated
    asyncio event loop running in a background thread so that the
    sync-first MeshConsole orchestrator can call into it seamlessly.
    """

    def __init__(
        self,
        connection_type: str = "ble",
        address: str = "",
        port: int | None = None,
        pin: str | None = None,
        verbose: bool = False,
        device_id: str = "",
    ):
        super().__init__()
        if not MESHCORE_AVAILABLE:
            raise ImportError(
                "meshcore is required for MeshCore support. "
                "Install it with: pip install meshconsole[meshcore]"
            )

        if device_id:
            self._device_id = device_id

        # Connection settings
        self._connection_type = ConnectionType(connection_type)
        self._address = address
        self._port = port
        self._pin = pin
        self._verbose = verbose

        # Internal state
        self._meshcore: Optional[object] = None  # MeshCore instance
        self._contacts: dict[str, dict] = {}     # pubkey_prefix -> contact dict
        self._channels: list[dict] = []           # channel info list
        self._last_rx_snr: Optional[float] = None  # SNR from most recent RX_LOG_DATA
        self._last_rx_rssi: Optional[float] = None # RSSI from most recent RX_LOG_DATA
        self._last_rx_path: str = ""               # Path from most recent RX_LOG_DATA
        self._last_rx_path_hash_size: int = 1
        self._connected = False
        self._local_node_id: str | None = None
        self._local_pub_key: str | None = None
        self._device_name: str | None = None
        self._recent_advert_emits: dict[str, float] = {}  # prefix -> timestamp

        # Asyncio event loop (owned by background thread)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._connect_event = threading.Event()
        self._connect_error: Optional[Exception] = None

        # Callbacks (registered by orchestrator)
        self._packet_callback: Callable[[UnifiedPacket], None] | None = None
        self._connection_callback: Callable[[], None] | None = None
        self._disconnection_callback: Callable[[], None] | None = None

        logger.info("MeshCoreBackend initialized.")

    # ══════════════════════════════════════════════════════════════
    # MeshBackend interface
    # ══════════════════════════════════════════════════════════════

    @property
    def backend_type(self) -> BackendType:
        return BackendType.MESHCORE

    @property
    def is_connected(self) -> bool:
        return self._connected and self._meshcore is not None

    @property
    def local_node_id(self) -> Optional[str]:
        return self._local_node_id

    def connect(self) -> None:
        """Start the asyncio event loop in a background thread and connect."""
        self._connect_event.clear()
        self._connect_error = None
        self._thread = threading.Thread(
            target=self._run_async_loop,
            name="meshcore-event-loop",
            daemon=True,
        )
        self._thread.start()

        # Wait for the async connection to complete (or fail)
        self._connect_event.wait(timeout=30)
        if self._connect_error:
            raise self._connect_error

    def disconnect(self) -> None:
        """Cleanly disconnect from the device, close the serial port, and stop the thread."""
        self._connected = False
        # Ask meshcore_py to close the serial/BLE/TCP connection
        if self._meshcore and self._loop and self._loop.is_running():
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._meshcore.disconnect(), self._loop
                )
                future.result(timeout=5)
            except Exception as e:
                logger.debug(f"Error during meshcore disconnect: {e}")
        # Stop the event loop
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._meshcore = None
        self._loop = None
        self._thread = None
        logger.info("MeshCoreBackend disconnected.")

    def get_nodes(self) -> dict[str, UnifiedNode]:
        """Convert the internal contacts cache to UnifiedNode objects."""
        nodes: dict[str, UnifiedNode] = {}
        for prefix, contact in self._contacts.items():
            node_id = f"mc:{prefix}"
            nodes[node_id] = UnifiedNode(
                node_id=node_id,
                display_name=contact.get("adv_name", "") or contact.get("name", prefix),
                short_name="",
                backend=BackendType.MESHCORE,
                public_key=contact.get("_full_pub_key") or contact.get("public_key"),
                last_seen=contact.get("last_seen"),
                latitude=contact.get("adv_lat") or contact.get("latitude"),
                longitude=contact.get("adv_lon") or contact.get("longitude"),
                raw_data=contact,
            )
        return nodes

    def resolve_node_name(self, node_id: str) -> str:
        """Resolve a node ID (mc:prefix) to the advertised name."""
        prefix = node_id.removeprefix("mc:")
        # Check if this is the local node
        if self._local_pub_key and prefix == self._local_pub_key[:12]:
            return self._device_name or prefix
        contact = self._contacts.get(prefix)
        if contact:
            return contact.get("adv_name", "") or contact.get("name", prefix) or prefix
        return prefix

    def send_message(self, destination: str, message: str) -> None:
        """Thread-safe message send via asyncio bridge.

        Passes the full contact dict when available so meshcore_py has the
        routing path for direct delivery. Falls back to hex prefix if the
        contact is unknown.
        """
        prefix = destination.removeprefix("mc:")
        # Use full contact dict if we have it (includes routing path)
        contact = self._contacts.get(prefix)
        if contact and "public_key" in contact:
            dest = contact
            logger.debug(f"Sending via contact dict (public_key={contact['public_key'][:12]}...)")
        else:
            dest = prefix
            logger.debug(f"Sending via hex prefix: {prefix}")

        future = asyncio.run_coroutine_threadsafe(
            self._meshcore.commands.send_msg_with_retry(dest, message),
            self._loop,
        )
        result = future.result(timeout=30)
        if result is None:
            logger.warning(f"Message to {destination} sent but no ACK received (may still arrive)")
        elif getattr(result, 'type', None) == EventType.ERROR:
            logger.error(f"Failed to send to {destination}: {getattr(result, 'payload', 'unknown error')}")
        else:
            logger.info(f"Message delivered to {destination}: {message}")

    def send_channel_message(self, channel_idx: int, message: str) -> None:
        """Send a message to a MeshCore channel by index."""
        if not self._meshcore or not self._loop:
            raise ConnectionError("Not connected")

        future = asyncio.run_coroutine_threadsafe(
            self._meshcore.commands.send_chan_msg(channel_idx, message),
            self._loop,
        )
        result = future.result(timeout=30)
        if result is None:
            logger.warning(f"Channel {channel_idx} message sent (no ACK)")
        elif getattr(result, 'type', None) == EventType.ERROR:
            logger.error(f"Failed to send to channel {channel_idx}: {getattr(result, 'payload', 'unknown')}")
        else:
            logger.info(f"Channel {channel_idx} message sent: {message}")

    def get_channels(self) -> list[dict]:
        """Return the channel list with index and name."""
        result = []
        for idx, ch in enumerate(self._channels):
            name = ch.get('_resolved_name', '') or f'Channel {idx}'
            result.append({'index': idx, 'name': name})
        return result

    def send_traceroute(self, destination: str, hop_limit: int = 10) -> None:
        """Initiate path discovery to the destination."""
        prefix = destination.removeprefix("mc:")
        contact = self._contacts.get(prefix)
        dest = contact if contact else prefix

        future = asyncio.run_coroutine_threadsafe(
            self._meshcore.commands.send_path_discovery(dest),
            self._loop,
        )
        result = future.result(timeout=30)
        if result and getattr(result, 'type', None) == EventType.ERROR:
            logger.error(f"Path discovery failed for {destination}: {getattr(result, 'payload', 'unknown')}")
        else:
            logger.info(f"Path discovery sent to {destination} (result: {getattr(result, 'type', 'unknown')})")
            logger.info(f"Waiting for PATH_RESPONSE event from {destination}...")

    def send_advertisement(self, flood: bool = False) -> str:
        """Send a device advertisement, optionally as a flood.

        Args:
            flood: If True, broadcast with 255-hop flood.

        Returns:
            'ok' on success, error string otherwise.
        """
        if not self._meshcore or not self._loop:
            return "Not connected"

        future = asyncio.run_coroutine_threadsafe(
            self._meshcore.commands.send_advert(flood=flood),
            self._loop,
        )
        result = future.result(timeout=15)
        if result is None:
            return "No response from device"
        if getattr(result, "type", None) == EventType.ERROR:
            return f"Error: {getattr(result, 'payload', 'unknown')}"
        logger.info(f"Advertisement sent (flood={flood}) from {self._device_name}")
        return "ok"

    def on_packet_received(self, callback: Callable[[UnifiedPacket], None]) -> None:
        """Register a callback for incoming packets."""
        self._packet_callback = callback

    def on_connection_established(self, callback: Callable[[], None]) -> None:
        """Register a callback for connection establishment."""
        self._connection_callback = callback

    def on_connection_lost(self, callback: Callable[[], None]) -> None:
        """Register a callback for connection loss."""
        self._disconnection_callback = callback

    # ══════════════════════════════════════════════════════════════
    # Async event loop (runs in background thread)
    # ══════════════════════════════════════════════════════════════

    def _run_async_loop(self):
        """Run the async event loop in a dedicated thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._async_connect())
        except Exception as exc:
            self._connect_error = exc
            self._connect_event.set()
            return
        self._connect_event.set()
        try:
            self._loop.run_forever()
        except Exception as exc:
            logger.error(f"MeshCore event loop error: {exc}")
        finally:
            self._connected = False

    async def _async_connect(self):
        """Establish the async connection to a MeshCore device."""
        if self._connection_type == ConnectionType.BLE:
            self._meshcore = await MeshCore.create_ble(
                self._address, pin=self._pin
            )
        elif self._connection_type == ConnectionType.USB:
            self._meshcore = await MeshCore.create_serial(
                self._address, 115200
            )
        elif self._connection_type == ConnectionType.TCP:
            self._meshcore = await MeshCore.create_tcp(
                self._address, self._port or 4000
            )
        else:
            raise ValueError(f"Unsupported connection type: {self._connection_type}")

        if self._meshcore is None:
            raise ConnectionError(
                f"Failed to connect to MeshCore device at {self._address}. "
                "Ensure the device is running MeshCore companion firmware "
                "and the connection type is correct."
            )

        # Subscribe to events
        self._meshcore.subscribe(EventType.CONTACT_MSG_RECV, self._on_contact_message)
        self._meshcore.subscribe(EventType.CHANNEL_MSG_RECV, self._on_channel_message)
        self._meshcore.subscribe(EventType.ADVERTISEMENT, self._on_advertisement)
        self._meshcore.subscribe(EventType.BATTERY, self._on_battery)
        self._meshcore.subscribe(EventType.TELEMETRY_RESPONSE, self._on_telemetry)
        self._meshcore.subscribe(EventType.PATH_RESPONSE, self._on_path_response)
        self._meshcore.subscribe(EventType.ACK, self._on_ack)
        self._meshcore.subscribe(EventType.DISCONNECTED, self._on_disconnected)
        self._meshcore.subscribe(EventType.STATUS_RESPONSE, self._on_status_response)
        self._meshcore.subscribe(EventType.RX_LOG_DATA, self._on_rx_log_data)
        self._meshcore.subscribe(EventType.MESSAGES_WAITING, self._on_messages_waiting)

        # Initialize device state
        self_info = await self._meshcore.commands.send_appstart()
        logger.debug(f"send_appstart result type: {getattr(self_info, 'type', None)}")
        if self_info and hasattr(self_info, "payload") and self_info.payload:
            info = self_info.payload
            # meshcore_py may use different key names — try common variants
            self._local_pub_key = (
                info.get("pub_key", "")
                or info.get("public_key", "")
                or info.get("pubkey", "")
            )
            self._device_name = (
                info.get("name", "")
                or info.get("adv_name", "")
                or info.get("device_name", "")
            )
            if self._local_pub_key:
                self._local_node_id = f"mc:{self._local_pub_key[:12]}"
            logger.info(f"Self info: name={self._device_name}, pub_key={self._local_pub_key[:12] if self._local_pub_key else 'N/A'}")

        device_info = await self._meshcore.commands.send_device_query()
        logger.debug(f"device_query result type: {getattr(device_info, 'type', None)}")

        # Set time on the device
        await self._meshcore.commands.set_time(int(time.time()))

        # Fetch contacts — payload is dict[full_pubkey_hex -> contact_dict]
        contacts_result = await self._meshcore.commands.get_contacts(lastmod=0)
        if contacts_result and hasattr(contacts_result, "payload") and contacts_result.payload:
            contacts_dict = contacts_result.payload
            if isinstance(contacts_dict, dict):
                for pub_key, contact in contacts_dict.items():
                    # meshcore_py uses 'public_key' and full hex key as dict key
                    prefix = pub_key[:12] if pub_key else ""
                    if prefix:
                        contact["_full_pub_key"] = pub_key
                        self._contacts[prefix] = contact
                        name = contact.get("adv_name", "")
                        logger.debug(f"Loaded contact: {name or prefix} ({prefix})")

        logger.info(f"Loaded {len(self._contacts)} contacts from device")

        # Fetch channels (try indices 0-7)
        self._channels = []
        for idx in range(8):
            try:
                ch_result = await self._meshcore.commands.get_channel(idx)
                if ch_result and hasattr(ch_result, "payload") and ch_result.payload:
                    ch_info = ch_result.payload
                    # meshcore_py uses 'channel_name' not 'name'
                    ch_name = ch_info.get("channel_name", "") or ch_info.get("name", "")
                    ch_info["_resolved_name"] = ch_name
                    self._channels.append(ch_info)
                    if ch_name:
                        logger.debug(f"Channel {idx}: {ch_name}")
            except Exception:
                break

        logger.info(f"Loaded {len(self._channels)} channels")

        # Start auto message fetching
        await self._meshcore.start_auto_message_fetching()

        self._connected = True
        logger.info(
            f"MeshCore connected via {self._connection_type.value} "
            f"(node: {self._local_node_id})"
        )

        if self._connection_callback:
            self._connection_callback()

    # ══════════════════════════════════════════════════════════════
    # Event handlers — convert meshcore events to UnifiedPacket
    # ══════════════════════════════════════════════════════════════

    async def _on_contact_message(self, event):
        """Convert a MeshCore contact message to a UnifiedPacket."""
        payload = event.payload
        ts = payload.get("timestamp", 0)
        timestamp = (
            datetime.fromtimestamp(ts).isoformat() if ts else datetime.now().isoformat()
        )
        pubkey_prefix = payload.get("pubkey_prefix", "unknown")

        # Enrich payload with cached RX signal data
        snr = payload.get("snr") or self._last_rx_snr
        rssi = payload.get("rssi") or self._last_rx_rssi
        enriched = dict(payload)
        if snr is not None:
            enriched["snr"] = snr
        if rssi is not None:
            enriched["rssi"] = rssi
        if self._last_rx_path:
            enriched.setdefault("path", self._last_rx_path)
            enriched.setdefault("path_hash_size", self._last_rx_path_hash_size)

        packet = UnifiedPacket(
            timestamp=timestamp,
            from_id=f"mc:{pubkey_prefix}",
            to_id=self._local_node_id or "self",
            from_name=self._resolve_contact_name(pubkey_prefix),
            to_name="Local",
            port_name="TEXT_MESSAGE",
            backend=BackendType.MESHCORE,
            message=payload.get("text", ""),
            snr=snr,
            rssi=rssi,
            hop_limit=payload.get("path_len"),
            raw_packet=enriched,
        )
        self._last_rx_snr = None
        self._last_rx_rssi = None
        self._emit_packet(packet)

    async def _on_channel_message(self, event):
        """Convert a MeshCore channel message to a UnifiedPacket."""
        payload = event.payload
        ts = payload.get("timestamp", 0)
        timestamp = (
            datetime.fromtimestamp(ts).isoformat() if ts else datetime.now().isoformat()
        )

        channel_idx = payload.get("channel_idx", 0)
        channel_name = (
            self._channels[channel_idx].get("_resolved_name", f"ch{channel_idx}")
            if channel_idx < len(self._channels)
            else f"ch{channel_idx}"
        )
        pubkey_prefix = payload.get("pubkey_prefix", "")
        text = payload.get("text", "")

        # Resolve sender name
        if pubkey_prefix:
            from_name = self._resolve_contact_name(pubkey_prefix)
            from_id = f"mc:{pubkey_prefix}"
        else:
            # Channel messages may not have sender pubkey.
            # MeshCore convention: message text is "SenderName: actual message"
            from_name = "Unknown"
            from_id = "channel"
            if ": " in text:
                embedded_name = text.split(": ", 1)[0]
                if 0 < len(embedded_name) <= 32:
                    from_name = embedded_name
                    from_id = embedded_name

        snr = payload.get("snr") or self._last_rx_snr
        rssi = payload.get("rssi") or self._last_rx_rssi
        enriched = dict(payload)
        if snr is not None:
            enriched["snr"] = snr
        if rssi is not None:
            enriched["rssi"] = rssi

        packet = UnifiedPacket(
            timestamp=timestamp,
            from_id=from_id,
            to_id=f"channel:{channel_name}",
            from_name=from_name,
            to_name=channel_name,
            port_name="TEXT_MESSAGE",
            backend=BackendType.MESHCORE,
            message=payload.get("text", ""),
            snr=snr,
            rssi=rssi,
            raw_packet=enriched,
        )
        self._last_rx_snr = None
        self._last_rx_rssi = None
        self._emit_packet(packet)

    async def _on_advertisement(self, event):
        """Convert an advertisement to a NODEINFO-equivalent UnifiedPacket.

        The ADVERTISEMENT event from meshcore_py may only contain 'public_key'.
        The richer data (name, lat, lon) arrives via RX_LOG_DATA and is stored
        in the contacts cache.  We merge both sources here.
        """
        payload = event.payload
        pub_key = (
            payload.get("public_key", "")
            or payload.get("pub_key", "")
            or payload.get("pubkey", "")
        )
        prefix = pub_key[:12] if pub_key else "unknown"

        # Skip if RX_LOG_DATA already emitted a packet for this advert recently
        last_emit = self._recent_advert_emits.get(prefix, 0)
        if time.time() - last_emit < 3:
            return

        # Look up existing contact info for richer data
        existing = self._contacts.get(prefix, {})
        adv_name = (
            payload.get("name", "")
            or payload.get("adv_name", "")
            or existing.get("adv_name", "")
            or prefix
        )
        latitude = payload.get("latitude") or payload.get("adv_lat") or existing.get("adv_lat")
        longitude = payload.get("longitude") or payload.get("adv_lon") or existing.get("adv_lon")

        # Pull SNR/RSSI from contacts cache (set by RX_LOG_DATA which fires first)
        snr = existing.get("_last_snr")
        rssi = existing.get("_last_rssi")

        # Enrich raw_packet with resolved data so it persists to DB
        enriched_raw = dict(payload)
        enriched_raw["adv_name"] = adv_name
        if pub_key:
            enriched_raw["public_key"] = pub_key
        if latitude:
            enriched_raw["adv_lat"] = latitude
        if longitude:
            enriched_raw["adv_lon"] = longitude
        if snr is not None:
            enriched_raw["snr"] = snr
        if rssi is not None:
            enriched_raw["rssi"] = rssi
        if self._last_rx_path:
            enriched_raw["path"] = self._last_rx_path
            enriched_raw["path_hash_size"] = self._last_rx_path_hash_size
            enriched_raw["path_len"] = len(self._last_rx_path) // max(1, self._last_rx_path_hash_size * 2)

        packet = UnifiedPacket(
            timestamp=datetime.now().isoformat(),
            from_id=f"mc:{prefix}",
            to_id="broadcast",
            from_name=adv_name,
            to_name="all",
            port_name="NODEINFO",
            backend=BackendType.MESHCORE,
            payload=f"Advertisement: {adv_name}",
            latitude=latitude,
            longitude=longitude,
            hop_limit=enriched_raw.get("path_len"),
            snr=snr,
            rssi=rssi,
            raw_packet=enriched_raw,
        )

        # Update contacts cache
        self._contacts[prefix] = {
            **existing,
            "pub_key_prefix": prefix,
            "adv_name": adv_name,
            "_full_pub_key": pub_key or existing.get("_full_pub_key", ""),
            "adv_lat": latitude,
            "adv_lon": longitude,
            "last_seen": datetime.now().isoformat(),
        }
        self._emit_packet(packet)

    async def _on_battery(self, event):
        """Convert a battery event to a TELEMETRY UnifiedPacket."""
        payload = event.payload
        voltage_mv = payload.get("voltage_mv", 0)
        voltage_v = voltage_mv / 1000.0 if voltage_mv else None

        packet = UnifiedPacket(
            timestamp=datetime.now().isoformat(),
            from_id=self._local_node_id or "self",
            to_id=self._local_node_id or "self",
            from_name=self._device_name or "Local",
            to_name="Local",
            port_name="TELEMETRY",
            backend=BackendType.MESHCORE,
            voltage=voltage_v,
            payload=f"Battery: {voltage_mv}mV",
            raw_packet=payload,
        )
        self._emit_packet(packet)

    async def _on_telemetry(self, event):
        """Convert a telemetry response to a TELEMETRY UnifiedPacket."""
        payload = event.payload

        packet = UnifiedPacket(
            timestamp=datetime.now().isoformat(),
            from_id=self._local_node_id or "self",
            to_id=self._local_node_id or "self",
            from_name=self._device_name or "Local",
            to_name="Local",
            port_name="TELEMETRY",
            backend=BackendType.MESHCORE,
            payload=str(payload),
            raw_packet=payload,
        )
        self._emit_packet(packet)

    async def _on_path_response(self, event):
        """Convert a path response to a TRACEROUTE UnifiedPacket."""
        payload = event.payload
        logger.info(f"PATH_RESPONSE received: {payload}")

        packet = UnifiedPacket(
            timestamp=datetime.now().isoformat(),
            from_id=self._local_node_id or "self",
            to_id=self._local_node_id or "self",
            from_name=self._device_name or "Local",
            to_name="Local",
            port_name="TRACEROUTE",
            backend=BackendType.MESHCORE,
            payload=str(payload),
            raw_packet=payload,
        )
        self._emit_packet(packet)

    async def _on_status_response(self, event):
        """Convert a status response to a TELEMETRY UnifiedPacket."""
        payload = event.payload

        packet = UnifiedPacket(
            timestamp=datetime.now().isoformat(),
            from_id=self._local_node_id or "self",
            to_id=self._local_node_id or "self",
            from_name=self._device_name or "Local",
            to_name="Local",
            port_name="TELEMETRY",
            backend=BackendType.MESHCORE,
            payload=str(payload),
            raw_packet=payload,
        )
        self._emit_packet(packet)

    async def _on_ack(self, event):
        """Convert an ACK event to a ROUTING UnifiedPacket."""
        payload = event.payload

        packet = UnifiedPacket(
            timestamp=datetime.now().isoformat(),
            from_id=self._local_node_id or "self",
            to_id=self._local_node_id or "self",
            from_name=self._device_name or "Local",
            to_name="Local",
            port_name="ROUTING",
            backend=BackendType.MESHCORE,
            payload=str(payload),
            raw_packet=payload,
        )
        self._emit_packet(packet)

    async def _on_rx_log_data(self, event):
        """Handle raw RX packet log data from MeshCore.

        This fires for every received LoRa packet with rich metadata (SNR,
        RSSI, payload type, etc.).  We use it to:
        1. Update the contacts cache with advert data (name, coords)
        2. Emit packets ONLY for types not already handled by dedicated handlers
           (CONTACT_MSG_RECV, CHANNEL_MSG_RECV, ADVERTISEMENT, etc.)
        """
        payload = event.payload
        payload_type = payload.get("payload_typename", "") or str(payload.get("payload_type", ""))
        snr = payload.get("snr")
        rssi = payload.get("rssi")

        # Store latest signal/path data so dedicated event handlers can use it
        # (RX_LOG_DATA fires before CONTACT_MSG_RECV/CHANNEL_MSG_RECV/ADVERTISEMENT)
        if snr is not None:
            self._last_rx_snr = snr
        if rssi is not None:
            self._last_rx_rssi = rssi
        self._last_rx_path = payload.get("path", "")
        self._last_rx_path_hash_size = payload.get("path_hash_size", 1)

        # Always update contacts cache from adverts (RX_LOG_DATA carries
        # adv_name, adv_key, and sometimes lat/lon + SNR/RSSI that the
        # ADVERTISEMENT event doesn't have)
        adv_name = payload.get("adv_name", "")
        adv_key = payload.get("adv_key", "")
        if adv_name and adv_key:
            prefix = adv_key[:12]
            existing = self._contacts.get(prefix, {})
            update = {
                **existing,
                "pub_key_prefix": prefix,
                "adv_name": adv_name,
                "_full_pub_key": adv_key,
                "last_seen": datetime.now().isoformat(),
            }
            # Store signal data and position from RX_LOG_DATA
            if snr is not None:
                update["_last_snr"] = snr
            if rssi is not None:
                update["_last_rssi"] = rssi
            adv_lat = payload.get("adv_lat") or payload.get("latitude")
            adv_lon = payload.get("adv_lon") or payload.get("longitude")
            if adv_lat:
                update["adv_lat"] = adv_lat
            if adv_lon:
                update["adv_lon"] = adv_lon
            self._contacts[prefix] = update

        # Skip types that have dedicated event handlers to avoid duplicates.
        # CONTACT_MSG_RECV handles TXT_MSG, CHANNEL_MSG_RECV handles GRP_TXT,
        # Dedicated handlers: CONTACT_MSG_RECV for TXT_MSG, CHANNEL_MSG_RECV
        # for GRP_TXT, _on_ack for ACK. ADVERT is NOT skipped here — the
        # _on_advertisement handler may not fire for all nodes.
        handled_types = {"TXT_MSG", "TEXT_MSG", "GRP_TXT", "ACK"}
        if payload_type in handled_types:
            return

        # Emit packets for other types (REQ, RESPONSE, TRACE, PATH, etc.)
        pkt_hash = payload.get("pkt_hash", "")
        if adv_key:
            from_prefix = adv_key[:12]
            from_name = adv_name or self._resolve_contact_name(from_prefix)
        elif payload_type in ("TRACE", "PATH"):
            # Trace/path packets are floods — we don't know the originator
            from_prefix = "mesh"
            from_name = "Mesh Network"
        else:
            from_prefix = self._local_pub_key[:12] if self._local_pub_key else "unknown"
            from_name = adv_name or self._resolve_contact_name(from_prefix)
            if not adv_name and from_name == from_prefix:
                from_name = self._device_name or "Local"

        port_map = {
            "ADVERT": "NODEINFO",
            "REQ": "ROUTING",
            "RESPONSE": "ROUTING",
            "TRACE": "TRACEROUTE",
            "PATH": "TRACEROUTE",
            "CONTROL": "CONTROL",
        }
        port_name = port_map.get(payload_type, payload_type or "RAW")

        packet = UnifiedPacket(
            timestamp=datetime.now().isoformat(),
            from_id=f"mc:{from_prefix}",
            to_id="broadcast",
            from_name=from_name,
            to_name="all",
            port_name=port_name,
            backend=BackendType.MESHCORE,
            payload=f"{payload_type}: {adv_name}" if adv_name else str(payload_type),
            snr=snr,
            rssi=rssi,
            hop_limit=payload.get("path_len"),
            raw_packet=payload,
        )
        self._emit_packet(packet)
        # Track ADVERT emits to prevent duplicate from _on_advertisement
        if payload_type == "ADVERT" and adv_key:
            self._recent_advert_emits[adv_key[:12]] = time.time()

    async def _on_messages_waiting(self, event):
        """Handle MESSAGES_WAITING — trigger message fetch."""
        logger.debug("Messages waiting on device, fetching...")
        if self._meshcore:
            try:
                await self._meshcore.commands.get_msg()
            except Exception as e:
                logger.debug(f"Error fetching messages: {e}")

    async def _on_disconnected(self, event):
        """Handle disconnection from the MeshCore device."""
        self._connected = False
        logger.warning("MeshCore device disconnected.")
        if self._disconnection_callback:
            self._disconnection_callback()

    # ══════════════════════════════════════════════════════════════
    # Internal helpers
    # ══════════════════════════════════════════════════════════════

    def _resolve_contact_name(self, pubkey_prefix: str) -> str:
        """Look up a contact's advertised name by pubkey prefix."""
        contact = self._contacts.get(pubkey_prefix)
        if contact:
            return (
                contact.get("adv_name", "")
                or contact.get("name", pubkey_prefix)
                or pubkey_prefix
            )
        return pubkey_prefix

    @staticmethod
    def _sanitize_for_json(obj):
        """Make a dict JSON-serializable by converting bytes to hex strings."""
        if isinstance(obj, dict):
            return {k: MeshCoreBackend._sanitize_for_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [MeshCoreBackend._sanitize_for_json(v) for v in obj]
        if isinstance(obj, bytes):
            return obj.hex()
        return obj

    def _emit_packet(self, packet: UnifiedPacket) -> None:
        """Fire the registered packet callback if one is set."""
        # Sanitize raw_packet to ensure JSON serialization works
        if packet.raw_packet:
            packet.raw_packet = self._sanitize_for_json(packet.raw_packet)
        if self._verbose:
            self._print_packet(packet)
        if self._packet_callback:
            self._packet_callback(packet)

    def _print_packet(self, packet: UnifiedPacket) -> None:
        """Print verbose packet summary to console."""
        parts = [
            f"[MeshCore] {packet.port_name}",
            f"{packet.from_name} ({packet.from_id})",
            "→",
            f"{packet.to_name}",
        ]
        if packet.message:
            parts.append(f": {packet.message}")
        if packet.snr is not None:
            parts.append(f" [SNR: {packet.snr}dB]")
        if packet.rssi is not None:
            parts.append(f" [RSSI: {packet.rssi}dBm]")
        if packet.latitude and packet.longitude:
            parts.append(f" [Pos: {packet.latitude:.4f},{packet.longitude:.4f}]")
        if packet.voltage:
            parts.append(f" [Batt: {packet.voltage:.2f}V]")
        print(" ".join(parts))
