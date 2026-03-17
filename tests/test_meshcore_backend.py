"""
Tests for the MeshCoreBackend implementation.

All tests mock the meshcore library so they run without hardware or the
meshcore package installed.
"""

import asyncio
import sys
import types
import threading
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

# ── Bootstrap: ensure src/ is importable ──────────────────────────
import os as _os
import sys as _sys

_project_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_src_dir = _os.path.join(_project_root, "src")
_sys.path = [p for p in _sys.path if _os.path.abspath(p) != _project_root]
if _src_dir not in _sys.path:
    _sys.path.insert(0, _src_dir)

from meshconsole.models import BackendType, ConnectionType, UnifiedPacket, UnifiedNode


# ══════════════════════════════════════════════════════════════════
# Helpers: mock meshcore module
# ══════════════════════════════════════════════════════════════════

class FakeEventType:
    """Mimic meshcore.EventType enum values."""
    CONTACT_MSG_RECV = "CONTACT_MSG_RECV"
    CHANNEL_MSG_RECV = "CHANNEL_MSG_RECV"
    ADVERTISEMENT = "ADVERTISEMENT"
    BATTERY = "BATTERY"
    TELEMETRY_RESPONSE = "TELEMETRY_RESPONSE"
    PATH_RESPONSE = "PATH_RESPONSE"
    ACK = "ACK"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    SELF_INFO = "SELF_INFO"
    MESSAGES_WAITING = "MESSAGES_WAITING"
    STATUS_RESPONSE = "STATUS_RESPONSE"


class FakeEvent:
    """A minimal event object returned by meshcore subscriptions."""
    def __init__(self, event_type, payload):
        self.type = event_type
        self.payload = payload


class FakeCommands:
    """Async stubs for meshcore commands."""
    send_appstart = AsyncMock(return_value=FakeEvent("SELF_INFO", {
        "pub_key": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
        "name": "TestDevice",
    }))
    send_device_query = AsyncMock(return_value=None)
    set_time = AsyncMock(return_value=None)
    get_contacts = AsyncMock(return_value=FakeEvent("CONTACTS", {}))
    get_channel = AsyncMock(return_value=FakeEvent("CHANNEL_INFO", {"name": "General", "index": 0}))
    send_msg = AsyncMock(return_value=FakeEvent("MSG_SENT", {}))
    send_chan_msg = AsyncMock(return_value=None)
    send_path_discovery = AsyncMock(return_value=FakeEvent("MSG_SENT", {}))
    send_trace = AsyncMock(return_value=None)
    send_statusreq = AsyncMock(return_value=None)
    get_msg = AsyncMock(return_value=None)
    send_advert = AsyncMock(return_value=None)


class FakeMeshCore:
    """Stand-in for the meshcore.MeshCore object."""
    def __init__(self):
        self.commands = FakeCommands()
        self._subscribers = {}
        self.self_pub_key = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"

    def subscribe(self, event_type, handler):
        self._subscribers.setdefault(event_type, []).append(handler)

    async def start_auto_message_fetching(self):
        pass

    @classmethod
    async def create_ble(cls, address, pin=None):
        return cls()

    @classmethod
    async def create_serial(cls, port, baud=115200):
        return cls()

    @classmethod
    async def create_tcp(cls, host, port):
        return cls()


def _install_fake_meshcore():
    """Insert a fake 'meshcore' module into sys.modules."""
    mod = types.ModuleType("meshcore")
    mod.MeshCore = FakeMeshCore
    mod.EventType = FakeEventType
    sys.modules["meshcore"] = mod
    return mod


def _uninstall_fake_meshcore():
    """Remove the fake meshcore module from sys.modules."""
    sys.modules.pop("meshcore", None)


# ══════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def _fake_meshcore():
    """Install the fake meshcore module for every test, clean up after."""
    _install_fake_meshcore()
    # Force re-import of the backend module so it picks up the fake
    mod_key = "meshconsole.backend.meshcore"
    sys.modules.pop(mod_key, None)
    yield
    sys.modules.pop(mod_key, None)
    _uninstall_fake_meshcore()


@pytest.fixture
def backend_cls():
    """Return a freshly imported MeshCoreBackend class."""
    from meshconsole.backend.meshcore import MeshCoreBackend
    return MeshCoreBackend


@pytest.fixture
def backend(backend_cls):
    """Return an unconnected MeshCoreBackend instance with BLE defaults."""
    return backend_cls(
        connection_type="ble",
        address="AA:BB:CC:DD:EE:FF",
        pin="123456",
    )


# ══════════════════════════════════════════════════════════════════
# Test: import guard
# ══════════════════════════════════════════════════════════════════

class TestImportGuard:
    """Verify MESHCORE_AVAILABLE flag and ImportError in __init__."""

    def test_available_when_meshcore_installed(self):
        from meshconsole.backend.meshcore import MESHCORE_AVAILABLE
        assert MESHCORE_AVAILABLE is True

    def test_unavailable_when_meshcore_missing(self):
        # Block the real meshcore too by inserting a failing module
        _uninstall_fake_meshcore()
        sys.modules.pop("meshconsole.backend.meshcore", None)

        # Temporarily make 'meshcore' import raise ImportError
        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "meshcore":
                raise ImportError("No module named 'meshcore'")
            return original_import(name, *args, **kwargs)

        builtins.__import__ = mock_import
        try:
            from meshconsole.backend.meshcore import MESHCORE_AVAILABLE
            assert MESHCORE_AVAILABLE is False
        finally:
            builtins.__import__ = original_import

    def test_init_raises_when_unavailable(self):
        _uninstall_fake_meshcore()
        sys.modules.pop("meshconsole.backend.meshcore", None)

        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "meshcore":
                raise ImportError("No module named 'meshcore'")
            return original_import(name, *args, **kwargs)

        builtins.__import__ = mock_import
        try:
            from meshconsole.backend.meshcore import MeshCoreBackend
            with pytest.raises(ImportError, match="meshcore is required"):
                MeshCoreBackend(connection_type="ble", address="AA:BB:CC:DD:EE:FF")
        finally:
            builtins.__import__ = original_import


# ══════════════════════════════════════════════════════════════════
# Test: properties and initial state
# ══════════════════════════════════════════════════════════════════

class TestProperties:
    """Verify backend_type, is_connected, local_node_id in initial state."""

    def test_backend_type(self, backend):
        assert backend.backend_type == BackendType.MESHCORE

    def test_not_connected_initially(self, backend):
        assert backend.is_connected is False

    def test_local_node_id_none_initially(self, backend):
        assert backend.local_node_id is None


# ══════════════════════════════════════════════════════════════════
# Test: event-to-UnifiedPacket conversion
# ══════════════════════════════════════════════════════════════════

class TestContactMessageConversion:
    """CONTACT_MSG_RECV -> UnifiedPacket with port_name TEXT_MESSAGE."""

    def test_basic_conversion(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))
        backend._local_node_id = "mc:a1b2c3d4e5f6"

        event = FakeEvent(FakeEventType.CONTACT_MSG_RECV, {
            "pubkey_prefix": "deadbeef1234",
            "path_len": 2,
            "txt_type": 0,
            "timestamp": 1710000000,
            "text": "Hello from MeshCore!",
            "snr": 8.5,
        })

        asyncio.run(backend._on_contact_message(event))

        assert len(received) == 1
        pkt = received[0]
        assert isinstance(pkt, UnifiedPacket)
        assert pkt.port_name == "TEXT_MESSAGE"
        assert pkt.from_id == "mc:deadbeef1234"
        assert pkt.to_id == "mc:a1b2c3d4e5f6"
        assert pkt.message == "Hello from MeshCore!"
        assert pkt.snr == 8.5
        assert pkt.hop_limit == 2
        assert pkt.backend == BackendType.MESHCORE

    def test_contact_name_resolved(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))
        backend._contacts["deadbeef1234"] = {"adv_name": "AliceNode"}

        event = FakeEvent(FakeEventType.CONTACT_MSG_RECV, {
            "pubkey_prefix": "deadbeef1234",
            "timestamp": 1710000000,
            "text": "Hi",
        })
        asyncio.run(backend._on_contact_message(event))

        assert received[0].from_name == "AliceNode"


class TestChannelMessageConversion:
    """CHANNEL_MSG_RECV -> UnifiedPacket with channel in to_id."""

    def test_basic_conversion(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))
        backend._channels = [{"_resolved_name": "General"}, {"_resolved_name": "Admin"}]

        event = FakeEvent(FakeEventType.CHANNEL_MSG_RECV, {
            "channel_idx": 1,
            "pubkey_prefix": "cafe1234abcd",
            "timestamp": 1710000000,
            "text": "Channel hello",
            "snr": -3.0,
        })
        asyncio.run(backend._on_channel_message(event))

        assert len(received) == 1
        pkt = received[0]
        assert pkt.port_name == "TEXT_MESSAGE"
        assert pkt.to_id == "channel:Admin"
        assert pkt.to_name == "Admin"
        assert pkt.from_id == "mc:cafe1234abcd"
        assert pkt.message == "Channel hello"

    def test_unknown_channel_index(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))
        backend._channels = []

        event = FakeEvent(FakeEventType.CHANNEL_MSG_RECV, {
            "channel_idx": 5,
            "timestamp": 1710000000,
            "text": "test",
        })
        asyncio.run(backend._on_channel_message(event))

        assert received[0].to_id == "channel:ch5"


class TestAdvertisementConversion:
    """ADVERTISEMENT -> UnifiedPacket with port_name NODEINFO."""

    def test_basic_conversion(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))

        event = FakeEvent(FakeEventType.ADVERTISEMENT, {
            "pub_key": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
            "name": "RelayNode",
            "latitude": 51.5,
            "longitude": -0.12,
        })
        asyncio.run(backend._on_advertisement(event))

        assert len(received) == 1
        pkt = received[0]
        assert pkt.port_name == "NODEINFO"
        assert pkt.from_id == "mc:a1b2c3d4e5f6"  # first 12 chars of pub_key
        assert pkt.from_name == "RelayNode"
        assert pkt.to_id == "broadcast"
        assert pkt.latitude == 51.5
        assert pkt.longitude == -0.12

    def test_contacts_cache_updated(self, backend):
        backend.on_packet_received(lambda p: None)

        event = FakeEvent(FakeEventType.ADVERTISEMENT, {
            "pub_key": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
            "name": "RelayNode",
            "latitude": 51.5,
            "longitude": -0.12,
        })
        asyncio.run(backend._on_advertisement(event))

        prefix = "a1b2c3d4e5f6"[:12]  # "a1b2c3d4e5f6"
        # The prefix used is pub_key[:12] = "a1b2c3d4e5f6"
        assert "a1b2c3d4e5f6" in backend._contacts
        contact = backend._contacts["a1b2c3d4e5f6"]
        assert contact["adv_name"] == "RelayNode"
        assert contact["adv_lat"] == 51.5


class TestBatteryConversion:
    """BATTERY -> UnifiedPacket with port_name TELEMETRY."""

    def test_basic_conversion(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))
        backend._local_node_id = "mc:a1b2c3d4e5f6"
        backend._device_name = "TestDevice"

        event = FakeEvent(FakeEventType.BATTERY, {
            "voltage_mv": 3700,
            "storage_kb": 128,
        })
        asyncio.run(backend._on_battery(event))

        assert len(received) == 1
        pkt = received[0]
        assert pkt.port_name == "TELEMETRY"
        assert pkt.voltage == 3.7
        assert pkt.from_name == "TestDevice"
        assert pkt.backend == BackendType.MESHCORE


class TestPathResponseConversion:
    """PATH_RESPONSE -> UnifiedPacket with port_name TRACEROUTE."""

    def test_basic_conversion(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))
        backend._local_node_id = "mc:a1b2c3d4e5f6"

        event = FakeEvent(FakeEventType.PATH_RESPONSE, {
            "hops": ["node1", "node2", "node3"],
        })
        asyncio.run(backend._on_path_response(event))

        assert len(received) == 1
        pkt = received[0]
        assert pkt.port_name == "TRACEROUTE"
        assert pkt.backend == BackendType.MESHCORE


class TestStatusResponseConversion:
    """STATUS_RESPONSE -> UnifiedPacket with port_name TELEMETRY."""

    def test_basic_conversion(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))
        backend._local_node_id = "mc:a1b2c3d4e5f6"

        event = FakeEvent(FakeEventType.STATUS_RESPONSE, {
            "uptime": 3600,
            "free_mem": 1024,
        })
        asyncio.run(backend._on_status_response(event))

        assert len(received) == 1
        pkt = received[0]
        assert pkt.port_name == "TELEMETRY"


class TestAckConversion:
    """ACK -> UnifiedPacket with port_name ROUTING."""

    def test_basic_conversion(self, backend):
        received = []
        backend.on_packet_received(lambda p: received.append(p))
        backend._local_node_id = "mc:a1b2c3d4e5f6"

        event = FakeEvent(FakeEventType.ACK, {
            "expected_ack": 42,
        })
        asyncio.run(backend._on_ack(event))

        assert len(received) == 1
        pkt = received[0]
        assert pkt.port_name == "ROUTING"


class TestDisconnectedEvent:
    """DISCONNECTED event sets _connected = False and fires callback."""

    def test_disconnected(self, backend):
        backend._connected = True
        callback_called = []
        backend.on_connection_lost(lambda: callback_called.append(True))

        asyncio.run(backend._on_disconnected(
            FakeEvent(FakeEventType.DISCONNECTED, {})
        ))

        assert backend._connected is False
        assert callback_called == [True]


# ══════════════════════════════════════════════════════════════════
# Test: node management
# ══════════════════════════════════════════════════════════════════

class TestNodeManagement:
    """Verify get_nodes() and resolve_node_name()."""

    def test_get_nodes_empty(self, backend):
        nodes = backend.get_nodes()
        assert nodes == {}

    def test_get_nodes_with_contacts(self, backend):
        backend._contacts = {
            "aabbccdd1234": {
                "adv_name": "AliceNode",
                "pub_key": "aabbccdd1234aabbccdd1234aabbccdd",
                "adv_lat": 40.7,
                "adv_lon": -74.0,
            },
            "11223344abcd": {
                "adv_name": "BobNode",
                "pub_key": "11223344abcd11223344abcd11223344",
            },
        }

        nodes = backend.get_nodes()
        assert len(nodes) == 2
        assert "mc:aabbccdd1234" in nodes
        assert "mc:11223344abcd" in nodes

        alice = nodes["mc:aabbccdd1234"]
        assert isinstance(alice, UnifiedNode)
        assert alice.display_name == "AliceNode"
        assert alice.backend == BackendType.MESHCORE
        assert alice.latitude == 40.7
        assert alice.longitude == -74.0
        assert alice.short_name == ""

    def test_resolve_node_name_known(self, backend):
        backend._contacts = {
            "aabbccdd1234": {"adv_name": "AliceNode"},
        }
        assert backend.resolve_node_name("mc:aabbccdd1234") == "AliceNode"

    def test_resolve_node_name_unknown(self, backend):
        assert backend.resolve_node_name("mc:unknownprefix") == "unknownprefix"

    def test_resolve_node_name_strips_prefix(self, backend):
        backend._contacts = {
            "aabbccdd1234": {"adv_name": "AliceNode"},
        }
        # Should also work if passed without mc: prefix
        assert backend.resolve_node_name("aabbccdd1234") == "AliceNode"

    def test_resolve_contact_name_fallback(self, backend):
        backend._contacts = {
            "noname": {"pub_key": "noname_full"},
        }
        # Contact with no adv_name or name -> falls back to prefix
        assert backend._resolve_contact_name("noname") == "noname"


# ══════════════════════════════════════════════════════════════════
# Test: send operations (sync-to-async bridge)
# ══════════════════════════════════════════════════════════════════

class TestSendOperations:
    """Verify send_message and send_traceroute strip mc: and bridge async."""

    def test_send_message_strips_prefix(self, backend):
        loop = asyncio.new_event_loop()
        backend._loop = loop
        mock_mc = FakeMeshCore()
        mock_mc.commands.send_msg_with_retry = AsyncMock(return_value=None)
        backend._meshcore = mock_mc

        # Run the loop in a thread so run_coroutine_threadsafe works
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()

        try:
            backend.send_message("mc:deadbeef1234", "Hello!")
            mock_mc.commands.send_msg_with_retry.assert_awaited_once_with("deadbeef1234", "Hello!")
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            loop.close()

    def test_send_traceroute_strips_prefix(self, backend):
        loop = asyncio.new_event_loop()
        backend._loop = loop
        mock_mc = FakeMeshCore()
        mock_mc.commands.send_path_discovery = AsyncMock(return_value=None)
        backend._meshcore = mock_mc

        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()

        try:
            backend.send_traceroute("mc:deadbeef1234", hop_limit=5)
            mock_mc.commands.send_path_discovery.assert_awaited_once_with("deadbeef1234")
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            loop.close()


# ══════════════════════════════════════════════════════════════════
# Test: callback registration
# ══════════════════════════════════════════════════════════════════

class TestCallbackRegistration:
    """Verify on_packet_received, on_connection_established, on_connection_lost."""

    def test_on_packet_received(self, backend):
        cb = lambda p: None
        backend.on_packet_received(cb)
        assert backend._packet_callback is cb

    def test_on_connection_established(self, backend):
        cb = lambda: None
        backend.on_connection_established(cb)
        assert backend._connection_callback is cb

    def test_on_connection_lost(self, backend):
        cb = lambda: None
        backend.on_connection_lost(cb)
        assert backend._disconnection_callback is cb

    def test_emit_packet_no_callback(self, backend):
        # Should not raise even without a callback
        pkt = UnifiedPacket(
            timestamp="2026-03-17T12:00:00",
            from_id="mc:test",
            to_id="mc:test",
            from_name="Test",
            to_name="Test",
            port_name="TEXT_MESSAGE",
            backend=BackendType.MESHCORE,
        )
        backend._emit_packet(pkt)  # no error


# ══════════════════════════════════════════════════════════════════
# Test: disconnect
# ══════════════════════════════════════════════════════════════════

class TestDisconnect:
    """Verify disconnect cleans up state."""

    def test_disconnect_clears_state(self, backend):
        backend._connected = True
        backend._meshcore = FakeMeshCore()
        backend._loop = asyncio.new_event_loop()
        # Don't start the loop, just test cleanup
        backend._loop.close()
        backend._loop = None
        backend._thread = None

        backend.disconnect()

        assert backend.is_connected is False
        assert backend._meshcore is None
        assert backend._loop is None
        assert backend._thread is None
