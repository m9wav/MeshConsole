"""
Tests for UnifiedPacket, UnifiedNode, and enum serialization.
"""

from dataclasses import asdict

import pytest

from meshconsole.models import (
    BackendType,
    ConnectionType,
    UnifiedPacket,
    UnifiedNode,
    PacketSummary,
)
from tests.conftest import make_unified_packet, make_unified_node


class TestBackendType:
    """Verify BackendType enum."""

    def test_values(self):
        assert BackendType.MESHTASTIC.value == "meshtastic"
        assert BackendType.MESHCORE.value == "meshcore"

    def test_from_string(self):
        assert BackendType("meshtastic") is BackendType.MESHTASTIC
        assert BackendType("meshcore") is BackendType.MESHCORE


class TestConnectionType:
    """Verify ConnectionType enum."""

    def test_values(self):
        assert ConnectionType.TCP.value == "tcp"
        assert ConnectionType.USB.value == "usb"
        assert ConnectionType.BLE.value == "ble"


class TestUnifiedPacket:
    """Verify UnifiedPacket creation and serialization."""

    def test_create_default(self):
        pkt = make_unified_packet()
        assert pkt.from_id == "!aabbccdd"
        assert pkt.backend == BackendType.MESHTASTIC

    def test_to_dict(self):
        pkt = make_unified_packet()
        d = pkt.to_dict()
        assert d['backend'] == 'meshtastic'
        assert d['from_id'] == '!aabbccdd'
        assert isinstance(d, dict)

    def test_asdict(self):
        pkt = make_unified_packet()
        d = asdict(pkt)
        # asdict preserves the enum object
        assert d['backend'] == BackendType.MESHTASTIC

    def test_optional_fields_default_none(self):
        pkt = make_unified_packet()
        assert pkt.latitude is None
        assert pkt.longitude is None
        assert pkt.altitude is None
        assert pkt.battery_level is None
        assert pkt.voltage is None
        assert pkt.uptime_hours is None

    def test_with_position(self):
        pkt = make_unified_packet(latitude=51.5, longitude=-0.12, altitude=100)
        assert pkt.latitude == 51.5
        assert pkt.longitude == -0.12
        assert pkt.altitude == 100

    def test_with_telemetry(self):
        pkt = make_unified_packet(
            battery_level=85.0,
            voltage=3.7,
            channel_util=15.2,
            uptime_hours=12,
            uptime_minutes=30,
        )
        assert pkt.battery_level == 85.0
        assert pkt.voltage == 3.7
        assert pkt.uptime_hours == 12

    def test_raw_packet_defaults_empty_dict(self):
        pkt = make_unified_packet()
        assert pkt.raw_packet == {}

    def test_meshcore_backend(self):
        pkt = make_unified_packet(
            from_id="mc:a1b2c3d4e5f6",
            backend=BackendType.MESHCORE,
        )
        assert pkt.backend == BackendType.MESHCORE
        d = pkt.to_dict()
        assert d['backend'] == 'meshcore'


class TestUnifiedNode:
    """Verify UnifiedNode creation and serialization."""

    def test_create_default(self):
        node = make_unified_node()
        assert node.node_id == "!aabbccdd"
        assert node.display_name == "Alice"
        assert node.backend == BackendType.MESHTASTIC

    def test_to_dict(self):
        node = make_unified_node()
        d = node.to_dict()
        assert d['backend'] == 'meshtastic'
        assert d['node_id'] == '!aabbccdd'

    def test_optional_fields(self):
        node = make_unified_node(
            public_key="deadbeef",
            hw_model="RAK4631",
            last_seen="2026-03-17T12:00:00",
            latitude=51.5,
            longitude=-0.12,
            battery_level=90.0,
        )
        assert node.public_key == "deadbeef"
        assert node.hw_model == "RAK4631"
        assert node.latitude == 51.5

    def test_raw_data_defaults_empty_dict(self):
        node = make_unified_node()
        assert node.raw_data == {}

    def test_meshcore_node(self):
        node = make_unified_node(
            node_id="mc:a1b2c3d4e5f6",
            display_name="MC Node",
            short_name="",
            backend=BackendType.MESHCORE,
            public_key="a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
        )
        assert node.backend == BackendType.MESHCORE
        d = node.to_dict()
        assert d['backend'] == 'meshcore'
        assert d['public_key'] is not None


class TestPacketSummaryAlias:
    """Verify that PacketSummary is an alias for UnifiedPacket."""

    def test_alias(self):
        assert PacketSummary is UnifiedPacket

    def test_create_via_alias(self):
        pkt = PacketSummary(
            timestamp="2026-03-17T12:00:00",
            from_id="!aabbccdd",
            to_id="!11223344",
            from_name="A",
            to_name="B",
            port_name="TEXT_MESSAGE_APP",
            backend=BackendType.MESHTASTIC,
        )
        assert isinstance(pkt, UnifiedPacket)
