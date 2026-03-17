"""
Shared fixtures for MeshConsole test suite.

Provides mock meshtastic interfaces, sample packets, test configuration,
and in-memory database instances.
"""

# ── Fix sys.path before any meshconsole imports ───────────────────
# The standalone meshconsole.py in the project root shadows the installed
# meshconsole package.  We must ensure the src/ directory takes priority.
import sys as _sys
import os as _os

_project_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_src_dir = _os.path.join(_project_root, "src")

# Remove project root from sys.path (contains meshconsole.py)
_sys.path = [p for p in _sys.path if _os.path.abspath(p) != _project_root]
# Ensure src/ is at the front
if _src_dir not in _sys.path:
    _sys.path.insert(0, _src_dir)
# ──────────────────────────────────────────────────────────────────

import json
import os
import sqlite3
import tempfile

import pytest

from meshconsole.database import DatabaseHandler
from meshconsole.models import BackendType, UnifiedPacket, UnifiedNode


# ── Sample data factories ─────────────────────────────────────────

def make_raw_meshtastic_packet(
    from_id="!aabbccdd",
    to_id="!11223344",
    portnum="TEXT_MESSAGE_APP",
    text="Hello mesh",
    hop_limit=3,
    rssi=-90,
    snr=8.5,
    position=None,
    telemetry=None,
    user=None,
):
    """Build a dict that looks like a raw meshtastic packet."""
    decoded = {"portnum": portnum}
    if text:
        decoded["text"] = text
    if position:
        decoded["position"] = position
    if telemetry:
        decoded["telemetry"] = telemetry
    if user:
        decoded["user"] = user

    pkt = {
        "fromId": from_id,
        "toId": to_id,
        "decoded": decoded,
        "hopLimit": hop_limit,
        "rxRssi": rssi,
        "rxSnr": snr,
    }
    return pkt


def make_unified_packet(
    timestamp="2026-03-17T12:00:00",
    from_id="!aabbccdd",
    to_id="!11223344",
    from_name="Alice",
    to_name="Bob",
    port_name="TEXT_MESSAGE_APP",
    backend=BackendType.MESHTASTIC,
    message="Hello",
    **kwargs,
) -> UnifiedPacket:
    """Create a UnifiedPacket with sensible defaults."""
    return UnifiedPacket(
        timestamp=timestamp,
        from_id=from_id,
        to_id=to_id,
        from_name=from_name,
        to_name=to_name,
        port_name=port_name,
        backend=backend,
        message=message,
        **kwargs,
    )


def make_unified_node(
    node_id="!aabbccdd",
    display_name="Alice",
    short_name="ALI",
    backend=BackendType.MESHTASTIC,
    **kwargs,
) -> UnifiedNode:
    """Create a UnifiedNode with sensible defaults."""
    return UnifiedNode(
        node_id=node_id,
        display_name=display_name,
        short_name=short_name,
        backend=backend,
        **kwargs,
    )


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    """Provide a DatabaseHandler backed by a temp file, cleaned up automatically."""
    db_file = str(tmp_path / "test.db")
    handler = DatabaseHandler(db_file=db_file)
    yield handler
    handler.close()


@pytest.fixture
def memory_db():
    """Provide a DatabaseHandler backed by :memory: SQLite.

    Because :memory: dbs are per-connection and DatabaseHandler opens its own
    connection, this fixture uses a temp file instead for reliability.
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    handler = DatabaseHandler(db_file=path)
    yield handler
    handler.close()
    os.unlink(path)


@pytest.fixture
def sample_packet():
    """Return a single sample UnifiedPacket."""
    return make_unified_packet()


@pytest.fixture
def sample_node():
    """Return a single sample UnifiedNode."""
    return make_unified_node()


@pytest.fixture
def sample_raw_packet():
    """Return a raw meshtastic-style packet dict."""
    return make_raw_meshtastic_packet()


@pytest.fixture
def populated_db(tmp_db):
    """A database pre-populated with a handful of test packets."""
    packets = [
        make_unified_packet(
            timestamp=f"2026-03-17T12:0{i}:00",
            from_id=f"!aabb{i:04x}",
            to_id="!11223344",
            from_name=f"Node{i}",
            to_name="Bob",
            port_name="TEXT_MESSAGE_APP" if i % 2 == 0 else "POSITION_APP",
            message=f"msg{i}" if i % 2 == 0 else "",
        )
        for i in range(10)
    ]
    for pkt in packets:
        pkt_dict = {
            'timestamp': pkt.timestamp,
            'from_id': pkt.from_id,
            'to_id': pkt.to_id,
            'port_name': pkt.port_name,
            'payload': pkt.payload,
            'raw_packet': pkt.raw_packet,
            'backend': pkt.backend.value,
        }
        tmp_db.log_packet(pkt_dict)
    return tmp_db
