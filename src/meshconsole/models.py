"""
MeshConsole Data Models
-----------------------
Unified data models for multi-backend mesh networking.

Author: M9WAV
License: MIT
"""

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


class BackendType(Enum):
    """Identifies which mesh backend produced a packet or node."""
    MESHTASTIC = "meshtastic"
    MESHCORE = "meshcore"


class ConnectionType(Enum):
    """Connection method to a mesh device."""
    TCP = "tcp"
    USB = "usb"
    BLE = "ble"


@dataclass
class UnifiedNode:
    """Common node representation across backends."""
    node_id: str              # "!aabbccdd" (meshtastic) or "mc:a1b2c3d4e5f6" (meshcore)
    display_name: str         # Long name or advertisement name
    short_name: str           # Short name (meshtastic) or "" (meshcore)
    backend: BackendType
    public_key: Optional[str] = None    # Full Ed25519 pubkey hex (meshcore)
    hw_model: Optional[str] = None
    last_seen: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    battery_level: Optional[float] = None
    raw_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable dictionary."""
        d = asdict(self)
        d['backend'] = self.backend.value
        return d


@dataclass
class UnifiedPacket:
    """Common packet representation across backends.

    This replaces the legacy PacketSummary dataclass and adds multi-backend support.
    """
    timestamp: str
    from_id: str
    to_id: str
    from_name: str
    to_name: str
    port_name: str            # Normalized: TEXT_MESSAGE_APP, POSITION_APP, TELEMETRY_APP, etc.
    backend: BackendType
    payload: str = ""
    message: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    position_time: Optional[float] = None
    hop_limit: Optional[int] = None
    priority: Optional[int] = None
    rssi: object = None       # Can be float or 'Unknown'
    snr: object = None        # Can be float or 'Unknown'
    battery_level: Optional[float] = None
    voltage: Optional[float] = None
    channel_util: Optional[float] = None
    air_util_tx: Optional[float] = None
    uptime_hours: Optional[int] = None
    uptime_minutes: Optional[int] = None
    raw_packet: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable dictionary."""
        d = asdict(self)
        d['backend'] = self.backend.value
        return d


# Legacy alias for backward compatibility
PacketSummary = UnifiedPacket
