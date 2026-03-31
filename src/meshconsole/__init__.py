"""
MeshConsole - A tool for interacting with Meshtastic and MeshCore mesh networking devices.

Author: M9WAV
License: MIT
"""

__version__ = "3.11.7"
__author__ = "M9WAV"

from meshconsole.core import (
    MeshtasticTool,
    MeshtasticToolError,
    MeshConsole,
    configure_logging,
)
from meshconsole.models import (
    PacketSummary,
    UnifiedPacket,
    UnifiedNode,
    BackendType,
    ConnectionType,
)
from meshconsole.database import DatabaseHandler

__all__ = [
    # New v3.0 exports
    "MeshConsole",
    "UnifiedPacket",
    "UnifiedNode",
    "BackendType",
    "ConnectionType",
    "DatabaseHandler",
    # Backward compatibility
    "MeshtasticTool",
    "MeshtasticToolError",
    "PacketSummary",
    "configure_logging",
    "__version__",
]
