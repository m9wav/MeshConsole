"""
MeshConsole Backend Base Class
------------------------------
Abstract base class defining the interface all mesh backends must implement.

Author: M9WAV
License: MIT
"""

from abc import ABC, abstractmethod
from typing import Optional, Callable

from meshconsole.models import BackendType, UnifiedPacket, UnifiedNode


class MeshBackend(ABC):
    """Abstract base class for mesh device backends."""

    @property
    @abstractmethod
    def backend_type(self) -> BackendType:
        """Return the backend type identifier."""
        ...

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Return True if the backend is currently connected to a device."""
        ...

    @property
    @abstractmethod
    def local_node_id(self) -> Optional[str]:
        """Return the canonical node ID of the connected device, or None."""
        ...

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the device. May start background threads."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Cleanly disconnect from the device."""
        ...

    @abstractmethod
    def get_nodes(self) -> dict[str, UnifiedNode]:
        """Return all known nodes, keyed by canonical node_id."""
        ...

    @abstractmethod
    def resolve_node_name(self, node_id: str) -> str:
        """Resolve a node ID to a human-readable name."""
        ...

    @abstractmethod
    def send_message(self, destination: str, message: str) -> None:
        """Send a text message to the specified destination."""
        ...

    @abstractmethod
    def send_traceroute(self, destination: str, hop_limit: int = 10) -> None:
        """Initiate a traceroute/path discovery to the destination."""
        ...

    @abstractmethod
    def on_packet_received(self, callback: Callable[[UnifiedPacket], None]) -> None:
        """Register a callback for incoming packets."""
        ...

    @abstractmethod
    def on_connection_established(self, callback: Callable[[], None]) -> None:
        """Register a callback for connection establishment."""
        ...

    @abstractmethod
    def on_connection_lost(self, callback: Callable[[], None]) -> None:
        """Register a callback for connection loss."""
        ...
