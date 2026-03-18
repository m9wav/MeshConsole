"""
MeshConsole Backend Registry
-----------------------------
Backend discovery, registration, and factory function.

Author: M9WAV
License: MIT
"""

from meshconsole.backend.base import MeshBackend
from meshconsole.models import BackendType, ConnectionType

# Registry of available backends
_BACKEND_CLASSES: dict[BackendType, type] = {}


def register_backend(backend_type: BackendType, cls: type) -> None:
    """Register a backend class for a given backend type."""
    _BACKEND_CLASSES[backend_type] = cls


def get_backend_class(backend_type: BackendType) -> type:
    """Get the backend class for a given type, importing lazily."""
    if backend_type not in _BACKEND_CLASSES:
        if backend_type == BackendType.MESHTASTIC:
            try:
                from meshconsole.backend.meshtastic import MeshtasticBackend
                register_backend(BackendType.MESHTASTIC, MeshtasticBackend)
            except ImportError:
                raise ImportError(
                    "meshtastic is required for Meshtastic support. "
                    "Install it with: pip install meshconsole[meshtastic]"
                )
        elif backend_type == BackendType.MESHCORE:
            try:
                from meshconsole.backend.meshcore import MeshCoreBackend
                register_backend(BackendType.MESHCORE, MeshCoreBackend)
            except ImportError:
                raise ImportError(
                    "meshcore is required for MeshCore support. "
                    "Install it with: pip install meshconsole[meshcore]"
                )

    return _BACKEND_CLASSES[backend_type]


def create_backend(backend_type: BackendType, **kwargs) -> MeshBackend:
    """Factory function to create a backend instance.

    Args:
        backend_type: Which backend to create.
        **kwargs: Backend-specific configuration passed to the constructor.

    Returns:
        An instance of the requested backend.
    """
    cls = get_backend_class(backend_type)
    return cls(**kwargs)


__all__ = [
    'MeshBackend',
    'create_backend',
    'get_backend_class',
    'register_backend',
]
