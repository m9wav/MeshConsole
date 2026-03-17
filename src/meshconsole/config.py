"""
MeshConsole Configuration
-------------------------
Configuration loading, validation, and defaults.

Supports INI files, environment variable overrides, and new MeshCore sections
while maintaining full backward compatibility with existing v2.x configs.

Author: M9WAV
License: MIT
"""

import configparser
import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE = 'config.ini'


class MeshConsoleConfig:
    """Configuration manager for MeshConsole.

    Loads from INI file with environment variable overrides.
    Missing sections/keys fall back to sensible defaults.
    """

    def __init__(self, config_file=DEFAULT_CONFIG_FILE):
        self.config_file = config_file
        self._parser = configparser.ConfigParser()
        self._parser.read(config_file)

    # ── Backend mode ──────────────────────────────────────────────

    @property
    def backend_mode(self) -> str:
        """Return the backend mode: 'meshtastic', 'meshcore', or 'dual'."""
        return (
            os.getenv('MESHCONSOLE_BACKEND_MODE')
            or self._parser.get('Backend', 'mode', fallback='meshtastic')
        )

    # ── Meshtastic device settings ────────────────────────────────

    @property
    def connection_type(self) -> str:
        return (
            os.getenv('MESHTASTIC_CONNECTION_TYPE')
            or self._parser.get('Device', 'connection_type', fallback='tcp')
        )

    @property
    def device_ip(self) -> str:
        return (
            os.getenv('MESHTASTIC_DEVICE_IP')
            or self._parser.get('Device', 'ip', fallback='127.0.0.1')
        )

    @property
    def serial_port(self) -> str | None:
        val = (
            os.getenv('MESHTASTIC_SERIAL_PORT')
            or self._parser.get('Device', 'serial_port', fallback='')
        )
        return val if val else None

    # ── MeshCore device settings ──────────────────────────────────

    @property
    def meshcore_connection_type(self) -> str:
        return (
            os.getenv('MESHCORE_CONNECTION_TYPE')
            or self._parser.get('MeshCore', 'connection_type', fallback='ble')
        )

    @property
    def meshcore_ble_address(self) -> str | None:
        val = (
            os.getenv('MESHCORE_BLE_ADDRESS')
            or self._parser.get('MeshCore', 'ble_address', fallback='')
        )
        return val if val else None

    @property
    def meshcore_ble_pin(self) -> str | None:
        val = (
            os.getenv('MESHCORE_BLE_PIN')
            or self._parser.get('MeshCore', 'ble_pin', fallback='')
        )
        return val if val else None

    @property
    def meshcore_serial_port(self) -> str | None:
        val = (
            os.getenv('MESHCORE_SERIAL_PORT')
            or self._parser.get('MeshCore', 'serial_port', fallback='')
        )
        return val if val else None

    @property
    def meshcore_tcp_host(self) -> str | None:
        val = (
            os.getenv('MESHCORE_TCP_HOST')
            or self._parser.get('MeshCore', 'tcp_host', fallback='')
        )
        return val if val else None

    @property
    def meshcore_tcp_port(self) -> int | None:
        val = (
            os.getenv('MESHCORE_TCP_PORT')
            or self._parser.get('MeshCore', 'tcp_port', fallback='')
        )
        return int(val) if val else None

    # ── Filter settings ───────────────────────────────────────────

    @property
    def sender_filter(self) -> str | None:
        val = (
            os.getenv('MESHTASTIC_SENDER_FILTER')
            or self._parser.get('Filter', 'sender', fallback='')
        )
        return val if val else None

    # ── Web settings ──────────────────────────────────────────────

    @property
    def web_enabled(self) -> bool:
        env = os.getenv('MESHTASTIC_WEB_ENABLED')
        if env is not None:
            return env.lower() == 'true'
        return self._parser.getboolean('Web', 'enabled', fallback=True)

    @property
    def web_host(self) -> str:
        return self._parser.get('Web', 'host', fallback='127.0.0.1')

    @property
    def web_port(self) -> int:
        return self._parser.getint('Web', 'port', fallback=5055)

    # ── Logging settings ──────────────────────────────────────────

    @property
    def log_level(self) -> str:
        return self._parser.get('Logging', 'level', fallback='INFO')

    @property
    def log_file(self) -> str:
        return self._parser.get('Logging', 'file', fallback='meshtastic_tool.log')

    @property
    def log_max_size_mb(self) -> int:
        return self._parser.getint('Logging', 'max_size', fallback=10)

    @property
    def log_backup_count(self) -> int:
        return self._parser.getint('Logging', 'backup_count', fallback=5)

    # ── Database settings ─────────────────────────────────────────

    @property
    def database_file(self) -> str:
        return self._parser.get('Database', 'file', fallback='meshtastic_messages.db')

    @property
    def max_packets_memory(self) -> int:
        return self._parser.getint('Database', 'max_packets_memory', fallback=1000)

    # ── Security settings ─────────────────────────────────────────

    @property
    def cors_enabled(self) -> bool:
        return self._parser.getboolean('Security', 'cors_enabled', fallback=False)

    @property
    def cors_origins(self) -> list[str]:
        raw = self._parser.get('Security', 'cors_origins', fallback='http://localhost,http://127.0.0.1')
        return [o.strip() for o in raw.split(',')]

    @property
    def auth_password(self) -> str:
        return self._parser.get('Security', 'auth_password', fallback='')

    @property
    def auth_timeout(self) -> int:
        """Session timeout in minutes."""
        return self._parser.getint('Security', 'auth_timeout', fallback=60)

    # ── Raw parser access (for backward compat) ──────────────────

    def get(self, section: str, key: str, fallback=None):
        """Proxy to the underlying ConfigParser.get()."""
        return self._parser.get(section, key, fallback=fallback)

    def getint(self, section: str, key: str, fallback=0):
        """Proxy to the underlying ConfigParser.getint()."""
        return self._parser.getint(section, key, fallback=fallback)

    def getboolean(self, section: str, key: str, fallback=False):
        """Proxy to the underlying ConfigParser.getboolean()."""
        return self._parser.getboolean(section, key, fallback=fallback)

    @property
    def verbose(self) -> bool:
        """Verbose mode (typically set via CLI, not config)."""
        return False
