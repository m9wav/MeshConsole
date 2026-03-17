# Changelog

## v3.0.0 (2026-03-17)

Major release adding MeshCore backend support alongside the existing Meshtastic backend.

### New Features

- **MeshCore backend** -- Full support for MeshCore mesh networking devices via the `meshcore` Python library. Connect over BLE, serial, or TCP. All MeshCore events (messages, advertisements, telemetry, path discovery) are mapped to unified packet types and logged alongside Meshtastic traffic.

- **Dual-device mode** -- Run a Meshtastic and MeshCore device simultaneously from a single MeshConsole instance. Use `--backend dual` on the CLI or set `mode = dual` in the `[Backend]` config section. Both devices share the same database, web dashboard, and packet feed.

- **Modular architecture** -- The monolithic `MeshtasticTool` class has been refactored into clean, separated modules:
  - `models.py` -- `UnifiedPacket`, `UnifiedNode`, `BackendType`, `ConnectionType`
  - `database.py` -- `DatabaseHandler` with thread-safe operations
  - `config.py` -- Configuration loading and validation
  - `web.py` -- Flask web application and REST API routes
  - `cli.py` -- CLI argument parsing and command dispatch
  - `backend/base.py` -- Abstract `MeshBackend` interface
  - `backend/meshtastic.py` -- `MeshtasticBackend` implementation
  - `backend/meshcore.py` -- `MeshCoreBackend` implementation
  - `core.py` -- `MeshConsole` orchestrator class

- **Test suite** -- 70 tests covering database operations, packet processing, configuration loading, backend behavior, web API endpoints, and dual-mode integration. All tests run without hardware using mocks.

- **New CLI arguments**:
  - `--backend {meshtastic,meshcore,dual}` -- select the backend mode
  - `--mc-ble ADDRESS` -- MeshCore BLE device address
  - `--mc-serial PORT` -- MeshCore serial port
  - `--mc-tcp HOST:PORT` -- MeshCore TCP connection

- **Web UI enhancements**:
  - Backend badges on packet cards (Meshtastic / MeshCore)
  - Backend filter dropdown to view traffic from one backend at a time
  - Per-backend connection status indicators
  - Backend field on node cards

- **Database migration** -- Existing v2.x databases are automatically migrated on first run. An additive `backend` column is added to the `packets` and `messages` tables with a default value of `'meshtastic'`, preserving all existing data. New indexes are created for backend-filtered queries.

- **New configuration sections**:
  - `[Backend]` -- controls backend mode (`meshtastic`, `meshcore`, or `dual`)
  - `[MeshCore]` -- MeshCore device connection settings (BLE address, serial port, TCP host/port, BLE PIN)

### Backward Compatibility

- Existing v2.x `config.ini` files work without any changes. When the `[Backend]` section is absent, MeshConsole defaults to `mode = meshtastic`.
- All existing CLI commands and arguments continue to work identically.
- The `MeshtasticTool` class name is preserved as an alias for `MeshConsole`.
- The `PacketSummary` dataclass is preserved as an alias for `UnifiedPacket`.
- All existing web API endpoints return the same data structures, with the `backend` field added as an additional property.
- The entry point (`meshconsole`) is unchanged.
- Environment variables (`MESHTASTIC_CONNECTION_TYPE`, etc.) continue to work.

### Installation

MeshCore support is an optional dependency to keep the default install lightweight:

```bash
pip install meshconsole            # Meshtastic only (same as before)
pip install meshconsole[meshcore]  # Adds MeshCore support
pip install meshconsole[all]       # Everything
```

### Dependencies

- New optional dependency: `meshcore>=0.1.0`
- New dev dependencies: `pytest>=7.0.0`, `pytest-asyncio>=0.21.0`, `build`, `twine`
- All existing dependencies unchanged

---

## v2.2.2 and earlier

See the [GitHub releases page](https://github.com/m9wav/MeshConsole/releases) for previous versions.
