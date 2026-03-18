# Changelog

## v3.2.0 (2026-03-18)

Multi-device support -- connect unlimited devices of any backend type simultaneously.

### New Features

- **Multi-device architecture** -- replaced the fixed one-Meshtastic + one-MeshCore slot design with a `backends: list[MeshBackend]` that supports any number of devices. Run 2 Meshtastic + 1 MeshCore, 3 MeshCore, or any combination.

- **`--device` CLI flag** -- repeatable argument for specifying multiple devices:
  ```
  meshconsole listen --device meshtastic:usb:/dev/ttyACM0 --device meshcore:usb:/dev/ttyUSB0 --web
  ```

- **Multi-device config format** -- new `[Devices]` section with numbered `[Device.N]` entries. Legacy `[Device]` + `[MeshCore]` format still works.

- **Per-device identity** -- each backend instance gets a unique `device_id` (e.g. `meshtastic:!fa9dc488`, `meshcore:mc:293f0e7dc6da`). Shown in status bar, system info, and stored on packets.

- **Dynamic backend filter** -- web UI filter dropdown auto-populates from connected devices with per-type groups and individual device entries.

- **pypubsub multi-instance fix** -- multiple MeshtasticBackend instances no longer cross-talk. Each instance only processes packets from its own interface.

- **Per-device health checks** -- reconnection logic works independently per backend. One device disconnecting doesn't disrupt others.

### Changes

- `self._backend` / `self._meshcore_backend` replaced with `self.backends` list (backward-compat properties preserved)
- Auto-detection now finds ALL devices, not just first of each type
- `/status` API returns `backends_list` array alongside legacy `backends` dict
- Database: new `device_id` column on packets/messages tables (auto-migrated)
- `send_message` routes by node ID prefix (`mc:` → MeshCore, `!` → Meshtastic)
- `MeshtasticBackend.connect()` now subscribes to pypubsub (moved from `__init__`)
- `MeshtasticBackend.disconnect()` now unsubscribes from pypubsub

---

## v3.1.0 (2026-03-18)

USB auto-detection and flexible dependency installation.

### New Features

- **USB auto-detection** -- `meshconsole listen --usb --web` now scans all serial ports, identifies which are Meshtastic and which are MeshCore, and connects to everything it finds. No need to specify `--backend`, `--port`, or `--mc-serial`. Just plug in and go.

- **Flexible dependencies** -- Backends are now fully optional. Neither Meshtastic nor MeshCore is a hard dependency:
  - `pip install meshconsole[meshtastic]` -- Meshtastic support only
  - `pip install meshconsole[meshcore]` -- MeshCore support only
  - `pip install meshconsole[all]` -- both backends
  - `pip install meshconsole` -- core only (web UI, database, CLI framework)

- **Auto backend mode** -- New `--backend auto` option (and the default when using `--usb` without explicit ports). Probes MeshCore first (fast fail), then Meshtastic, and sets single or dual mode based on what's detected.

- **Smart fallback** -- If only one backend library is installed, MeshConsole automatically defaults to that backend without requiring explicit `--backend` selection.

### Changes

- `meshtastic` and `protobuf` moved from core dependencies to `[meshtastic]` optional extra
- `pyserial` added as a core dependency (lightweight, needed for port scanning)
- `MeshtasticBackend` now has an import guard matching the existing `MeshCoreBackend` pattern
- All top-level meshtastic imports removed from `core.py` -- the package now imports cleanly even without meshtastic installed
- Clear error messages when a backend library is missing (e.g., "Install with: pip install meshconsole[meshtastic]")

---

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
