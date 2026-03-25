<p align="center">
  <img src="https://raw.githubusercontent.com/m9wav/MeshConsole/main/logo.png" alt="MeshConsole" width="400"/>
</p>

<p align="center">
  <strong>A web-based monitoring and control dashboard for Meshtastic and MeshCore mesh networks.</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/meshconsole/"><img src="https://img.shields.io/pypi/v/meshconsole?v=3.9.1" alt="PyPI"></a>
  <a href="https://m9wav.uk/">m9wav.uk</a>
</p>

---

## What's New

**v3.9.0** -- Deep packet intelligence: MeshCore protocol-aware routing target and sender resolution using RX path context, traceroute visual paths, hops-used display, channel badges

**v3.8.0** -- Private messaging: threaded conversation view with chat-style bubbles, per-device source selection, configurable DM privacy from feed

**v3.7.0** -- Mesh Map: live geographic network map with dark basemap, zoom-progressive node loading, high-volume trunk visualization, smart distance filtering

**v3.6.0** -- Smart graph: server-side filtering with importance scoring, home/refresh controls, node slider, bootstrap learning from geo-resolved hops

**v3.5.0** -- Geographic route intelligence: three-phase geo-disambiguation for hash collisions, cascading resolution, confidence-based graph colouring, MeshCore flood advertisement tab

**v3.4.0** -- Route intelligence: decoded path trails, ML route learning, D3.js mesh topology graph, node search autocomplete

**v3.3.0** -- Auth UX, routing noise filter, cleaner UI

**v3.2.0** -- Unlimited simultaneous devices, per-device identity

**v3.1.0** -- USB auto-detection, flexible optional dependencies

**v3.0.0** -- MeshCore backend, dual-device mode, modular architecture

---

## Installation

```bash
# Just Meshtastic
pip install meshconsole[meshtastic]

# Just MeshCore
pip install meshconsole[meshcore]

# Both backends
pip install meshconsole[all]

# Core only (no backend deps -- for custom setups)
pip install meshconsole
```

Or install from source:

```bash
git clone https://github.com/m9wav/MeshConsole.git
cd MeshConsole
pip install -e ".[all]"
```

---

So I got really into Meshtastic after picking up a couple of LoRa radios and wanted a way to monitor my mesh network from my computer. The official app is fine but I wanted something I could leave running on a server, log everything to a database, and maybe poke at later.

This started as a quick script and... well, it grew. Now it's got a web UI, MeshCore support, dual-device mode, USB auto-detection, and everything. Figured I'd clean it up and share it.

## What it does

- **Auto-detects** your Meshtastic and MeshCore devices over USB -- just plug in and go
- Connects to **Meshtastic** over USB or TCP/IP (WiFi)
- Connects to **MeshCore** over BLE, serial, or TCP
- Runs both backends simultaneously in **dual mode**
- Logs all packets to a SQLite database (with backend tagging)
- Shows a live web dashboard with all the node activity
- Lets you send messages and run traceroutes from the web UI
- Exports your data to JSON/CSV if you want to analyze it elsewhere
- Auto-reconnects if the connection drops

The web interface shows positions on a map, telemetry data (battery, signal strength, etc), and you can see message history. Pretty handy for debugging mesh issues.

## Quick Start

The simplest way -- plug in your device(s) and run:

```bash
meshconsole listen --usb --web
```

MeshConsole will scan your serial ports, figure out what's Meshtastic and what's MeshCore, and connect to everything it finds. Open **http://localhost:5055** in your browser.

### Explicit connections

```bash
# Meshtastic via USB (specific port)
meshconsole listen --usb --port /dev/ttyACM0 --web

# Meshtastic via TCP/IP
meshconsole listen --ip 192.168.1.100 --web

# MeshCore via serial
meshconsole listen --backend meshcore --mc-serial /dev/ttyUSB0 --web

# MeshCore via BLE
meshconsole listen --backend meshcore --mc-ble "AA:BB:CC:DD:EE:FF" --web

# Dual mode (explicit)
meshconsole listen --backend dual --usb --mc-serial /dev/ttyUSB0 --web

# Multiple devices (any combination)
meshconsole listen --device meshtastic:usb:/dev/ttyACM0 --device meshtastic:tcp:192.168.1.100 --device meshcore:usb:/dev/ttyUSB0 --web
```

### Other Commands

```bash
# Listen without web interface (CLI output only)
meshconsole listen --usb --verbose

# List nodes your device knows about
meshconsole nodes --usb

# Send a message
meshconsole send --usb --dest !12345678 --message "hey there"

# Traceroute to a node
meshconsole traceroute --usb --dest !12345678
```

## Configuration

For more control, create a `config.ini`:

```bash
cp config.example.ini config.ini
```

### Meshtastic only

```ini
[Device]
connection_type = usb
serial_port = /dev/ttyACM0
```

### MeshCore only

```ini
[Backend]
mode = meshcore

[MeshCore]
connection_type = usb
serial_port = /dev/ttyUSB0
```

### Dual mode

```ini
[Backend]
mode = dual

[Device]
connection_type = usb
serial_port = /dev/ttyACM0

[MeshCore]
connection_type = usb
serial_port = /dev/ttyUSB0
```

### Auto-detect (default for USB)

```ini
[Device]
connection_type = usb
```

No `[Backend]` section needed -- MeshConsole will scan and detect automatically when using USB without explicit ports.

### Multiple devices (new in v3.2.0)

```ini
[Devices]
count = 3

[Device.0]
type = meshtastic
connection_type = usb
serial_port = /dev/ttyACM0

[Device.1]
type = meshtastic
connection_type = tcp
ip = 192.168.1.100

[Device.2]
type = meshcore
connection_type = usb
serial_port = /dev/ttyUSB0
```

CLI arguments (`--backend`, `--mc-serial`, `--device`, etc.) always override config file values.

## The web dashboard

When you run with `--web`, you get a dashboard at port 5055. It shows:

- Live packet feed (updates automatically)
- Node list with signal info
- Map with positions (if nodes are reporting GPS)
- Stats about your network
- Backend badges on each packet (Meshtastic or MeshCore)
- Backend filter dropdown to view traffic from one backend at a time
- Per-backend connection status in the header

There's a password for sending messages/traceroutes so you can leave the dashboard open without worrying about someone messing with your network. Set it in `config.ini` under `[Security]`. Leave `auth_password` blank if you don't care.

## Production Deployment

For running MeshConsole as a persistent service behind a reverse proxy (nginx, caddy, etc.):

```bash
pip install meshconsole[all] gunicorn
```

Create a `wsgi.py` entry point, then run with gunicorn:

```bash
gunicorn --workers 1 --threads 1 --bind 127.0.0.1:5055 --timeout 120 wsgi:application
```

A systemd service file:

```ini
[Unit]
Description=MeshConsole Web Interface
After=network.target

[Service]
Type=simple
User=meshconsole
WorkingDirectory=/opt/meshconsole
Environment="PATH=/opt/meshconsole/venv/bin:/usr/bin"
ExecStart=/opt/meshconsole/venv/bin/gunicorn --workers 1 --threads 1 --bind 127.0.0.1:5055 --timeout 120 wsgi:application
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

**Important:** Use `--workers 1` since each worker maintains its own device connection.

## Files

After running for a while you'll have:
- `meshtastic_messages.db` - SQLite database with all your packets
- `meshtastic_tool.log` - Logs (rotates automatically)

The database is useful if you want to do your own analysis. The `packets` table has everything including the full raw packet data as JSON, with a `backend` column indicating which backend each packet came from.

## Exporting data

```bash
meshconsole export --format json
meshconsole export --format csv
```

Or use the Export button in the web dashboard's Settings tab.

## Troubleshooting

**Can't connect via USB:**
- Make sure you have the right drivers (CP2102/CH340/etc)
- Check `ls /dev/cu.usb*` (Mac) or `ls /dev/ttyUSB*` (Linux) to see if the device shows up
- Try specifying the port explicitly with `--port`

**Can't connect via TCP:**
- Make sure WiFi is enabled on your Meshtastic device
- Check you can ping the IP
- The device uses port 4403 by default

**Can't connect to MeshCore:**
- Make sure `meshcore` is installed: `pip install meshconsole[meshcore]`
- For BLE, ensure Bluetooth is enabled and the device is in range
- For serial, check that the correct port is specified with `--mc-serial`
- The device must be running MeshCore companion firmware

**"meshtastic package required" error:**
- Install the meshtastic extra: `pip install meshconsole[meshtastic]`

**Auto-detect not finding devices:**
- Check that devices show up in `ls /dev/ttyUSB* /dev/ttyACM*`
- Install the backend libraries: `pip install meshconsole[all]`
- Try specifying ports explicitly to narrow down the issue

**Web interface not loading:**
- Check if port 5055 is already in use
- Try a different port in `config.ini` under `[Web]`

## Dependencies

Core: flask, flask-cors, pypubsub, pyserial, requests

Optional backends:
- `meshconsole[meshtastic]`: meshtastic, protobuf
- `meshconsole[meshcore]`: meshcore

## License

MIT. Do whatever you want with it.

---

Built by [M9WAV](https://m9wav.uk/). If you find bugs or have ideas, feel free to open an issue.
