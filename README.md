<p align="center">
  <img src="https://raw.githubusercontent.com/m9wav/MeshConsole/main/logo.png" alt="MeshConsole" width="400"/>
</p>

<p align="center">
  <strong>A web-based monitoring and control dashboard for Meshtastic and MeshCore mesh networks.</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/meshconsole/"><img src="https://img.shields.io/pypi/v/meshconsole" alt="PyPI"></a>
  <a href="https://m9wav.uk/">m9wav.uk</a>
</p>

---

## What's New in v3.0.0

- **MeshCore backend support** -- connect to MeshCore devices via BLE, serial, or TCP alongside Meshtastic
- **Dual-device mode** -- monitor a Meshtastic and MeshCore device simultaneously from a single dashboard
- **Modular architecture** -- refactored from a monolithic core into clean backend/orchestrator/web modules
- **Backend badges and filters** -- the web UI shows which backend each packet came from, with filtering
- **70-test suite** -- comprehensive test coverage for all components
- **Automatic database migration** -- existing v2.x databases are upgraded transparently on first run
- **Fully backward compatible** -- all existing CLI commands, config files, and APIs continue to work unchanged

---

## Installation

```bash
# Meshtastic support only (same as before)
pip install meshconsole

# With MeshCore support
pip install meshconsole[meshcore]

# Everything (MeshCore + dev tools)
pip install meshconsole[all]
```

Or install from source:

```bash
git clone https://github.com/m9wav/MeshConsole.git
cd MeshConsole
pip install -e ".[meshcore]"
```

---

So I got really into Meshtastic after picking up a couple of LoRa radios and wanted a way to monitor my mesh network from my computer. The official app is fine but I wanted something I could leave running on a server, log everything to a database, and maybe poke at later.

This started as a quick script and... well, it grew. Now it's got a web UI, MeshCore support, dual-device mode, and everything. Figured I'd clean it up and share it.

## What it does

- Connects to your **Meshtastic** device over **USB or TCP/IP** (WiFi)
- Connects to your **MeshCore** device over **BLE, serial, or TCP**
- Runs both backends simultaneously in **dual mode**
- Logs all packets to a SQLite database (with backend tagging)
- Shows a live web dashboard with all the node activity
- Lets you send messages and run traceroutes from the web UI
- Exports your data to JSON/CSV if you want to analyze it elsewhere
- Auto-reconnects if the connection drops

The web interface shows positions on a map, telemetry data (battery, signal strength, etc), and you can see message history. Pretty handy for debugging mesh issues.

## Setup

```bash
pip install meshconsole
cp config.example.ini config.ini
```

Edit `config.ini` with your setup. The main thing is picking your backend and connection type:

```ini
[Device]
# "usb" for plugged-in device, "tcp" for network
connection_type = usb

# Only needed for TCP mode
ip = 192.168.1.100

# Usually leave blank for auto-detect, but you can specify
# serial_port = /dev/cu.usbserial-0001
```

If you're using TCP, your device needs to have WiFi enabled and you need to know its IP.

## Quick Start

### Meshtastic -- USB Connection (device plugged in)

```bash
# Start web dashboard with USB-connected device
meshconsole listen --usb --web

# Specify serial port explicitly
meshconsole listen --usb --port /dev/ttyUSB0 --web

# macOS example
meshconsole listen --usb --port /dev/cu.usbserial-0001 --web
```

### Meshtastic -- TCP/IP Connection (WiFi-enabled device)

```bash
# Start web dashboard with network-connected device
meshconsole listen --ip 192.168.1.100 --web
```

Then open **http://localhost:5055** in your browser.

### MeshCore -- Serial Connection

```bash
meshconsole listen --backend meshcore --mc-serial /dev/cu.usbserial-0001 --web
```

### MeshCore -- BLE Connection

```bash
meshconsole listen --backend meshcore --mc-ble "AA:BB:CC:DD:EE:FF" --web
```

### Dual Mode -- Meshtastic + MeshCore simultaneously

```bash
meshconsole listen --backend dual --usb --mc-serial /dev/ttyUSB0 --web
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

## MeshCore Configuration

To use MeshCore, install with `pip install meshconsole[meshcore]` and add these sections to your `config.ini`:

```ini
[Backend]
# Backend mode: meshtastic, meshcore, or dual
# Default: meshtastic (backward compatible -- omit this section entirely for existing setups)
mode = meshcore

[MeshCore]
# Connection type: ble, usb, or tcp
connection_type = usb
# Serial port (for usb connection type)
serial_port = /dev/ttyUSB0
# BLE address (for ble connection type)
ble_address =
# BLE PIN (optional, for secured devices)
ble_pin =
# TCP host (for tcp connection type)
tcp_host =
# TCP port (for tcp connection type)
tcp_port =
```

For dual mode, set `mode = dual` and configure both `[Device]` (Meshtastic) and `[MeshCore]` sections. CLI arguments (`--backend`, `--mc-serial`, etc.) override config file values.

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
pip install meshconsole[meshcore] gunicorn
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

The database is useful if you want to do your own analysis. The `packets` table has everything including the full raw packet data as JSON. In v3.0.0, packets also include a `backend` column indicating which backend they came from.

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

**Web interface not loading:**
- Check if port 5055 is already in use
- Try a different port in `config.ini` under `[Web]`

**Seeing your own messages in the log:**
- Shouldn't happen - the tool auto-detects your local node and filters it out
- If it's not working, check the logs for the detected node ID

## Dependencies

- meshtastic
- flask
- flask-cors
- protobuf
- pypubsub
- meshcore (optional, for MeshCore support)

## License

MIT. Do whatever you want with it.

---

Built by [M9WAV](https://m9wav.uk/). If you find bugs or have ideas, feel free to open an issue.
