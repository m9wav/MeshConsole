<p align="center">
  <img src="https://raw.githubusercontent.com/m9wav/MeshConsole/main/logo.png" alt="MeshConsole" width="400"/>
</p>

<p align="center">
  <strong>A web-based monitoring and control dashboard for Meshtastic mesh networks.</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/meshconsole/"><img src="https://img.shields.io/pypi/v/meshconsole" alt="PyPI"></a>
  <a href="https://m9wav.uk/">m9wav.uk</a>
</p>

---

## Installation

```bash
pip install meshconsole
```

Or install from source:

```bash
git clone https://github.com/m9wav/MeshConsole.git
cd MeshConsole
pip install -r requirements.txt
```

---

So I got really into Meshtastic after picking up a couple of LoRa radios and wanted a way to monitor my mesh network from my computer. The official app is fine but I wanted something I could leave running on a server, log everything to a database, and maybe poke at later.

This started as a quick script and... well, it grew. Now it's got a web UI and everything. Figured I'd clean it up and share it.

## What it does

- Connects to your Meshtastic device over **USB or TCP/IP** (WiFi)
- Logs all packets to a SQLite database
- Shows a live web dashboard with all the node activity
- Lets you send messages and run traceroutes from the web UI
- Exports your data to JSON/CSV if you want to analyze it elsewhere
- Auto-reconnects if the connection drops

The web interface shows positions on a map, telemetry data (battery, signal strength, etc), and you can see message history. Pretty handy for debugging mesh issues.

## Setup

```bash
pip install -r requirements.txt
cp config.example.ini config.ini
```

Edit `config.ini` with your setup. The main thing is picking USB or TCP:

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

### USB Connection (device plugged in)

```bash
# Start web dashboard with USB-connected device
meshconsole listen --usb --web

# Specify serial port explicitly
meshconsole listen --usb --port /dev/ttyUSB0 --web

# macOS example
meshconsole listen --usb --port /dev/cu.usbserial-0001 --web
```

### TCP/IP Connection (WiFi-enabled device)

```bash
# Start web dashboard with network-connected device
meshconsole listen --ip 192.168.1.100 --web
```

Then open **http://localhost:5055** in your browser.

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

> **Note:** If installed from source, use `python3 meshconsole.py` instead of `meshconsole`.

## The web dashboard

When you run with `--web`, you get a dashboard at port 5055. It shows:

- Live packet feed (updates automatically)
- Node list with signal info
- Map with positions (if nodes are reporting GPS)
- Stats about your network

There's a password for sending messages/traceroutes so you can leave the dashboard open without worrying about someone messing with your network. Set it in `config.ini` under `[Security]`. Leave `auth_password` blank if you don't care.

## Files

After running for a while you'll have:
- `meshtastic_messages.db` - SQLite database with all your packets
- `meshtastic_tool.log` - Logs (rotates automatically)

The database is useful if you want to do your own analysis. The `packets` table has everything including the full raw packet data as JSON.

## Exporting data

```bash
python3 meshconsole.py export --format json
python3 meshconsole.py export --format csv
```

Spits out `meshtastic_data.json` or `meshtastic_data.csv`.

## Troubleshooting

**Can't connect via USB:**
- Make sure you have the right drivers (CP2102/CH340/etc)
- Check `ls /dev/cu.usb*` (Mac) or `ls /dev/ttyUSB*` (Linux) to see if the device shows up
- Try specifying the port explicitly with `--port`

**Can't connect via TCP:**
- Make sure WiFi is enabled on your Meshtastic device
- Check you can ping the IP
- The device uses port 4403 by default

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

All in `requirements.txt`.

## License

MIT. Do whatever you want with it.

---

Built by [M9WAV](https://m9wav.uk/). If you find bugs or have ideas, feel free to open an issue.
