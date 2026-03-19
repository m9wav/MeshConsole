"""
MeshConsole CLI
----------------
Command-line interface with argparse setup and dispatch logic.

Extracted from the main() function in core.py.

Author: M9WAV
License: MIT
"""

import argparse
import json
import logging
import sys
import time

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build and return the argparse parser with all subcommands."""
    parser = argparse.ArgumentParser(
        description="MeshConsole - Send and receive messages over Meshtastic and MeshCore devices.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--version', action='version', version='MeshConsole 3.5.2')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # ── Common arguments factory ──────────────────────────────

    def add_connection_args(sub):
        """Add shared connection arguments to a subparser."""
        sub.add_argument('--ip', type=str, required=False,
                         help="IP address of the Meshtastic device (uses config.ini if not specified)")
        sub.add_argument('--usb', action='store_true',
                         help="Connect via USB instead of TCP")
        sub.add_argument('--port', type=str, required=False,
                         help="Serial port for USB connection (e.g., /dev/cu.usbserial-0001)")
        # New v3.0 backend arguments
        sub.add_argument('--backend', choices=['meshtastic', 'meshcore', 'dual', 'auto'],
                         default=None,
                         help="Backend mode (default: from config)")
        sub.add_argument('--mc-ble', type=str, metavar='ADDRESS',
                         help="MeshCore BLE device address")
        sub.add_argument('--mc-serial', type=str, metavar='PORT',
                         help="MeshCore serial port")
        sub.add_argument('--mc-tcp', type=str, metavar='HOST:PORT',
                         help="MeshCore TCP host:port")
        sub.add_argument('--device', action='append', metavar='TYPE:CONN:ADDR',
                         dest='devices', default=None,
                         help="Add a device (repeatable). Format: meshtastic:usb:/dev/ttyACM0 or meshcore:tcp:host:port")

    # ── send ──────────────────────────────────────────────────

    send_parser = subparsers.add_parser('send', help='Send a message to a node')
    add_connection_args(send_parser)
    send_parser.add_argument('--dest', type=str, required=True,
                             help="Destination node ID to send the message to")
    send_parser.add_argument('--message', type=str, required=True,
                             help="Message to send")
    send_parser.add_argument('--verbose', action='store_true',
                             help="Enable verbose output")

    # ── listen ────────────────────────────────────────────────

    listen_parser = subparsers.add_parser('listen', help='Listen for incoming messages')
    add_connection_args(listen_parser)
    listen_parser.add_argument('--sender', type=str, required=False,
                               help="Sender ID to filter messages")
    listen_parser.add_argument('--web', action='store_true',
                               help="Enable the web server")
    listen_parser.add_argument('--verbose', action='store_true',
                               help="Enable verbose output")

    # ── nodes ─────────────────────────────────────────────────

    nodes_parser = subparsers.add_parser('nodes', help='List all known nodes')
    add_connection_args(nodes_parser)

    # ── export ────────────────────────────────────────────────

    export_parser = subparsers.add_parser('export', help='Export data to a file')
    export_parser.add_argument('--format', choices=['json', 'csv'], default='json',
                               help='Export format')

    # ── stats ─────────────────────────────────────────────────

    subparsers.add_parser('stats', help='Display network statistics')

    # ── traceroute ────────────────────────────────────────────

    traceroute_parser = subparsers.add_parser('traceroute', help='Perform a traceroute to a node')
    add_connection_args(traceroute_parser)
    traceroute_parser.add_argument('--dest', type=str, required=True,
                                   help="Destination node ID for traceroute")
    traceroute_parser.add_argument('--hop-limit', type=int, default=10,
                                   help="Maximum hop limit for traceroute")
    traceroute_parser.add_argument('--verbose', action='store_true',
                                   help="Enable verbose output")

    return parser


def _parse_device_spec(spec):
    """Parse a --device TYPE:CONN:ADDR spec into a config dict.

    Accepted formats:
        meshtastic:usb:/dev/ttyACM0
        meshtastic:tcp:192.168.1.100
        meshcore:usb:/dev/ttyUSB0
        meshcore:ble:AA:BB:CC:DD:EE:FF
        meshcore:tcp:host:port
    """
    parts = spec.split(':', 2)
    if len(parts) < 3:
        raise ValueError(f"Invalid --device format: {spec}. Expected TYPE:CONN:ADDR")

    btype, conn, addr = parts[0], parts[1], parts[2]
    cfg = {
        'type': btype,
        'connection_type': conn,
        'ip': '',
        'serial_port': '',
        'ble_address': '',
        'ble_pin': '',
        'tcp_host': '',
        'tcp_port': '',
        'device_id': '',
    }

    if btype == 'meshtastic':
        if conn == 'usb':
            cfg['serial_port'] = addr
        elif conn == 'tcp':
            cfg['ip'] = addr
    elif btype == 'meshcore':
        if conn == 'usb':
            cfg['serial_port'] = addr
        elif conn == 'ble':
            cfg['ble_address'] = addr
        elif conn == 'tcp':
            if ':' in addr:
                host, port = addr.rsplit(':', 1)
                cfg['tcp_host'] = host
                cfg['tcp_port'] = port
            else:
                cfg['tcp_host'] = addr

    return cfg


def _apply_backend_env(args):
    """Translate --backend / --mc-* / --device CLI args into environment variables.

    MeshtasticTool.__init__ and _connect_meshcore() already read these env
    vars, so setting them before instantiation is the simplest bridge.

    When --usb is passed without --port and --mc-serial, and no explicit
    backend is set, defaults to 'auto' mode for USB auto-detection.

    If no backend is specified and meshtastic is unavailable but meshcore is,
    defaults to meshcore mode.

    v3.2.0: --device repeatable args are stored in MESHCONSOLE_DEVICES env var
    as JSON for the orchestrator to read.
    """
    import os

    # Handle --device repeatable args (v3.2.0 multi-device)
    devices = getattr(args, 'devices', None)
    if devices:
        configs = [_parse_device_spec(d) for d in devices]
        os.environ['MESHCONSOLE_DEVICE_CONFIGS'] = json.dumps(configs)
        # Infer backend mode from device types
        types = {c['type'] for c in configs}
        if 'meshtastic' in types and 'meshcore' in types:
            os.environ['MESHCONSOLE_BACKEND_MODE'] = 'dual'
        elif 'meshcore' in types:
            os.environ['MESHCONSOLE_BACKEND_MODE'] = 'meshcore'
        else:
            os.environ['MESHCONSOLE_BACKEND_MODE'] = 'meshtastic'
        return  # --device overrides all other backend args

    backend = getattr(args, 'backend', None)

    # Auto-detect mode: --usb without explicit ports and no explicit backend
    if not backend:
        usb = getattr(args, 'usb', False)
        port = getattr(args, 'port', None)
        mc_serial = getattr(args, 'mc_serial', None)
        if usb and not port and not mc_serial:
            backend = 'auto'

    # If no backend specified, check availability and fall back
    if not backend:
        try:
            from meshconsole.backend.meshtastic import MESHTASTIC_AVAILABLE
        except ImportError:
            MESHTASTIC_AVAILABLE = False
        if not MESHTASTIC_AVAILABLE:
            try:
                from meshconsole.backend.meshcore import MESHCORE_AVAILABLE
            except ImportError:
                MESHCORE_AVAILABLE = False
            if MESHCORE_AVAILABLE:
                logger.info("Meshtastic unavailable; defaulting to meshcore backend.")
                backend = 'meshcore'

    if backend:
        os.environ['MESHCONSOLE_BACKEND_MODE'] = backend

    mc_ble = getattr(args, 'mc_ble', None)
    if mc_ble:
        os.environ['MESHCORE_CONNECTION_TYPE'] = 'ble'
        os.environ['MESHCORE_BLE_ADDRESS'] = mc_ble

    mc_serial = getattr(args, 'mc_serial', None)
    if mc_serial:
        os.environ['MESHCORE_CONNECTION_TYPE'] = 'usb'
        os.environ['MESHCORE_SERIAL_PORT'] = mc_serial

    mc_tcp = getattr(args, 'mc_tcp', None)
    if mc_tcp:
        os.environ['MESHCORE_CONNECTION_TYPE'] = 'tcp'
        if ':' in mc_tcp:
            host, port = mc_tcp.rsplit(':', 1)
            os.environ['MESHCORE_TCP_HOST'] = host
            os.environ['MESHCORE_TCP_PORT'] = port
        else:
            os.environ['MESHCORE_TCP_HOST'] = mc_tcp


def dispatch(args):
    """Execute the CLI command described by *args*.

    This is separated from main() so it can be called from tests or
    alternative entry points without re-parsing sys.argv.
    """
    # Lazy import to avoid circular imports and keep startup fast
    from meshconsole.core import (
        MeshtasticTool,
        MeshtasticToolError,
        configure_logging,
    )

    configure_logging()

    # Bridge CLI --backend/--mc-* args into env vars for MeshtasticTool
    _apply_backend_env(args)

    try:
        if args.command == 'send':
            conn_type = 'usb' if args.usb else None
            tool = MeshtasticTool(
                device_ip=args.ip, serial_port=args.port,
                connection_type=conn_type, verbose=getattr(args, 'verbose', False)
            )
            tool._connect_interface()
            tool.send_message(destination_id=args.dest, message=args.message)
            tool.cleanup()

        elif args.command == 'listen':
            conn_type = 'usb' if args.usb else None
            tool = MeshtasticTool(
                device_ip=args.ip, serial_port=args.port,
                connection_type=conn_type,
                sender_filter=getattr(args, 'sender', None),
                web_enabled=getattr(args, 'web', False),
                verbose=getattr(args, 'verbose', False)
            )
            tool._connect_interface()
            tool.start_listening()

        elif args.command == 'nodes':
            conn_type = 'usb' if args.usb else None
            tool = MeshtasticTool(
                device_ip=args.ip, serial_port=args.port,
                connection_type=conn_type
            )
            tool._connect_interface()
            tool.list_nodes()
            tool.cleanup()

        elif args.command == 'export':
            tool = MeshtasticTool()
            tool.export_data(export_format=args.format)
            tool.cleanup()

        elif args.command == 'stats':
            tool = MeshtasticTool()
            tool.display_stats()
            tool.cleanup()

        elif args.command == 'traceroute':
            conn_type = 'usb' if args.usb else None
            tool = MeshtasticTool(
                device_ip=args.ip, serial_port=args.port,
                connection_type=conn_type,
                verbose=getattr(args, 'verbose', False)
            )
            tool.is_traceroute_mode = True
            tool._connect_interface()
            tool.send_traceroute(destination_id=args.dest, hop_limit=args.hop_limit)
            try:
                timeout = 30
                start_time = time.time()
                while True:
                    time.sleep(1)
                    if tool.traceroute_completed:
                        break
                    if time.time() - start_time > timeout:
                        print("Traceroute timed out.")
                        break
            except KeyboardInterrupt:
                print("Traceroute interrupted by user.")
            finally:
                tool.cleanup()

        else:
            build_parser().print_help()

    except MeshtasticToolError as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Program interrupted by user.")
        sys.exit(0)


def cli_main():
    """Entry point: parse args and dispatch."""
    parser = build_parser()
    args = parser.parse_args()
    dispatch(args)
