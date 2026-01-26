#!/usr/bin/env python3
"""
WSGI entry point for production deployment with Gunicorn/uWSGI.

This file is OPTIONAL - use it for production deployments with a WSGI server.
For simple usage, run meshconsole.py directly which has its own built-in Flask server.

STANDALONE MODE (development/simple use):
    python meshconsole.py --web

PRODUCTION MODE (with Gunicorn - recommended):
    gunicorn --workers 1 --threads 1 --bind 127.0.0.1:5055 --timeout 120 wsgi:application

Why use this wrapper for production?
- Better process management and graceful restarts
- Connection keepalive handling with automatic reconnection
- Designed for reverse proxy setups (nginx, caddy)
- Single worker recommended to maintain one device connection
"""

import threading
import sys
import os
import logging
import secrets
import time

# Change to script directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from meshconsole import MeshtasticTool, configure_logging
from flask import Flask, render_template, jsonify, Response, request, session
from flask_cors import CORS
from functools import wraps
from datetime import datetime, timedelta
import configparser
import json

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE = 'config.ini'

# Global tool instance - initialized lazily per worker
_tool = None
_tool_lock = threading.Lock()
_server_start_time = None
_connection_start_time = None

def get_tool():
    """Get or initialize the Meshtastic tool (lazy singleton per worker)."""
    global _tool, _server_start_time, _connection_start_time
    if _tool is None:
        with _tool_lock:
            if _tool is None:
                logger.info("Initializing Meshtastic Tool in worker...")
                _server_start_time = datetime.now()
                _tool = MeshtasticTool(web_enabled=True)
                _tool._connect_interface()
                _connection_start_time = datetime.now()

                # Load recent packets from database
                _tool._load_recent_packets_from_db()
                
                # Start a simple keepalive thread (no signal handlers)
                def keepalive_thread():
                    """Keep connection alive and handle reconnection."""
                    retry_delay = 1
                    max_retry_delay = 30
                    while True:
                        try:
                            time.sleep(5)
                            # Check connection health
                            if _tool.interface:
                                try:
                                    _ = len(_tool.interface.nodes)
                                except:
                                    raise ConnectionError("Interface check failed")
                            else:
                                raise ConnectionError("No interface")
                            retry_delay = 1
                        except Exception as e:
                            logger.error(f"Connection issue: {e}")
                            try:
                                if _tool.interface:
                                    _tool.interface.close()
                            except:
                                pass
                            _tool.interface = None
                            
                            logger.info(f"Reconnecting in {retry_delay}s...")
                            time.sleep(retry_delay)
                            try:
                                _tool._connect_interface()
                                global _connection_start_time
                                _connection_start_time = datetime.now()
                                logger.info("Reconnected successfully")
                                retry_delay = 1
                            except Exception as re:
                                logger.error(f"Reconnection failed: {re}")
                                retry_delay = min(retry_delay * 2, max_retry_delay)
                
                thread = threading.Thread(target=keepalive_thread, daemon=True)
                thread.start()
                logger.info("Meshtastic Tool initialized and keepalive started")
    return _tool

# Create Flask app
app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

# Load config
config = configparser.ConfigParser()
config.read(DEFAULT_CONFIG_FILE)

# Configure CORS
cors_enabled = config.getboolean('Security', 'cors_enabled', fallback=False)
if cors_enabled:
    cors_origins = config.get('Security', 'cors_origins', fallback='http://localhost').split(',')
    CORS(app, resources={r"/*": {"origins": cors_origins}})

# Authentication decorator
def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_password = config.get('Security', 'auth_password', fallback='')
        if not auth_password:
            return f(*args, **kwargs)
        if not session.get('authenticated'):
            return jsonify({'success': False, 'error': 'Authentication required', 'auth_required': True}), 401
        auth_timeout = config.getint('Security', 'auth_timeout', fallback=60)
        if 'auth_time' in session:
            auth_time = datetime.fromisoformat(session['auth_time'])
            if datetime.now() - auth_time > timedelta(minutes=auth_timeout):
                session.clear()
                return jsonify({'success': False, 'error': 'Session expired', 'auth_required': True}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def index():
    get_tool()  # Ensure initialized
    return render_template('index.html')

@app.route('/auth/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        password = data.get('password', '')
        auth_password = config.get('Security', 'auth_password', fallback='')
        if not auth_password:
            return jsonify({'success': False, 'error': 'Authentication not configured'}), 400
        if password == auth_password:
            session['authenticated'] = True
            session['auth_time'] = datetime.now().isoformat()
            return jsonify({'success': True, 'message': 'Authentication successful'})
        return jsonify({'success': False, 'error': 'Invalid password'}), 401
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/auth/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'success': True, 'message': 'Logged out'})

@app.route('/auth/status')
def auth_status():
    auth_password = config.get('Security', 'auth_password', fallback='')
    auth_required = bool(auth_password)
    if not auth_required:
        return jsonify({'auth_required': False, 'authenticated': True})
    authenticated = session.get('authenticated', False)
    if authenticated and 'auth_time' in session:
        auth_timeout = config.getint('Security', 'auth_timeout', fallback=60)
        auth_time = datetime.fromisoformat(session['auth_time'])
        if datetime.now() - auth_time > timedelta(minutes=auth_timeout):
            session.clear()
            authenticated = False
    return jsonify({'auth_required': auth_required, 'authenticated': authenticated})

@app.route('/packets')
def get_packets():
    tool = get_tool()
    max_packets = config.getint('Database', 'max_packets_memory', fallback=1000)
    limit = int(request.args.get('limit', max_packets))
    offset = int(request.args.get('offset', 0))
    port_filter = request.args.get('port_filter', '')
    node_filter = request.args.get('node_filter', '')
    unique_locations = request.args.get('unique_locations', '') == '1'

    # If filtering by node or port, query database for more complete results
    if node_filter or port_filter or unique_locations:
        # For unique locations, force POSITION_APP filter
        effective_port_filter = 'POSITION_APP' if unique_locations else (port_filter or None)
        packets = tool.db_handler.fetch_packets_filtered(
            node_filter=node_filter or None,
            port_filter=effective_port_filter,
            limit=max_packets
        )
        # Resolve node names using current node database
        for packet in packets:
            packet['from_name'] = tool._resolve_node_name(packet.get('from_id', ''))
            packet['to_name'] = tool._resolve_node_name(packet.get('to_id', ''))

        # If unique_locations, deduplicate by coordinates (keep most recent per location)
        if unique_locations:
            seen_locations = {}
            for packet in packets:
                lat = packet.get('latitude')
                lon = packet.get('longitude')
                alt = packet.get('altitude', 0)
                if lat is not None and lon is not None:
                    # Round to 5 decimal places (~1m precision) to group nearby positions
                    # Exclude altitude - same lat/lon at different altitudes treated as same location
                    location_key = (round(lat, 5), round(lon, 5))
                    if location_key not in seen_locations:
                        seen_locations[location_key] = packet
            packets = list(seen_locations.values())

        total_count = len(packets)
        # Already sorted by timestamp DESC from database
        paginated = packets[offset:offset + limit]
    else:
        # No filters - use in-memory cache for speed
        with tool.latest_packets_lock:
            packets = list(tool.latest_packets)

        total_count = len(packets)
        packets = packets[::-1]  # Newest first
        paginated = packets[offset:offset + limit]

    return Response(json.dumps({
        'packets': paginated,
        'total': total_count,
        'filtered': bool(port_filter or node_filter or unique_locations)
    }, default=tool._json_serializer), mimetype='application/json')

@app.route('/send-message', methods=['POST'])
@require_auth
def send_message():
    try:
        tool = get_tool()
        data = request.get_json()
        destination = data.get('destination')
        message = data.get('message')
        if not destination or not message:
            return jsonify({'success': False, 'error': 'Missing destination or message'}), 400
        tool.send_message(destination, message)
        return jsonify({'success': True, 'message': 'Message sent'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/traceroute', methods=['POST'])
@require_auth
def traceroute():
    try:
        tool = get_tool()
        data = request.get_json()
        destination = data.get('destination')
        hop_limit = data.get('hopLimit', 10)
        if not destination:
            return jsonify({'success': False, 'error': 'Missing destination'}), 400
        with tool.traceroute_results_lock:
            tool.traceroute_results = {}
        tool.traceroute_completed = False
        tool.send_traceroute(destination, hop_limit)
        timeout = 30
        start = time.time()
        while not tool.traceroute_completed and (time.time() - start) < timeout:
            time.sleep(0.1)
        with tool.traceroute_results_lock:
            if tool.traceroute_results:
                return jsonify(tool.traceroute_results)
            return jsonify({'success': False, 'error': 'Timeout', 'timeout': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/status')
def get_status():
    """Return server status including uptime and connection info."""
    try:
        tool = get_tool()
        now = datetime.now()

        # Calculate uptimes
        server_uptime_seconds = int((now - _server_start_time).total_seconds()) if _server_start_time else 0
        connection_uptime_seconds = int((now - _connection_start_time).total_seconds()) if _connection_start_time else 0

        # Check if interface is connected
        connected = tool.interface is not None

        return jsonify({
            'connected': connected,
            'server_start': _server_start_time.isoformat() if _server_start_time else None,
            'connection_start': _connection_start_time.isoformat() if _connection_start_time else None,
            'server_uptime_seconds': server_uptime_seconds,
            'connection_uptime_seconds': connection_uptime_seconds,
            'local_node_id': tool.local_node_id
        })
    except Exception as e:
        return jsonify({'error': str(e), 'connected': False}), 500

@app.route('/stats')
def get_stats():
    try:
        tool = get_tool()
        packet_count, node_count, port_usage = tool.db_handler.fetch_packet_stats()
        hours, hourly_packets, hourly_messages = tool.db_handler.fetch_hourly_stats()
        today = datetime.now().date()
        messages_today = 0
        with tool.latest_packets_lock:
            for p in tool.latest_packets:
                try:
                    if datetime.fromisoformat(p['timestamp']).date() == today and p['port_name'] == 'TEXT_MESSAGE_APP':
                        messages_today += 1
                except:
                    pass
        return jsonify({
            'totalPackets': packet_count,
            'totalNodes': node_count,
            'messagesToday': messages_today,
            'portUsage': {p: c for p, c in port_usage},
            'hourlyData': {
                'hours': hours,
                'packets': hourly_packets,
                'messages': hourly_messages
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export')
def export_data():
    try:
        tool = get_tool()
        fmt = request.args.get('format', 'json')
        # Only export last 48 hours of data
        packets = tool.db_handler.fetch_packets(hours=48)
        if fmt == 'json':
            data = [{'timestamp': p[0], 'from_id': p[1], 'to_id': p[2], 'port_name': p[3], 'payload': p[4], 'raw_packet': json.loads(p[5])} for p in packets]
            resp = Response(json.dumps(data, default=tool._json_serializer, indent=2), mimetype='application/json')
            resp.headers['Content-Disposition'] = 'attachment; filename=meshtastic-data.json'
            return resp
        elif fmt == 'csv':
            import csv, io
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['timestamp', 'from_id', 'to_id', 'port_name', 'payload', 'raw_packet'])
            for p in packets:
                writer.writerow(p)
            resp = Response(output.getvalue(), mimetype='text/csv')
            resp.headers['Content-Disposition'] = 'attachment; filename=meshtastic-data.csv'
            return resp
        return jsonify({'error': 'Invalid format'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# WSGI application
application = app

if __name__ == '__main__':
    # Use config values for host/port (for direct execution, though gunicorn is recommended)
    host = config.get('Web', 'host', fallback='127.0.0.1')
    port = config.getint('Web', 'port', fallback=5055)
    app.run(host=host, port=port, debug=False)
