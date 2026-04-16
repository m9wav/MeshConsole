"""
MeshConsole Web Server
-----------------------
Flask application and all HTTP routes, extracted from MeshtasticTool.start_web_server().

Usage:
    app = create_app(orchestrator)
    app.run(host='127.0.0.1', port=5055)

Author: M9WAV
License: MIT
"""

import configparser
import hashlib
import importlib.resources
import json
import logging
import os
import secrets
import time

from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, render_template, jsonify, Response, request, session
from flask_cors import CORS

from meshconsole.models import BackendType

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE = 'config.ini'


class _ResponseCache:
    """TTL cache with optional generation-aware invalidation."""

    def __init__(self):
        self._store = {}  # key -> (timestamp, response_json, generation)

    def get(self, key, ttl, generation=None):
        entry = self._store.get(key)
        if not entry:
            return None
        # If generation is provided and unchanged, serve regardless of TTL
        if generation is not None and len(entry) >= 3 and entry[2] == generation:
            return entry[1]
        if (time.time() - entry[0]) < ttl:
            return entry[1]
        return None

    def set(self, key, value, generation=None):
        self._store[key] = (time.time(), value, generation)


# ── Authentication helpers ────────────────────────────────────────

def hash_password(password):
    """Hash a password for secure storage."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def check_password(password, hashed):
    """Check if password matches the stored hash."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest() == hashed


def _make_require_auth(config):
    """Build a require_auth decorator bound to the given config."""
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
    return require_auth


# ── App factory ───────────────────────────────────────────────────

def create_app(orchestrator):
    """Create and configure the Flask application.

    Args:
        orchestrator: The MeshConsole orchestrator instance that provides
                      access to backends, database, node resolution, etc.

    Returns:
        A configured Flask app ready to run.
    """
    # Resolve template directory
    try:
        template_dir = importlib.resources.files('meshconsole') / 'templates'
        app = Flask(__name__, template_folder=str(template_dir))
    except (TypeError, AttributeError):
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        app = Flask(__name__, template_folder=template_dir)

    app.secret_key = secrets.token_hex(32)

    # Get config from orchestrator
    config = orchestrator.config

    # Configure CORS
    cors_enabled = config.getboolean('Security', 'cors_enabled', fallback=False)
    if cors_enabled:
        cors_origins = config.get('Security', 'cors_origins', fallback='http://localhost,http://127.0.0.1').split(',')
        CORS(app, resources={
            r"/packets": {"origins": cors_origins},
            r"/send-message": {"origins": cors_origins},
            r"/traceroute": {"origins": cors_origins},
            r"/stats": {"origins": cors_origins},
            r"/export": {"origins": cors_origins},
            r"/auth/*": {"origins": cors_origins}
        })

    require_auth = _make_require_auth(config)
    _cache = _ResponseCache()

    # ── Routes ────────────────────────────────────────────────

    @app.route('/')
    def index():
        from meshconsole import __version__
        return render_template('index.html', version=__version__)

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
            else:
                return jsonify({'success': False, 'error': 'Invalid password'}), 401
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return jsonify({'success': False, 'error': 'Authentication failed'}), 500

    @app.route('/auth/logout', methods=['POST'])
    def logout():
        session.clear()
        return jsonify({'success': True, 'message': 'Logged out successfully'})

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
        max_packets = orchestrator.max_packets_memory
        limit = int(request.args.get('limit', max_packets))
        offset = int(request.args.get('offset', 0))
        port_filter = request.args.get('port_filter', '')
        node_filter = request.args.get('node_filter', '')
        backend_filter = request.args.get('backend', '')
        device_filter = request.args.get('device_id', '')
        unique_locations = request.args.get('unique_locations', '') == '1'

        if node_filter or port_filter or unique_locations or backend_filter or device_filter:
            effective_port_filter = 'POSITION_APP,NODEINFO_APP,NODEINFO' if unique_locations else (port_filter or None)
            db_limit = max_packets if unique_locations else (offset + limit)
            packets = orchestrator.db_handler.fetch_packets_filtered(
                node_filter=node_filter or None,
                port_filter=effective_port_filter,
                limit=db_limit,
                backend=backend_filter or None,
                device_id=device_filter or None,
            )

            if unique_locations:
                seen_locations = {}
                for packet in packets:
                    lat = packet.get('latitude')
                    lon = packet.get('longitude')
                    if lat is not None and lon is not None:
                        location_key = (round(lat, 5), round(lon, 5))
                        if location_key not in seen_locations:
                            seen_locations[location_key] = packet
                packets = list(seen_locations.values())

            # Bulk-resolve node names in one pass instead of per-packet
            all_ids = set()
            for packet in packets:
                all_ids.add(packet.get('from_id', ''))
                all_ids.add(packet.get('to_id', ''))
            all_ids.discard('')
            name_map = orchestrator.resolve_node_names_bulk(all_ids)
            for packet in packets:
                packet['from_name'] = name_map.get(packet.get('from_id', ''), packet.get('from_id', ''))
                packet['to_name'] = name_map.get(packet.get('to_id', ''), packet.get('to_id', ''))

            total_packets = len(packets)
            paginated_packets = packets[offset:offset + limit]
        else:
            with orchestrator.latest_packets_lock:
                packets = list(orchestrator.latest_packets)
            total_packets = len(packets)
            packets = packets[::-1]
            paginated_packets = packets[offset:offset + limit]

        # Ensure all packets have top-level latitude/longitude for inline maps
        for packet in paginated_packets:
            if not packet.get('latitude'):
                raw = packet.get('raw_packet', {})
                port = packet.get('port_name', '')
                be = packet.get('backend', '')
                if port in ('POSITION_APP', 'POSITION'):
                    pos = raw.get('decoded', {}).get('position', {})
                    lat = pos.get('latitude', pos.get('latitudeI', 0) / 1e7 if 'latitudeI' in pos else None)
                    lon = pos.get('longitude', pos.get('longitudeI', 0) / 1e7 if 'longitudeI' in pos else None)
                    if lat and lon:
                        packet['latitude'] = lat
                        packet['longitude'] = lon
                        packet['altitude'] = pos.get('altitude', 0)
                elif port in ('NODEINFO', 'NODEINFO_APP') and be == 'meshcore':
                    lat = raw.get('adv_lat') or raw.get('latitude')
                    lon = raw.get('adv_lon') or raw.get('longitude')
                    if lat and lon:
                        packet['latitude'] = lat
                        packet['longitude'] = lon

        # Enrich MeshCore packets with decoded route data + target resolution
        for packet in paginated_packets:
            raw = packet.get('raw_packet', {})
            if packet.get('backend') == 'meshcore' and isinstance(raw, dict):
                path = raw.get('path', '')
                if path and len(path) >= 4:
                    hash_size = raw.get('path_hash_size', 1) or 1
                    packet['route_hops'] = orchestrator.decode_route(path, hash_size)

                # Resolve routing target from pkt_payload.
                # pkt_payload first 20 bytes = SHA256(target_pubkey)[:20]
                pkt_payload = raw.get('pkt_payload', '')
                pt = raw.get('payload_typename', '')
                if pt in ('REQ', 'RESPONSE', 'ANON_REQ') and pkt_payload and len(pkt_payload) >= 2:
                    # Byte 0 = target 1-byte hash, Byte 1 = sender 1-byte hash (REQ/RESPONSE)
                    # ANON_REQ: Byte 0 = target hash, Bytes 1-64 = sender full pubkey
                    # Build a mini route: [target_hash] + the RX path hops for context
                    rx_path = raw.get('path', '')
                    rx_hs = raw.get('path_hash_size', 1) or 1
                    context_path = pkt_payload[:2] + rx_path[:rx_hs * 2 * 3]  # target + up to 3 nearby hops
                    target_hops = orchestrator.decode_route(context_path, 1)
                    target_info = {'name': None, 'candidates': 0, 'confidence': 0}
                    if target_hops and target_hops[0]['candidates'] == 1:
                        target_info = {
                            'name': target_hops[0]['candidate_names'][0],
                            'candidates': 1,
                            'confidence': target_hops[0].get('confidence', 1.0),
                        }
                    elif target_hops and target_hops[0]['candidates'] > 1:
                        hop = target_hops[0]
                        target_info = {
                            'name': hop.get('name'),
                            'candidates': hop['candidates'],
                            'confidence': hop.get('confidence', 0.3),
                            'candidate_names': hop.get('candidate_names', []),
                        }

                    # Resolve sender
                    sender_info = None
                    if pt == 'ANON_REQ' and len(pkt_payload) >= 66:
                        # Bytes 1-64 (hex chars 2-66) = sender full pubkey
                        sender_pk = pkt_payload[2:66].lower()
                        sender_prefix = sender_pk[:12]
                        for b in orchestrator.backends:
                            if b.backend_type.value != 'meshcore':
                                continue
                            for pfx, contact in getattr(b, '_contacts', {}).items():
                                fk = (contact.get('_full_pub_key', '') or '').lower()
                                if fk and fk[:12] == sender_prefix:
                                    sender_info = contact.get('adv_name', '') or pfx
                                    break
                            if sender_info:
                                break
                        if not sender_info:
                            sender_info = sender_prefix
                    elif pt in ('REQ', 'RESPONSE') and len(pkt_payload) >= 4:
                        # Use RX path context for geo-aware sender resolution
                        sender_hash = pkt_payload[2:4].lower()
                        sender_context = sender_hash + rx_path[:rx_hs * 2 * 3]
                        s_hops = orchestrator.decode_route(sender_context, 1)
                        if s_hops and s_hops[0]['candidates'] >= 1:
                            hop = s_hops[0]
                            if hop['candidates'] == 1 or hop.get('confidence', 0) >= 0.5:
                                sender_info = hop.get('name') or hop['candidate_names'][0]

                    packet['target_node'] = target_info
                    if sender_info:
                        packet['sender_node'] = sender_info

        try:
            response_data = {
                'packets': paginated_packets,
                'total': total_packets,
                'filtered': bool(port_filter or node_filter or unique_locations or backend_filter)
            }
            packets_json = json.dumps(response_data, default=orchestrator._json_serializer)
        except TypeError as e:
            logger.error(f"Failed to serialize packets: {e}")
            return jsonify({'error': 'Failed to serialize packets'}), 500
        return Response(packets_json, mimetype='application/json')

    @app.route('/nodes')
    def get_nodes():
        try:
            backend_filter = request.args.get('backend', '')

            cache_key = f"nodes:{backend_filter}"
            cached = _cache.get(cache_key, ttl=15)
            if cached:
                return Response(cached, mimetype='application/json')

            with orchestrator.db_handler.lock:
                # Build query with optional backend filter
                query = '''
                    SELECT p.from_id, p.raw_packet, p.timestamp, p.backend
                    FROM packets p
                    INNER JOIN (
                        SELECT from_id, MAX(timestamp) as max_ts
                        FROM packets
                        WHERE port_name IN ('NODEINFO_APP', 'NODEINFO')
                '''
                params = []
                if backend_filter:
                    query += ' AND backend = ?'
                    params.append(backend_filter)
                query += '''
                        GROUP BY from_id
                    ) latest ON p.from_id = latest.from_id AND p.timestamp = latest.max_ts
                    WHERE p.port_name IN ('NODEINFO_APP', 'NODEINFO')
                '''
                if backend_filter:
                    query += ' AND p.backend = ?'
                    params.append(backend_filter)
                query += ' ORDER BY p.timestamp DESC'

                orchestrator.db_handler.cursor.execute(query, params)
                rows = orchestrator.db_handler.cursor.fetchall()

            nodes_by_id = {}
            db_names = {}  # fallback names from raw_packet
            for row in rows:
                try:
                    node_id = row[0]
                    raw_packet = json.loads(row[1]) if row[1] else {}
                    node_backend = row[3] if len(row) > 3 and row[3] else ('meshcore' if node_id.startswith('mc:') else 'meshtastic')

                    if node_backend == 'meshcore':
                        db_name = (
                            raw_packet.get('adv_name', '')
                            or raw_packet.get('name', '')
                            or node_id
                        )
                        db_short = ''
                        hw_model = ''
                    else:
                        user = raw_packet.get('decoded', {}).get('user', {})
                        db_name = user.get('longName', node_id)
                        db_short = user.get('shortName', '')
                        hw_model = user.get('hwModel', '')

                    db_names[node_id] = db_name
                    nodes_by_id[node_id] = {
                        'id': node_id,
                        'longName': db_name,  # will be overridden by live name below
                        'shortName': db_short,
                        'hwModel': hw_model,
                        'lastSeen': row[2],
                        'backend': node_backend,
                    }
                except Exception:
                    continue

            # Bulk-resolve live names and short names
            live_names = orchestrator.resolve_node_names_bulk(list(nodes_by_id.keys()))
            short_names = orchestrator.node_short_name_map
            for node_id, node in nodes_by_id.items():
                live_name = live_names.get(node_id, node_id)
                if live_name and live_name != node_id:
                    node['longName'] = live_name
                live_short = short_names.get(node_id, '')
                if live_short:
                    node['shortName'] = live_short

            # Also pull live nodes from ALL backend contacts caches
            # (they may not have NODEINFO packets in DB yet)
            all_backends = getattr(orchestrator, 'backends', [])
            for b in all_backends:
                if backend_filter and b.backend_type.value != backend_filter:
                    continue
                if not b.is_connected:
                    continue
                try:
                    live_nodes = b.get_nodes()
                    for node_id, unified_node in live_nodes.items():
                        if node_id not in nodes_by_id:
                            nodes_by_id[node_id] = {
                                'id': node_id,
                                'longName': unified_node.display_name or node_id,
                                'shortName': unified_node.short_name or '',
                                'hwModel': '',
                                'lastSeen': unified_node.last_seen or '',
                                'backend': b.backend_type.value,
                                'device_id': b.device_id,
                            }
                        else:
                            # Update name from live cache if the DB one is just the raw ID
                            existing = nodes_by_id[node_id]
                            if existing['longName'] in (node_id, '', None) and unified_node.display_name:
                                existing['longName'] = unified_node.display_name
                except Exception as e:
                    logger.debug(f"Error fetching nodes from {b.backend_type.value}: {e}")

            # Filter out invalid/ghost nodes and the local node
            local_id = orchestrator.local_node_id
            valid_nodes = []
            for node in nodes_by_id.values():
                nid = node['id']
                # Skip the local node (it's us, not a network peer)
                if local_id and nid == local_id:
                    continue
                # Skip non-node IDs
                if nid in ('channel', 'broadcast', 'self', 'mc:unknown', 'mc:mesh'):
                    continue
                # Skip mc: nodes with purely numeric suffixes (pkt_hash artifacts)
                mc_suffix = nid.removeprefix('mc:')
                if nid.startswith('mc:') and mc_suffix.isdigit():
                    continue
                valid_nodes.append(node)

            resp_json = json.dumps({'nodes': valid_nodes})
            _cache.set(cache_key, resp_json)
            return Response(resp_json, mimetype='application/json')
        except Exception as e:
            logger.error(f"Error fetching nodes: {e}")
            return jsonify({'error': str(e)}), 500

    @app.route('/send-message', methods=['POST'])
    @require_auth
    def send_message_api():
        try:
            data = request.get_json()
            destination = data.get('destination')
            message = data.get('message')
            device_id = data.get('device_id')  # v3.2.0: optional routing

            if not destination or not message:
                return jsonify({'success': False, 'error': 'Missing destination or message'}), 400

            orchestrator.send_message(destination, message, device_id=device_id)
            # Log sent message to DB for thread view
            local_id = orchestrator.local_node_id or 'self'
            if device_id:
                for b in orchestrator.backends:
                    if b.device_id == device_id and b.local_node_id:
                        local_id = b.local_node_id
                        break
            backend_str = 'meshcore' if destination.startswith('mc:') else 'meshtastic'
            orchestrator.db_handler.log_message(
                timestamp=datetime.now().isoformat(),
                from_id=local_id,
                to_id=destination,
                port_name='TEXT_MESSAGE',
                message=message,
                backend=backend_str,
                device_id=device_id or '',
            )
            return jsonify({'success': True, 'message': 'Message sent successfully'})
        except Exception as e:
            logger.error(f"Error sending message via API: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/channels')
    def get_channels():
        """Return available MeshCore channels with activity data."""
        try:
            cached = _cache.get('channels-list', ttl=10)
            if cached:
                return Response(cached, mimetype='application/json')

            # Live channel configs from devices
            live_channels = []
            for b in orchestrator.backends:
                if b.is_connected and hasattr(b, 'get_channels'):
                    btype = 'meshcore' if b.backend_type == BackendType.MESHCORE else 'meshtastic'
                    dev_name = getattr(b, '_device_name', '') or b.device_id
                    for ch in b.get_channels():
                        ch['device_id'] = b.device_id
                        ch['device_name'] = dev_name
                        ch['backend'] = btype
                        live_channels.append(ch)

            # Channel activity from database (cross-device merged)
            db_channels = orchestrator.db_handler.fetch_channel_conversations()
            activity_map = {c['channel_name']: c for c in db_channels}

            # Merge: combine live config with DB activity, dedup by name
            merged = {}
            for lc in live_channels:
                name = lc['name']
                if name not in merged:
                    act = activity_map.get(name, {})
                    merged[name] = {
                        'name': name,
                        'index': lc['index'],
                        'device_id': lc.get('device_id', ''),
                        'devices': [{'id': lc.get('device_id', ''), 'name': lc.get('device_name', lc.get('device_id', '')), 'backend': lc.get('backend', '')}],
                        'backend': lc.get('backend', ''),
                        'message_count': act.get('message_count', 0),
                        'last_timestamp': act.get('last_timestamp', ''),
                        'last_message': act.get('last_message', ''),
                        'last_sender_id': act.get('last_sender_id', ''),
                    }
                else:
                    merged[name]['devices'].append({'id': lc.get('device_id', ''), 'name': lc.get('device_name', lc.get('device_id', '')), 'backend': lc.get('backend', '')})
                    if lc.get('backend') and lc['backend'] != merged[name].get('backend'):
                        merged[name]['backend'] = 'both'

            # Include DB-only channels (device disconnected but history exists)
            for dbc in db_channels:
                if dbc['channel_name'] not in merged:
                    merged[dbc['channel_name']] = {
                        'name': dbc['channel_name'],
                        'index': -1,
                        'device_id': '',
                        'devices': [],
                        'message_count': dbc.get('message_count', 0),
                        'last_timestamp': dbc.get('last_timestamp', ''),
                        'last_message': dbc.get('last_message', ''),
                        'last_sender_id': dbc.get('last_sender_id', ''),
                    }

            # Sort by last activity, resolve sender names
            channel_list = sorted(merged.values(),
                                  key=lambda c: c.get('last_timestamp', ''),
                                  reverse=True)
            # Only resolve node-style IDs, not bare names from MeshCore
            node_ids = [c['last_sender_id'] for c in channel_list
                        if c.get('last_sender_id', '').startswith(('mc:', '!'))]
            name_map = orchestrator.resolve_node_names_bulk(list(set(node_ids))) if node_ids else {}
            for c in channel_list:
                sid = c.get('last_sender_id', '')
                c['last_sender_name'] = name_map.get(sid, sid)

            resp_json = json.dumps({'channels': channel_list})
            _cache.set('channels-list', resp_json)
            return Response(resp_json, mimetype='application/json')
        except Exception as e:
            return jsonify({'channels': [], 'error': str(e)}), 500

    @app.route('/channel-messages/<path:channel_name>')
    def get_channel_messages(channel_name):
        """Return messages for a specific channel."""
        try:
            limit = int(request.args.get('limit', 1000))
            hours = int(request.args.get('hours', 48))
            search = request.args.get('search', '').strip() or None

            cache_key = f"ch-msgs:{channel_name}:{hours}:{search or ''}"
            cached = _cache.get(cache_key, ttl=15)
            if cached:
                return Response(cached, mimetype='application/json')

            messages = orchestrator.db_handler.fetch_channel_messages(
                channel_name, limit=limit, hours=hours, search=search
            )
            # Only resolve IDs that look like node addresses (mc: or !)
            # MeshCore channel senders like "Major Distraction" are already names
            node_ids = [m['from_id'] for m in messages
                        if m['from_id'].startswith(('mc:', '!', 'channel'))]
            name_map = orchestrator.resolve_node_names_bulk(list(set(node_ids))) if node_ids else {}
            local_ids = set(orchestrator.get_local_node_ids())
            for m in messages:
                m['from_name'] = name_map.get(m['from_id'], m['from_id'])
                m['is_self'] = m['from_id'] in local_ids
            resp_json = json.dumps({'channel_name': channel_name, 'messages': messages})
            _cache.set(cache_key, resp_json)
            return Response(resp_json, mimetype='application/json')
        except Exception as e:
            logger.error(f"Error fetching channel messages: {e}")
            return jsonify({'messages': [], 'error': str(e)}), 500

    @app.route('/send-channel', methods=['POST'])
    @require_auth
    def send_channel_message_api():
        """Send a message to a MeshCore channel."""
        try:
            data = request.get_json()
            channel_idx = data.get('channel')
            message = data.get('message')
            device_id = data.get('device_id')

            if channel_idx is None or not message:
                return jsonify({'success': False, 'error': 'Missing channel or message'}), 400

            orchestrator.send_channel_message(int(channel_idx), message, device_id=device_id)
            return jsonify({'success': True, 'message': 'Channel message sent'})
        except Exception as e:
            logger.error(f"Error sending channel message: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/set-channel', methods=['POST'])
    @require_auth
    def set_channel_api():
        """Set a MeshCore channel name."""
        try:
            data = request.get_json()
            channel_idx = data.get('channel')
            channel_name = data.get('name', '').strip()
            device_id = data.get('device_id')

            if channel_idx is None or not channel_name:
                return jsonify({'success': False, 'error': 'Missing channel index or name'}), 400

            orchestrator.set_channel(int(channel_idx), channel_name, device_id=device_id)
            return jsonify({'success': True})
        except Exception as e:
            logger.error(f"Error setting channel: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    # ── Conversations / Private Messaging ────────────────────

    @app.route('/conversations')
    @require_auth
    def get_conversations():
        """Return list of conversation threads."""
        try:
            local_ids = orchestrator.get_local_node_ids()
            conversations = orchestrator.db_handler.fetch_conversations(local_ids)
            # Bulk-resolve node names
            conv_ids = [c['node_id'] for c in conversations]
            name_map = orchestrator.resolve_node_names_bulk(conv_ids)
            for conv in conversations:
                conv['node_name'] = name_map.get(conv['node_id'], conv['node_id'])
            return jsonify({'conversations': conversations})
        except Exception as e:
            logger.error(f"Error fetching conversations: {e}")
            return jsonify({'conversations': [], 'error': str(e)}), 500

    @app.route('/conversations/<path:node_id>')
    @require_auth
    def get_thread(node_id):
        """Return messages for a conversation thread."""
        try:
            local_ids = orchestrator.get_local_node_ids()
            limit = int(request.args.get('limit', 50))
            messages = orchestrator.db_handler.fetch_thread(node_id, local_ids, limit)
            # Bulk-resolve names
            msg_ids = set()
            for msg in messages:
                msg_ids.add(msg['from_id'])
                msg_ids.add(msg['to_id'])
            msg_ids.add(node_id)
            name_map = orchestrator.resolve_node_names_bulk(msg_ids)
            for msg in messages:
                msg['from_name'] = name_map.get(msg['from_id'], msg['from_id'])
                msg['to_name'] = name_map.get(msg['to_id'], msg['to_id'])
            return jsonify({'messages': messages, 'node_id': node_id,
                            'node_name': name_map.get(node_id, node_id)})
        except Exception as e:
            logger.error(f"Error fetching thread: {e}")
            return jsonify({'messages': [], 'error': str(e)}), 500

    @app.route('/conversations/<path:node_id>/send', methods=['POST'])
    @require_auth
    def send_thread_reply(node_id):
        """Send a reply in a conversation thread."""
        try:
            data = request.get_json()
            message = data.get('message', '')
            device_id = data.get('device_id')
            if not message:
                return jsonify({'success': False, 'error': 'Empty message'}), 400
            orchestrator.send_message(node_id, message, device_id=device_id)
            # Log sent message to DB so it appears in the thread
            local_id = orchestrator.local_node_id or 'self'
            if device_id:
                for b in orchestrator.backends:
                    if b.device_id == device_id and b.local_node_id:
                        local_id = b.local_node_id
                        break
            backend_str = 'meshcore' if node_id.startswith('mc:') else 'meshtastic'
            orchestrator.db_handler.log_message(
                timestamp=datetime.now().isoformat(),
                from_id=local_id,
                to_id=node_id,
                port_name='TEXT_MESSAGE',
                message=message,
                backend=backend_str,
                device_id=device_id or '',
            )
            return jsonify({'success': True})
        except Exception as e:
            logger.error(f"Error sending thread reply: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/traceroute', methods=['POST'])
    @require_auth
    def traceroute_api():
        try:
            data = request.get_json()
            destination = data.get('destination')
            hop_limit = data.get('hopLimit', 10)

            if not destination:
                return jsonify({'success': False, 'error': 'Missing destination'}), 400

            orchestrator.clear_traceroute_results()
            orchestrator.send_traceroute(destination, hop_limit)

            # For MeshCore, use a shorter timeout — direct connections won't
            # produce PATH_RESPONSE events
            is_meshcore = destination.startswith('mc:')
            timeout = 10 if is_meshcore else 30
            start_time = time.time()
            while not orchestrator.traceroute_completed and (time.time() - start_time) < timeout:
                time.sleep(0.1)

            results = orchestrator.get_traceroute_results()
            if results:
                return jsonify(results)
            else:
                return jsonify({
                    'success': False,
                    'error': 'Traceroute timed out — node may be unreachable or out of range',
                    'timeout': True
                })
        except Exception as e:
            logger.error(f"Error running traceroute via API: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/traceroute-results')
    def get_traceroute_results():
        try:
            results = orchestrator.get_traceroute_results()
            if results:
                return jsonify(results)
            else:
                return jsonify({'success': False, 'error': 'No traceroute results available'})
        except Exception as e:
            logger.error(f"Error fetching traceroute results: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/status')
    def get_status():
        try:
            now = datetime.now()
            server_uptime = int((now - orchestrator.server_start_time).total_seconds()) if orchestrator.server_start_time else 0
            connection_uptime = int((now - orchestrator.connection_start_time).total_seconds()) if orchestrator.connection_start_time else 0
            connected = orchestrator.is_connected

            response = {
                'connected': connected,
                'server_start': orchestrator.server_start_time.isoformat() if orchestrator.server_start_time else None,
                'connection_start': orchestrator.connection_start_time.isoformat() if orchestrator.connection_start_time else None,
                'server_uptime_seconds': server_uptime,
                'connection_uptime_seconds': connection_uptime,
                'local_node_id': orchestrator.local_node_id,
            }

            # Per-backend status
            if hasattr(orchestrator, 'get_backend_status'):
                response['backends'] = orchestrator.get_backend_status()
                response['backend_mode'] = getattr(orchestrator, 'backend_mode', 'meshtastic')
            else:
                response['backends'] = {
                    'meshtastic': {
                        'connected': connected,
                        'local_node_id': orchestrator.local_node_id,
                    }
                }
                response['backend_mode'] = 'meshtastic'

            # v3.2.0: also include flat list for multi-device UI
            if hasattr(orchestrator, 'get_backend_status_list'):
                response['backends_list'] = orchestrator.get_backend_status_list()

            return jsonify(response)
        except Exception as e:
            logger.error(f"Error fetching status: {e}")
            return jsonify({'error': str(e), 'connected': False}), 500

    # ── Flood advertisement endpoints ─────────────────────────

    @app.route('/meshcore/devices')
    def meshcore_devices():
        """Return connected MeshCore devices (for flood advertisement UI)."""
        try:
            devices = orchestrator.get_meshcore_devices()
            return jsonify({'devices': devices})
        except Exception as e:
            logger.error(f"Error fetching MeshCore devices: {e}")
            return jsonify({'devices': [], 'error': str(e)}), 500

    @app.route('/meshcore/flood-advert', methods=['POST'])
    @require_auth
    def flood_advert():
        """Send a flooded advertisement from a MeshCore device."""
        try:
            data = request.get_json() or {}
            device_id = data.get('device_id')
            result = orchestrator.send_flood_advertisement(device_id=device_id)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Error sending flood advertisement: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/network-map-data')
    def network_map_data():
        """Return all nodes with their last known coordinates for the map."""
        try:
            cached = _cache.get('network-map-data', ttl=30)
            if cached:
                return Response(cached, mimetype='application/json')

            nodes_with_coords = []
            seen_ids = set()

            # Source 1: live MeshCore contacts with coordinates
            for b in getattr(orchestrator, 'backends', []):
                if not b.is_connected:
                    continue
                try:
                    live_nodes = b.get_nodes()
                    for node_id, un in live_nodes.items():
                        if node_id in seen_ids:
                            continue
                        if un.latitude and un.longitude:
                            seen_ids.add(node_id)
                            nodes_with_coords.append({
                                'id': node_id,
                                'name': un.display_name or node_id,
                                'lat': un.latitude,
                                'lon': un.longitude,
                                'backend': b.backend_type.value,
                                'last_seen': un.last_seen or '',
                                'is_local': node_id == b.local_node_id,
                            })
                except Exception:
                    pass

            # Source 2: most recent position/nodeinfo per node from DB
            with orchestrator.db_handler.lock:
                orchestrator.db_handler.cursor.execute('''
                    SELECT p.from_id, p.raw_packet, p.timestamp, p.backend
                    FROM packets p
                    INNER JOIN (
                        SELECT from_id, MAX(timestamp) AS max_ts
                        FROM packets
                        WHERE port_name IN ('POSITION_APP', 'NODEINFO', 'NODEINFO_APP')
                        GROUP BY from_id
                    ) latest ON p.from_id = latest.from_id AND p.timestamp = latest.max_ts
                    WHERE p.port_name IN ('POSITION_APP', 'NODEINFO', 'NODEINFO_APP')
                ''')
                rows = orchestrator.db_handler.cursor.fetchall()

            # First pass: collect nodes with coordinates and their DB-level names
            pending_nodes = []
            for row in rows:
                from_id = row[0]
                if from_id in seen_ids:
                    continue
                try:
                    raw = json.loads(row[1]) if row[1] else {}
                    pkt_backend = row[3] if len(row) > 3 else 'meshtastic'

                    lat = lon = None
                    name = from_id
                    if pkt_backend == 'meshcore':
                        lat = raw.get('adv_lat') or raw.get('latitude')
                        lon = raw.get('adv_lon') or raw.get('longitude')
                        name = raw.get('adv_name', '') or from_id
                    else:
                        pos = raw.get('decoded', {}).get('position', {})
                        lat = pos.get('latitude')
                        lon = pos.get('longitude')
                        user = raw.get('decoded', {}).get('user', {})
                        name = user.get('longName', '') or from_id

                    if lat and lon and abs(float(lat)) > 0.01:
                        seen_ids.add(from_id)
                        pending_nodes.append({
                            'id': from_id, 'db_name': name,
                            'lat': float(lat), 'lon': float(lon),
                            'backend': pkt_backend, 'last_seen': row[2] or '',
                            'is_local': from_id == orchestrator.local_node_id,
                        })
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue

            # Bulk-resolve live names for all pending nodes
            if pending_nodes:
                live_names = orchestrator.resolve_node_names_bulk([n['id'] for n in pending_nodes])
                for n in pending_nodes:
                    live_name = live_names.get(n['id'], n['id'])
                    nodes_with_coords.append({
                        'id': n['id'],
                        'name': live_name if live_name != n['id'] else n['db_name'],
                        'lat': n['lat'], 'lon': n['lon'],
                        'backend': n['backend'],
                        'last_seen': n['last_seen'],
                        'is_local': n['is_local'],
                    })

            # Add local node if it has coordinates and wasn't found
            for b in getattr(orchestrator, 'backends', []):
                if b.local_node_id and b.local_node_id not in seen_ids:
                    nodes_with_coords.append({
                        'id': b.local_node_id,
                        'name': getattr(b, '_device_name', '') or b.local_node_id,
                        'lat': None, 'lon': None,
                        'backend': b.backend_type.value,
                        'last_seen': '',
                        'is_local': True,
                    })

            resp_json = json.dumps({'nodes': nodes_with_coords}, default=orchestrator._json_serializer)
            _cache.set('network-map-data', resp_json)
            return Response(resp_json, mimetype='application/json')
        except Exception as e:
            logger.error(f"Error fetching network map data: {e}")
            return jsonify({'nodes': [], 'error': str(e)}), 500

    @app.route('/mesh-graph')
    def get_mesh_graph():
        """Return graph data for D3.js mesh topology visualization."""
        try:
            max_nodes = int(request.args.get('max_nodes', 80))
            min_count = int(request.args.get('min_count', 2))
            device_ids = request.args.get('device_ids', '')
            device_id_list = [d.strip() for d in device_ids.split(',') if d.strip()] if device_ids else None
            focus_node = request.args.get('focus_node', '').strip() or None
            max_hops = int(request.args.get('max_hops', 2))

            cache_key = f"graph:{max_nodes}:{min_count}:{device_ids}:{focus_node}:{max_hops}"
            gen = orchestrator.route_analyzer._graph_generation
            cached = _cache.get(cache_key, ttl=30, generation=gen)
            if cached:
                return Response(cached, mimetype='application/json')

            graph_data = orchestrator.get_mesh_graph_data(
                max_nodes=max_nodes, min_count=min_count, device_ids=device_id_list,
                focus_node=focus_node, max_hops=max_hops,
            )
            resp_json = json.dumps(graph_data, default=orchestrator._json_serializer)
            _cache.set(cache_key, resp_json, generation=gen)
            return Response(resp_json, mimetype='application/json')
        except Exception as e:
            logger.error(f"Error fetching mesh graph data: {e}")
            return jsonify({'error': str(e), 'nodes': [], 'links': []}), 500

    @app.route('/stats')
    def get_stats():
        try:
            backend_filter = request.args.get('backend', None)

            cache_key = f"stats:{backend_filter or ''}"
            cached = _cache.get(cache_key, ttl=15)
            if cached:
                return Response(cached, mimetype='application/json')

            packet_count, node_count, port_usage = orchestrator.db_handler.fetch_packet_stats(backend=backend_filter)
            hours, hourly_packets, hourly_messages = orchestrator.db_handler.fetch_hourly_stats(backend=backend_filter)

            today = datetime.now().date()
            messages_today = 0
            # Copy list under lock, then process outside to minimize lock hold time
            with orchestrator.latest_packets_lock:
                packets_snapshot = list(orchestrator.latest_packets)
            for packet in packets_snapshot:
                try:
                    packet_date = datetime.fromisoformat(packet['timestamp']).date()
                    if packet_date == today and packet['port_name'] in ('TEXT_MESSAGE_APP', 'TEXT_MESSAGE'):
                        if not backend_filter or packet.get('backend') == backend_filter:
                            messages_today += 1
                except Exception:
                    pass

            port_usage_dict = {port: count for port, count in port_usage}

            network_health = orchestrator.db_handler.fetch_network_health(backend=backend_filter)
            # Resolve busiest node name
            if network_health.get('busiest_node'):
                network_health['busiest_node_name'] = orchestrator.resolve_node_name(
                    network_health['busiest_node'])

            resp_data = {
                'totalPackets': packet_count,
                'totalNodes': node_count,
                'messagesToday': messages_today,
                'portUsage': port_usage_dict,
                'hourlyData': {
                    'hours': hours,
                    'packets': hourly_packets,
                    'messages': hourly_messages
                },
                'networkHealth': network_health,
            }
            resp_json = json.dumps(resp_data)
            _cache.set(cache_key, resp_json)
            return Response(resp_json, mimetype='application/json')
        except Exception as e:
            logger.error(f"Error fetching stats via API: {e}")
            return jsonify({'error': str(e)}), 500

    @app.route('/export')
    def export_data_api():
        try:
            export_format = request.args.get('format', 'json')

            if export_format not in ['json', 'csv']:
                return jsonify({'error': 'Invalid format. Use json or csv.'}), 400

            packets = orchestrator.db_handler.fetch_packets(hours=48)

            if export_format == 'json':
                data = []
                for packet in packets:
                    data.append({
                        'timestamp': packet[0],
                        'from_id': packet[1],
                        'to_id': packet[2],
                        'port_name': packet[3],
                        'payload': packet[4],
                        'raw_packet': json.loads(packet[5])
                    })

                response_data = json.dumps(data, default=orchestrator._json_serializer, indent=2)
                response = Response(response_data, mimetype='application/json')
                response.headers['Content-Disposition'] = 'attachment; filename=meshtastic-data.json'

            elif export_format == 'csv':
                import io
                import csv

                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(['timestamp', 'from_id', 'to_id', 'port_name', 'payload', 'raw_packet'])

                for packet in packets:
                    writer.writerow(packet)

                response_data = output.getvalue()
                response = Response(response_data, mimetype='text/csv')
                response.headers['Content-Disposition'] = 'attachment; filename=meshtastic-data.csv'

            return response

        except Exception as e:
            logger.error(f"Error exporting data via API: {e}")
            return jsonify({'error': str(e)}), 500

    # ── Server-Sent Events ────────────────────────────────

    @app.route('/stream')
    def sse_stream():
        """SSE endpoint for real-time packet push.

        Connections auto-close after 60s to prevent thread exhaustion
        in gunicorn threaded mode. The browser's EventSource auto-reconnects.
        """
        def generate():
            q = orchestrator.subscribe_sse()
            start = time.time()
            try:
                while time.time() - start < 60:
                    try:
                        msg = q.get(timeout=10)
                        yield f"data: {json.dumps(msg)}\n\n"
                    except Exception:
                        yield ": heartbeat\n\n"
            except GeneratorExit:
                pass
            finally:
                orchestrator.unsubscribe_sse(q)

        return Response(generate(), mimetype='text/event-stream',
                        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

    # ── Device telemetry ──────────────────────────────────

    @app.route('/stats/devices')
    def get_device_stats():
        """Return per-device telemetry for enhanced Stats page."""
        try:
            cached = _cache.get('stats-devices', ttl=10)
            if cached:
                return Response(cached, mimetype='application/json')

            devices = orchestrator.get_device_telemetry()
            resp_json = json.dumps({'devices': devices})
            _cache.set('stats-devices', resp_json)
            return Response(resp_json, mimetype='application/json')
        except Exception as e:
            logger.error(f"Error fetching device stats: {e}")
            return jsonify({'devices': [], 'error': str(e)}), 500

    return app
