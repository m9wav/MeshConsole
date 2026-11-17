"""
Tests for the v3.15.2 efficiency work: time-window queries, the nodes_live
coordinate source, zstd-aware node endpoints and response compression.
"""

import configparser
import gzip
import json
import threading
from datetime import datetime, timedelta

from meshconsole.core import GeoResolver
from meshconsole.models import BackendType
from meshconsole.web import create_app
from tests.conftest import make_unified_packet


def _log(db, from_id, port, age, raw=None, backend=BackendType.MESHCORE):
    ts = (datetime.now() - age).isoformat()
    db.log_packet(make_unified_packet(
        from_id=from_id, port_name=port, timestamp=ts,
        raw_packet=raw or {}, backend=backend))


class TestTimeWindows:
    def test_network_health_counts_only_last_hour(self, tmp_db):
        for _ in range(3):
            _log(tmp_db, 'mc:busy', 'TEXT_MESSAGE', timedelta(minutes=5))
        _log(tmp_db, 'mc:quiet', 'TEXT_MESSAGE', timedelta(minutes=30))
        # Same calendar day but outside the window must not be counted.
        for _ in range(5):
            _log(tmp_db, 'mc:old', 'TEXT_MESSAGE', timedelta(hours=2))

        health = tmp_db.fetch_network_health()
        assert health['nodes_last_hour'] == 2
        assert health['packets_last_hour'] == 4
        assert health['busiest_node'] == 'mc:busy'
        assert health['busiest_count'] == 3

    def test_network_health_empty(self, tmp_db):
        health = tmp_db.fetch_network_health()
        assert health['packets_last_hour'] == 0
        assert health['busiest_node'] is None

    def test_network_health_backend_filter(self, tmp_db):
        _log(tmp_db, 'mc:a', 'TEXT_MESSAGE', timedelta(minutes=1))
        _log(tmp_db, '!abc', 'TEXT_MESSAGE_APP', timedelta(minutes=1),
             backend=BackendType.MESHTASTIC)
        assert tmp_db.fetch_network_health(backend='meshcore')['nodes_last_hour'] == 1

    def test_hourly_stats_excludes_older_than_24h(self, tmp_db):
        _log(tmp_db, 'mc:a', 'TEXT_MESSAGE', timedelta(minutes=1))
        _log(tmp_db, 'mc:a', 'TEXT_MESSAGE', timedelta(hours=30))
        _, packets, messages = tmp_db.fetch_hourly_stats()
        assert sum(packets) == 1
        assert sum(messages) == 1

    def test_fetch_packets_hours(self, tmp_db):
        _log(tmp_db, 'mc:a', 'TEXT_MESSAGE', timedelta(minutes=10))
        _log(tmp_db, 'mc:a', 'TEXT_MESSAGE', timedelta(hours=3))
        assert len(tmp_db.fetch_packets(hours=1)) == 1
        assert len(tmp_db.fetch_packets()) == 2


class TestGeoFromNodesLive:
    def test_coords_come_from_nodes_live(self, tmp_db):
        tmp_db.upsert_node('aa' * 32, 'meshcore', name='Hilltop', lat=50.7, lon=-3.5)
        tmp_db.upsert_node('bb' * 32, 'meshcore', name='NoGps')
        tmp_db.upsert_node('cc' * 32, 'meshtastic', name='OtherBackend', lat=51.0, lon=-1.0)

        class LiveBackend:
            backend_type = BackendType.MESHCORE
            _contacts = {'ddee': {'adv_name': 'Live', 'adv_lat': 52.0, 'adv_lon': 0.5}}

        geo = GeoResolver()
        geo.refresh_coords([LiveBackend()], tmp_db)
        assert geo.get_coords('Hilltop') == (50.7, -3.5)
        assert geo.get_coords('Live') == (52.0, 0.5)
        assert geo.get_coords('NoGps') is None
        assert geo.get_coords('OtherBackend') is None


class _Analyzer:
    _graph_generation = 0


class _Orchestrator:
    """Just enough of the orchestrator for the read-only web routes."""

    def __init__(self, db):
        self.config = configparser.ConfigParser()
        self.db_handler = db
        self.backends = []
        self.local_node_id = None
        self.node_short_name_map = {}
        self.latest_packets = []
        self.latest_packets_lock = threading.Lock()
        self.max_packets_memory = 1000
        self.route_analyzer = _Analyzer()

    def resolve_node_names_bulk(self, node_ids, db_fallback=True):
        return {n: n for n in node_ids}

    def resolve_node_name(self, node_id):
        return node_id

    @staticmethod
    def _json_serializer(obj):
        return str(obj)


class TestWeb:
    def _client(self, db):
        return create_app(_Orchestrator(db)).test_client()

    def test_nodes_reads_compressed_rows(self, tmp_db):
        """NODEINFO rows are stored zstd-compressed; they must still be listed."""
        _log(tmp_db, 'mc:aabbccddeeff', 'NODEINFO', timedelta(minutes=1),
             raw={'adv_name': 'Hilltop', 'adv_lat': 50.7, 'adv_lon': -3.5})
        client = self._client(tmp_db)

        nodes = client.get('/nodes').get_json()['nodes']
        assert [n['longName'] for n in nodes] == ['Hilltop']

        mapped = client.get('/network-map-data').get_json()['nodes']
        assert [(n['name'], n['lat'], n['lon']) for n in mapped] == [('Hilltop', 50.7, -3.5)]

    def test_large_json_is_gzipped(self, tmp_db):
        for i in range(60):
            _log(tmp_db, f'mc:{i:012x}', 'NODEINFO', timedelta(minutes=1),
                 raw={'adv_name': f'Node number {i}'})
        client = self._client(tmp_db)

        plain = client.get('/nodes')
        assert 'Content-Encoding' not in plain.headers

        resp = client.get('/nodes', headers={'Accept-Encoding': 'gzip, br'})
        assert resp.headers['Content-Encoding'] == 'gzip'
        assert 'Accept-Encoding' in resp.headers['Vary']
        assert int(resp.headers['Content-Length']) == len(resp.data) < len(plain.data)
        assert json.loads(gzip.decompress(resp.data)) == plain.get_json()

        # Second hit is served from the compression memo — same bytes.
        again = client.get('/nodes', headers={'Accept-Encoding': 'gzip'})
        assert again.data == resp.data

    def test_small_responses_left_alone(self, tmp_db):
        resp = self._client(tmp_db).get('/auth/status', headers={'Accept-Encoding': 'gzip'})
        assert 'Content-Encoding' not in resp.headers
        assert resp.get_json()['authenticated'] is True


class TestLatestPerNode:
    def test_one_newest_row_per_node(self, tmp_db):
        _log(tmp_db, 'mc:a', 'NODEINFO', timedelta(hours=3), raw={'adv_name': 'old'})
        _log(tmp_db, 'mc:a', 'NODEINFO', timedelta(hours=1), raw={'adv_name': 'new'})
        _log(tmp_db, 'mc:a', 'TEXT_MESSAGE', timedelta(minutes=1))
        _log(tmp_db, '!b', 'NODEINFO_APP', timedelta(hours=2), backend=BackendType.MESHTASTIC)
        _log(tmp_db, '!b', 'POSITION_APP', timedelta(minutes=5), backend=BackendType.MESHTASTIC)

        rows = tmp_db.fetch_latest_per_node(('POSITION_APP', 'NODEINFO', 'NODEINFO_APP'))
        assert [r[0] for r in rows] == ['!b', 'mc:a']          # newest first
        from meshconsole.database import decode_raw_packet
        assert decode_raw_packet(rows[1][1])['adv_name'] == 'new'

        only_info = tmp_db.fetch_latest_per_node(('NODEINFO', 'NODEINFO_APP'))
        assert {r[0] for r in only_info} == {'mc:a', '!b'}

        assert [r[0] for r in tmp_db.fetch_latest_per_node(
            ('NODEINFO', 'NODEINFO_APP'), backend='meshcore')] == ['mc:a']


class TestDecodeRouteCache:
    def _tool(self, tmp_db):
        from meshconsole.core import MeshtasticTool, RouteAnalyzer

        class Backend:
            backend_type = BackendType.MESHCORE
            _contacts_generation = 0
            _device_name = None
            _contacts = {
                'aa11': {'adv_name': 'Alpha', '_full_pub_key': 'aa11' + '0' * 60},
                'bb22': {'adv_name': 'Bravo', '_full_pub_key': 'bb22' + '0' * 60},
            }

        tmp_db.upsert_node('aa11' + '0' * 60, 'meshcore', name='Alpha', path_len=1)
        tmp_db.upsert_node('bb22' + '0' * 60, 'meshcore', name='Bravo', path_len=1)

        tool = MeshtasticTool.__new__(MeshtasticTool)
        tool.backends = [Backend()]
        tool.db_handler = tmp_db
        tool.route_analyzer = RouteAnalyzer(tmp_db)
        tool._nodeinfo_parsed_cache = None
        tool._nodeinfo_evidence_cache = None
        tool._nodeinfo_cache_ttl = 0          # worst case: always stale
        tool._decode_route_cache = {}
        tool._decode_route_gen = -1
        tool._decode_route_lookups = {}
        tool._decode_route_built = 0.0

        builds = []
        real = tool._build_route_lookup
        tool._build_route_lookup = lambda hs: (builds.append(hs), real(hs))[1]
        return tool, builds, Backend

    def test_lookup_built_once_per_hash_size(self, tmp_db):
        tool, builds, _ = self._tool(tmp_db)
        for _ in range(5):
            assert [h['name'] for h in tool.decode_route('aabb', 1)] == ['Alpha', 'Bravo']
            assert [h['name'] for h in tool.decode_route('aa11bb22', 2)] == ['Alpha', 'Bravo']
            tool.decode_route('bbaa', 1)
        assert sorted(builds) == [1, 2]

    def test_contact_change_invalidates(self, tmp_db):
        tool, builds, backend = self._tool(tmp_db)
        tool.decode_route('aabb', 1)
        backend._contacts_generation += 1
        tool.decode_route('aabb', 1)
        assert builds == [1, 1]


class TestStream:
    def test_stream_opens_immediately(self, tmp_db):
        """The first bytes must not wait for a packet or the 10 s heartbeat."""
        import queue
        import time

        orch = _Orchestrator(tmp_db)
        subscribed = []
        orch.subscribe_sse = lambda: subscribed.append(queue.Queue()) or subscribed[-1]
        orch.unsubscribe_sse = lambda q: subscribed.remove(q)

        resp = create_app(orch).test_client().get(
            '/stream', headers={'Accept-Encoding': 'gzip'}, buffered=False)
        t0 = time.time()
        first = next(resp.response)
        assert time.time() - t0 < 1
        assert first == b': connected\n\n'
        assert 'Content-Encoding' not in resp.headers
        resp.close()
        assert subscribed == []
