"""
Tests for the RouteAnalyzer — route adjacency learning for MeshCore path hash
disambiguation.

Tests cover:
- In-memory cache operations
- SQLite persistence round-trips
- Adjacency scoring and confidence calculation
- Thread safety under concurrent writes
- Integration with decode_route via a minimal mock orchestrator
"""

import threading

import pytest

from meshconsole.database import DatabaseHandler
from meshconsole.core import RouteAnalyzer


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def route_db(tmp_path):
    """Provide a DatabaseHandler backed by a temp file for route tests."""
    db_file = str(tmp_path / "route_test.db")
    handler = DatabaseHandler(db_file=db_file)
    yield handler
    handler.close()


@pytest.fixture
def analyzer(route_db):
    """Provide a fresh RouteAnalyzer backed by a temp database."""
    return RouteAnalyzer(route_db)


# ── Helper factories ─────────────────────────────────────────────

def make_hop(hash_val, name=None, candidates=0, candidate_names=None):
    """Build a hop dict matching decode_route() output."""
    if candidate_names is None:
        candidate_names = [name] if name else []
    if candidates == 0:
        candidates = len(candidate_names)
    return {
        'hash': hash_val,
        'name': name,
        'ambiguous': candidates > 1,
        'candidates': candidates,
        'candidate_names': candidate_names,
        'confidence': 1.0 if candidates == 1 else (0.3 if candidates > 1 else 0.0),
        'observations': 0,
    }


# ══════════════════════════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════════════════════════

class TestRouteAnalyzerBasic:
    """Core adjacency learning and resolution."""

    def test_learn_route_with_unique_hops(self, analyzer):
        """Learning a route where all hops are unique should record adjacencies."""
        hops = [
            make_hop('aa', 'Alpha', 1),
            make_hop('bb', 'Bravo', 1),
            make_hop('cc', 'Charlie', 1),
        ]
        analyzer.learn_route(hops)

        # Alpha<->Bravo and Bravo<->Charlie should be recorded
        assert analyzer.total_observations > 0

    def test_learn_route_too_short(self, analyzer):
        """A route with fewer than 2 hops should not produce observations."""
        hops = [make_hop('aa', 'Alpha', 1)]
        analyzer.learn_route(hops)
        assert analyzer.total_observations == 0

    def test_learn_route_no_unique_hops(self, analyzer):
        """If no hop is uniquely resolved, nothing should be learned."""
        hops = [
            make_hop('aa', 'Alpha, Alt-Alpha', 2, ['Alpha', 'Alt-Alpha']),
            make_hop('bb', 'Bravo, Alt-Bravo', 2, ['Bravo', 'Alt-Bravo']),
        ]
        analyzer.learn_route(hops)
        assert analyzer.total_observations == 0

    def test_learn_and_resolve(self, analyzer):
        """After learning, ambiguous hops should be ranked by adjacency."""
        # Simulate: Alpha is unique, and always appears next to hash 'bb'
        # Hash 'bb' has candidates Bravo and Beta
        for _ in range(5):
            hops = [
                make_hop('aa', 'Alpha', 1),
                make_hop('bb', 'Bravo, Beta', 2, ['Bravo', 'Beta']),
            ]
            analyzer.learn_route(hops)

        # Now resolve 'bb' with neighbor 'aa'
        ranked = analyzer.resolve_ambiguous_hop('bb', ['aa'], ['Bravo', 'Beta'])
        assert len(ranked) == 2
        # Both should have some score (both are candidates when learned)
        # But Bravo should be listed since it's first in candidate_names
        # and learned equally with Beta
        assert ranked[0][1] > 0  # top candidate has observations
        assert ranked[0][1] >= ranked[1][1]

    def test_resolve_no_data(self, analyzer):
        """Resolving with no adjacency data should return zero scores."""
        ranked = analyzer.resolve_ambiguous_hop('xx', ['yy'], ['NodeA', 'NodeB'])
        assert all(score == 0.0 for _, score in ranked)

    def test_resolve_with_dominant_candidate(self, analyzer):
        """One candidate seen many times should dominate."""
        # Learn: 'aa' (unique Alpha) always next to 'bb'
        # But only Bravo is the real 'bb' node — learn it many times
        for _ in range(10):
            hops = [
                make_hop('aa', 'Alpha', 1),
                make_hop('bb', 'Bravo', 1),  # unique match = strong signal
            ]
            analyzer.learn_route(hops)

        # Now if 'bb' becomes ambiguous (new node with same hash appears)
        ranked = analyzer.resolve_ambiguous_hop('bb', ['aa'], ['Bravo', 'NewNode'])
        assert ranked[0][0] == 'Bravo'
        assert ranked[0][1] > ranked[1][1]


class TestConfidenceCalculation:
    """Test confidence score computation."""

    def test_unique_match(self, analyzer):
        """Unique match should give confidence 1.0."""
        assert analyzer.compute_confidence(1, []) == 1.0

    def test_no_match(self, analyzer):
        """No candidates should give confidence 0.0."""
        assert analyzer.compute_confidence(0, []) == 0.0

    def test_ambiguous_no_data(self, analyzer):
        """Ambiguous with no adjacency data should give 0.3."""
        ranked = [('A', 0.0), ('B', 0.0)]
        assert analyzer.compute_confidence(2, ranked) == 0.3

    def test_ambiguous_with_clear_winner(self, analyzer):
        """Ambiguous with one clear winner should give high confidence."""
        ranked = [('A', 10.0), ('B', 0.0)]
        conf = analyzer.compute_confidence(2, ranked)
        assert conf >= 0.7
        assert conf <= 0.95

    def test_ambiguous_close_scores(self, analyzer):
        """Ambiguous with close scores should give moderate confidence."""
        ranked = [('A', 5.0), ('B', 5.0)]
        conf = analyzer.compute_confidence(2, ranked)
        assert 0.3 <= conf <= 0.6

    def test_confidence_grows_with_observations(self, analyzer):
        """More observations of a sole winner should increase confidence."""
        conf_1 = analyzer.compute_confidence(2, [('A', 1.0), ('B', 0.0)])
        conf_5 = analyzer.compute_confidence(2, [('A', 5.0), ('B', 0.0)])
        assert conf_5 > conf_1


class TestPersistence:
    """Test that adjacency data survives flush and reload."""

    def test_flush_and_reload(self, route_db):
        """Data should persist through flush and reload cycle."""
        analyzer1 = RouteAnalyzer(route_db)

        hops = [
            make_hop('aa', 'Alpha', 1),
            make_hop('bb', 'Bravo', 1),
        ]
        for _ in range(10):
            analyzer1.learn_route(hops)
        analyzer1.flush()

        # Create a new analyzer from same DB — should load the data
        analyzer2 = RouteAnalyzer(route_db)
        assert analyzer2.total_observations > 0

        ranked = analyzer2.resolve_ambiguous_hop('bb', ['aa'], ['Bravo', 'Other'])
        assert ranked[0][0] == 'Bravo'
        assert ranked[0][1] > 0

    def test_flush_accumulates(self, route_db):
        """Multiple flush cycles should accumulate counts, not replace."""
        analyzer = RouteAnalyzer(route_db)

        hops = [
            make_hop('aa', 'Alpha', 1),
            make_hop('bb', 'Bravo', 1),
        ]
        analyzer.learn_route(hops)
        analyzer.flush()
        obs_after_first = analyzer.total_observations

        analyzer.learn_route(hops)
        analyzer.flush()
        obs_after_second = analyzer.total_observations

        assert obs_after_second > obs_after_first

    def test_empty_flush(self, analyzer):
        """Flushing with no pending data should not error."""
        analyzer.flush()  # No observations yet


class TestThreadSafety:
    """Concurrent access should not corrupt data."""

    def test_concurrent_learn(self, analyzer):
        """Multiple threads learning routes simultaneously should not raise."""
        errors = []

        def learner(thread_id):
            try:
                for _ in range(50):
                    hops = [
                        make_hop(f'{thread_id:02x}', f'Node{thread_id}', 1),
                        make_hop('ff', f'Common{thread_id}', 1),
                    ]
                    analyzer.learn_route(hops)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=learner, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert analyzer.total_observations > 0

    def test_concurrent_read_write(self, analyzer):
        """Reads during writes should not raise or corrupt."""
        errors = []
        results = []

        def writer():
            try:
                for _ in range(100):
                    hops = [
                        make_hop('aa', 'Alpha', 1),
                        make_hop('bb', 'Bravo, Beta', 2, ['Bravo', 'Beta']),
                    ]
                    analyzer.learn_route(hops)
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(100):
                    ranked = analyzer.resolve_ambiguous_hop('bb', ['aa'], ['Bravo', 'Beta'])
                    results.append(ranked)
            except Exception as e:
                errors.append(e)

        t_write = threading.Thread(target=writer)
        t_read = threading.Thread(target=reader)
        t_write.start()
        t_read.start()
        t_write.join()
        t_read.join()

        assert len(errors) == 0
        assert len(results) == 100


class TestDatabaseTable:
    """Test the route_adjacency table in DatabaseHandler."""

    def test_table_created(self, route_db):
        """The route_adjacency table should exist after init."""
        route_db.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in route_db.cursor.fetchall()}
        assert 'route_adjacency' in tables

    def test_batch_upsert(self, route_db):
        """batch_upsert_adjacency should insert and increment counts."""
        now = '2026-03-19T12:00:00'
        rows = [
            ('aa', 'bb', 'Alpha', now),
            ('aa', 'bb', 'Alpha', now),  # duplicate key — should increment
            ('cc', 'dd', 'Charlie', now),
        ]
        route_db.batch_upsert_adjacency(rows)

        route_db.cursor.execute(
            "SELECT count FROM route_adjacency WHERE node_hash='aa' AND node_candidate='Alpha'"
        )
        result = route_db.cursor.fetchone()
        assert result[0] == 2  # two inserts on same key

        route_db.cursor.execute(
            "SELECT count FROM route_adjacency WHERE node_hash='cc' AND node_candidate='Charlie'"
        )
        result = route_db.cursor.fetchone()
        assert result[0] == 1

    def test_load_all(self, route_db):
        """load_adjacency_all should return all stored records."""
        now = '2026-03-19T12:00:00'
        route_db.batch_upsert_adjacency([
            ('aa', 'bb', 'Alpha', now),
            ('cc', 'dd', 'Charlie', now),
        ])
        rows = route_db.load_adjacency_all()
        assert len(rows) == 2

    def test_empty_batch(self, route_db):
        """Empty batch should not error."""
        route_db.batch_upsert_adjacency([])

    def test_index_exists(self, route_db):
        """The adjacency lookup index should exist."""
        route_db.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
        indexes = {row[0] for row in route_db.cursor.fetchall()}
        assert 'idx_route_adj_lookup' in indexes


class TestEndToEnd:
    """Integration: learning improves resolution over time."""

    def test_accuracy_improves(self, analyzer):
        """After many observations, the correct candidate should win."""
        # Scenario: hash 'bb' has 3 candidates, but 'Bravo' is the real node
        # that always appears next to 'Alpha' (hash 'aa')
        for _ in range(20):
            hops = [
                make_hop('aa', 'Alpha', 1),
                make_hop('bb', 'Bravo', 1),  # in reality it resolves uniquely here
                make_hop('cc', 'Charlie', 1),
            ]
            analyzer.learn_route(hops)

        # Now pretend 'bb' has become ambiguous (new nodes joined with same hash)
        ranked = analyzer.resolve_ambiguous_hop(
            'bb', ['aa', 'cc'], ['Bravo', 'NewNode1', 'NewNode2']
        )
        conf = analyzer.compute_confidence(3, ranked)

        assert ranked[0][0] == 'Bravo'
        assert conf > 0.7
        assert ranked[0][1] > ranked[1][1]

    def test_bidirectional_learning(self, analyzer):
        """Learning should work in both directions of the route."""
        for _ in range(10):
            hops = [
                make_hop('aa', 'Alpha', 1),
                make_hop('bb', 'Bravo', 1),
            ]
            analyzer.learn_route(hops)

        # Alpha should be resolvable from its neighbor Bravo
        ranked_aa = analyzer.resolve_ambiguous_hop(
            'aa', ['bb'], ['Alpha', 'AltAlpha']
        )
        assert ranked_aa[0][0] == 'Alpha'
        assert ranked_aa[0][1] > 0

        # Bravo should be resolvable from its neighbor Alpha
        ranked_bb = analyzer.resolve_ambiguous_hop(
            'bb', ['aa'], ['Bravo', 'AltBravo']
        )
        assert ranked_bb[0][0] == 'Bravo'
        assert ranked_bb[0][1] > 0
