"""
Tests for DatabaseHandler — CRUD operations, filtering, migration, thread safety.
"""

import json
import os
import sqlite3
import tempfile
import threading

import pytest

from meshconsole.database import DatabaseHandler
from meshconsole.models import BackendType
from tests.conftest import make_unified_packet


class TestDatabaseSetup:
    """Verify schema creation and table structure."""

    def test_tables_exist(self, tmp_db):
        """Both 'messages' and 'packets' tables should be created."""
        tmp_db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in tmp_db.cursor.fetchall()}
        assert 'messages' in tables
        assert 'packets' in tables

    def test_backend_column_exists(self, tmp_db):
        """The backend column should be present after migration."""
        tmp_db.cursor.execute("PRAGMA table_info(packets)")
        columns = [row[1] for row in tmp_db.cursor.fetchall()]
        assert 'backend' in columns

        tmp_db.cursor.execute("PRAGMA table_info(messages)")
        columns = [row[1] for row in tmp_db.cursor.fetchall()]
        assert 'backend' in columns

    def test_indexes_exist(self, tmp_db):
        """Key indexes should be created."""
        tmp_db.cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = {row[0] for row in tmp_db.cursor.fetchall()}
        assert 'idx_packets_from_id' in indexes
        assert 'idx_packets_to_id' in indexes
        assert 'idx_packets_port_name' in indexes
        assert 'idx_packets_timestamp' in indexes
        assert 'idx_packets_backend' in indexes

    def test_migration_on_existing_db(self, tmp_path):
        """Migrating a pre-v3 database should add the backend column without data loss."""
        db_file = str(tmp_path / "legacy.db")
        # Create a "legacy" database without backend column
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        c.execute('''CREATE TABLE packets (
            timestamp TEXT, from_id TEXT, to_id TEXT,
            port_name TEXT, payload TEXT, raw_packet TEXT
        )''')
        c.execute('''CREATE TABLE messages (
            timestamp TEXT, from_id TEXT, to_id TEXT,
            port_name TEXT, message TEXT
        )''')
        c.execute("INSERT INTO packets VALUES (?, ?, ?, ?, ?, ?)",
                  ("2026-01-01T00:00:00", "!aabb", "!ccdd", "TEXT_MESSAGE_APP", "", "{}"))
        conn.commit()
        conn.close()

        # Now open with DatabaseHandler which should migrate
        handler = DatabaseHandler(db_file=db_file)

        # Check column was added
        handler.cursor.execute("PRAGMA table_info(packets)")
        columns = [row[1] for row in handler.cursor.fetchall()]
        assert 'backend' in columns

        # Check existing data got default value
        handler.cursor.execute("SELECT backend FROM packets")
        row = handler.cursor.fetchone()
        assert row[0] == 'meshtastic'

        handler.close()


class TestLogPacket:
    """Verify packet logging."""

    def test_log_packet_dict(self, tmp_db):
        """Logging a dict should insert a row."""
        pkt = {
            'timestamp': '2026-03-17T12:00:00',
            'from_id': '!aabbccdd',
            'to_id': '!11223344',
            'port_name': 'TEXT_MESSAGE_APP',
            'payload': 'hello',
            'raw_packet': {'decoded': {'text': 'hello'}},
            'backend': 'meshtastic',
        }
        tmp_db.log_packet(pkt)

        tmp_db.cursor.execute("SELECT COUNT(*) FROM packets")
        assert tmp_db.cursor.fetchone()[0] == 1

    def test_log_packet_with_enum_backend(self, tmp_db):
        """Backend as an enum should be stored as its string value."""
        pkt = {
            'timestamp': '2026-03-17T12:00:00',
            'from_id': '!aabbccdd',
            'to_id': '!11223344',
            'port_name': 'POSITION_APP',
            'payload': '',
            'raw_packet': {},
            'backend': BackendType.MESHTASTIC,
        }
        tmp_db.log_packet(pkt)

        tmp_db.cursor.execute("SELECT backend FROM packets")
        row = tmp_db.cursor.fetchone()
        assert row[0] == 'meshtastic'

    def test_log_packet_default_backend(self, tmp_db):
        """If backend key is missing, default to 'meshtastic'."""
        pkt = {
            'timestamp': '2026-03-17T12:00:00',
            'from_id': '!aabbccdd',
            'to_id': '!11223344',
            'port_name': 'TEXT_MESSAGE_APP',
            'payload': '',
            'raw_packet': {},
        }
        tmp_db.log_packet(pkt)

        tmp_db.cursor.execute("SELECT backend FROM packets")
        row = tmp_db.cursor.fetchone()
        assert row[0] == 'meshtastic'


class TestLogMessage:
    """Verify message logging."""

    def test_log_message_basic(self, tmp_db):
        tmp_db.log_message('2026-03-17T12:00:00', '!aa', '!bb', 'TEXT_MESSAGE_APP', 'Hi')
        tmp_db.cursor.execute("SELECT COUNT(*) FROM messages")
        assert tmp_db.cursor.fetchone()[0] == 1

    def test_log_message_with_backend(self, tmp_db):
        tmp_db.log_message('2026-03-17T12:00:00', '!aa', '!bb', 'TEXT_MESSAGE_APP', 'Hi', backend='meshcore')
        tmp_db.cursor.execute("SELECT backend FROM messages")
        assert tmp_db.cursor.fetchone()[0] == 'meshcore'


class TestFetchPackets:
    """Verify fetch_packets and fetch_packets_filtered."""

    def test_fetch_all(self, populated_db):
        packets = populated_db.fetch_packets()
        assert len(packets) == 10

    def test_fetch_with_backend_filter(self, populated_db):
        """All test packets have backend='meshtastic', so filtering should return all."""
        packets = populated_db.fetch_packets(backend='meshtastic')
        assert len(packets) == 10

        packets = populated_db.fetch_packets(backend='meshcore')
        assert len(packets) == 0

    def test_fetch_filtered_by_port(self, populated_db):
        packets = populated_db.fetch_packets_filtered(port_filter='TEXT_MESSAGE_APP')
        # Indices 0,2,4,6,8 are TEXT_MESSAGE_APP (5 packets)
        assert len(packets) == 5
        for p in packets:
            assert p['port_name'] == 'TEXT_MESSAGE_APP'

    def test_fetch_filtered_by_node(self, populated_db):
        packets = populated_db.fetch_packets_filtered(node_filter='!aabb0000')
        assert len(packets) == 1

    def test_fetch_filtered_limit(self, populated_db):
        packets = populated_db.fetch_packets_filtered(limit=3)
        assert len(packets) == 3

    def test_fetch_filtered_with_backend(self, populated_db):
        packets = populated_db.fetch_packets_filtered(backend='meshtastic')
        assert len(packets) == 10

        packets = populated_db.fetch_packets_filtered(backend='meshcore')
        assert len(packets) == 0


class TestLookupNodeName:
    """Verify node name lookup from NODEINFO packets."""

    def test_lookup_found(self, tmp_db):
        """Should return longName from NODEINFO_APP packet."""
        raw = {'decoded': {'user': {'longName': 'Alice Node'}}}
        pkt = {
            'timestamp': '2026-03-17T12:00:00',
            'from_id': '!aabbccdd',
            'to_id': '!ffffffff',
            'port_name': 'NODEINFO_APP',
            'payload': '',
            'raw_packet': raw,
            'backend': 'meshtastic',
        }
        tmp_db.log_packet(pkt)
        assert tmp_db.lookup_node_name('!aabbccdd') == 'Alice Node'

    def test_lookup_not_found(self, tmp_db):
        """Should return the node_id unchanged if no NODEINFO found."""
        assert tmp_db.lookup_node_name('!unknown') == '!unknown'


class TestPacketStats:
    """Verify statistics queries."""

    def test_stats_basic(self, populated_db):
        packet_count, node_count, port_usage = populated_db.fetch_packet_stats()
        assert packet_count == 10
        assert node_count == 10  # 10 different from_ids
        port_names = {p[0] for p in port_usage}
        assert 'TEXT_MESSAGE_APP' in port_names
        assert 'POSITION_APP' in port_names

    def test_stats_with_backend_filter(self, populated_db):
        packet_count, node_count, _ = populated_db.fetch_packet_stats(backend='meshcore')
        assert packet_count == 0

    def test_hourly_stats(self, populated_db):
        hours, packets, messages = populated_db.fetch_hourly_stats()
        assert len(hours) == 24
        assert len(packets) == 24
        assert len(messages) == 24


class TestThreadSafety:
    """Basic thread safety checks."""

    def test_concurrent_writes(self, tmp_db):
        """Multiple threads writing simultaneously should not raise."""
        errors = []

        def writer(thread_id):
            try:
                for i in range(20):
                    pkt = {
                        'timestamp': f'2026-03-17T12:{thread_id:02d}:{i:02d}',
                        'from_id': f'!{thread_id:08x}',
                        'to_id': '!11223344',
                        'port_name': 'TEXT_MESSAGE_APP',
                        'payload': f'msg-{thread_id}-{i}',
                        'raw_packet': {},
                        'backend': 'meshtastic',
                    }
                    tmp_db.log_packet(pkt)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0

        tmp_db.cursor.execute("SELECT COUNT(*) FROM packets")
        assert tmp_db.cursor.fetchone()[0] == 100  # 5 threads x 20 packets


class TestClose:
    """Verify closing the database."""

    def test_close_no_error(self, tmp_db):
        tmp_db.close()
        # Calling close again should not raise
        tmp_db.close()
