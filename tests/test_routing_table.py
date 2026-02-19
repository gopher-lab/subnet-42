import unittest
import os
import tempfile
from db.routing_table_database import RoutingTableDatabase
from validator.routing_table import RoutingTable
import sqlite3
from contextlib import closing


class TestRoutingTableDatabase(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        db_path = os.path.join(self._tmpdir.name, "routing_table.db")
        self.db = RoutingTableDatabase(db_path=db_path)

    def tearDown(self):
        # Clear the database after each test
        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM miner_addresses")
            cursor.execute("DELETE FROM worker_registry")
            cursor.execute("DELETE FROM unregistered_tees")
            conn.commit()

    def _insert_miner_address_row(self, hotkey, uid, address, worker_id=None):
        # Used to simulate legacy/dirty DB state (duplicates per hotkey).
        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO miner_addresses (hotkey, uid, address, worker_id)
                VALUES (?, ?, ?, ?)
                """,
                (hotkey, uid, address, worker_id),
            )
            conn.commit()

    def test_add_or_refresh_inserted(self):
        action, pruned = self.db.add_or_refresh_address_keep_newest(
            hotkey="hotkey1", uid="uid1", address="address1"
        )
        self.assertEqual(action, "inserted")
        self.assertEqual(pruned, 0)

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT hotkey, uid, address FROM miner_addresses WHERE address = ?",
                ("address1",),
            )
            row = cursor.fetchone()
            self.assertEqual(row, ("hotkey1", "uid1", "address1"))

    def test_add_or_refresh_refreshed(self):
        self.db.add_or_refresh_address_keep_newest(
            hotkey="hotkey1", uid="uid1", address="address1"
        )
        action, pruned = self.db.add_or_refresh_address_keep_newest(
            hotkey="hotkey1", uid="uid1", address="address1"
        )
        self.assertEqual(action, "refreshed")
        self.assertEqual(pruned, 0)

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM miner_addresses WHERE hotkey = ?", ("hotkey1",))
            self.assertEqual(cursor.fetchone()[0], 1)

    def test_add_or_refresh_uid_churn_replaces_old_row(self):
        self.db.add_or_refresh_address_keep_newest(
            hotkey="hotkey1", uid="uid1", address="address1"
        )
        self.db.add_or_refresh_address_keep_newest(
            hotkey="hotkey1", uid="uid1", address="address2"
        )

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT address FROM miner_addresses WHERE hotkey = ?",
                ("hotkey1",),
            )
            rows = cursor.fetchall()
            self.assertEqual(rows, [("address2",)])

    def test_add_or_refresh_orphaned_conflict_reassigns_address(self):
        # If address is only associated with 1 row on old hotkey, allow reuse.
        self.db.add_or_refresh_address_keep_newest(
            hotkey="old_hotkey", uid="uid1", address="address1"
        )
        action, pruned = self.db.add_or_refresh_address_keep_newest(
            hotkey="new_hotkey", uid="uid2", address="address1"
        )
        self.assertEqual(action, "inserted")
        self.assertEqual(pruned, 0)

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT hotkey FROM miner_addresses WHERE address = ?",
                ("address1",),
            )
            self.assertEqual(cursor.fetchone()[0], "new_hotkey")

    def test_add_or_refresh_active_conflict_skips(self):
        # Simulate an old hotkey with multiple rows (legacy/dirty DB state).
        self._insert_miner_address_row("old_hotkey", "uid1", "address1")
        self._insert_miner_address_row("old_hotkey", "uid2", "address2")

        action, pruned = self.db.add_or_refresh_address_keep_newest(
            hotkey="new_hotkey", uid="uid3", address="address1"
        )
        self.assertEqual(action, "skipped_conflict")
        self.assertEqual(pruned, 0)

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT hotkey FROM miner_addresses WHERE address = ?",
                ("address1",),
            )
            self.assertEqual(cursor.fetchone()[0], "old_hotkey")

    def test_prune_all_hotkeys_keep_newest(self):
        # Simulate duplicates per-hotkey (legacy/dirty DB state).
        self._insert_miner_address_row("hotkey1", "uid1", "address1")
        self._insert_miner_address_row("hotkey1", "uid2", "address2")
        self._insert_miner_address_row("hotkey2", "uid1", "address3")
        self._insert_miner_address_row("hotkey2", "uid2", "address4")
        self._insert_miner_address_row("hotkey3", "uid1", "address5")

        deleted = self.db.prune_all_hotkeys_keep_newest()
        # hotkey1: 2->1 (1 deleted), hotkey2: 2->1 (1 deleted), hotkey3: 1->1 (0)
        self.assertEqual(deleted, 2)

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT hotkey, COUNT(*) FROM miner_addresses GROUP BY hotkey")
            counts = dict(cursor.fetchall())
            self.assertEqual(counts.get("hotkey1"), 1)
            self.assertEqual(counts.get("hotkey2"), 1)
            self.assertEqual(counts.get("hotkey3"), 1)

    def test_prune_all_hotkeys_timestamp_tie_uses_rowid(self):
        # If timestamps tie, pruning should keep the row with higher rowid.
        self._insert_miner_address_row("hotkey1", "uid1", "address1")
        self._insert_miner_address_row("hotkey1", "uid2", "address2")

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE miner_addresses SET timestamp = '2000-01-01 00:00:00' WHERE hotkey = ?",
                ("hotkey1",),
            )
            conn.commit()
            cursor.execute(
                "SELECT rowid, address FROM miner_addresses WHERE hotkey = ? ORDER BY rowid ASC",
                ("hotkey1",),
            )
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)
            (rowid1, addr1), (rowid2, addr2) = rows
            self.assertLess(rowid1, rowid2)

        deleted = self.db.prune_all_hotkeys_keep_newest()
        self.assertEqual(deleted, 1)

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT address FROM miner_addresses WHERE hotkey = ?",
                ("hotkey1",),
            )
            kept = cursor.fetchone()
            self.assertIsNotNone(kept)
            # Newest-by-rowid (rowid2) should win.
            self.assertEqual(kept[0], addr2)


class TestRoutingTable(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        db_path = os.path.join(self._tmpdir.name, "routing_table.db")
        self.routing_table = RoutingTable(db_path=db_path)

    def tearDown(self):
        # Clear the database after each test to avoid cross-test pollution
        with self.routing_table.db.lock, closing(
            sqlite3.connect(self.routing_table.db.db_path)
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM miner_addresses")
            cursor.execute("DELETE FROM worker_registry")
            cursor.execute("DELETE FROM unregistered_tees")
            conn.commit()

    def test_clear_miner(self):
        self.routing_table.clear_miner("hotkey1")
        self.routing_table.clear_miner("hotkey2")
        try:
            self.routing_table.add_miner_address("hotkey1", "uid1", "address1")
            self.routing_table.add_miner_address("hotkey1", "uid2", "address2")
            self.routing_table.clear_miner("hotkey1")
            # Verify all addresses for the miner were cleared
            with (
                self.routing_table.db.lock,
                closing(sqlite3.connect(self.routing_table.db.db_path)) as conn,
            ):
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM miner_addresses WHERE hotkey = ?",
                    ("hotkey1",),
                )
                result = cursor.fetchall()
                self.assertEqual(len(result), 0)
        except sqlite3.Error as e:
            self.fail(f"Unexpected database error: {e}")

    def test_get_miner_addresses(self):
        self.routing_table.clear_miner("hotkey1")
        self.routing_table.clear_miner("hotkey2")
        try:
            self.routing_table.add_miner_address("hotkey1", "uid1", "address1")
            self.routing_table.add_miner_address("hotkey1", "uid2", "address2")
            addresses = self.routing_table.get_miner_addresses("hotkey1")
            # Enforced invariant: only the newest address is kept per hotkey.
            self.assertEqual(len(addresses), 1)
            self.assertEqual(addresses[0][0], "address2")
        except sqlite3.Error as e:
            self.fail(f"Unexpected database error: {e}")

    def test_get_all_addresses(self):
        self.routing_table.clear_miner("hotkey1")
        self.routing_table.clear_miner("hotkey2")
        try:
            self.routing_table.add_miner_address("hotkey1", "uid1", "address1")
            self.routing_table.add_miner_address("hotkey2", "uid2", "address2")
            addresses = self.routing_table.get_all_addresses()

            self.assertEqual(len(addresses), 2)
            self.assertIn("address1", addresses)
            self.assertIn("address2", addresses)
        except sqlite3.Error as e:
            self.fail(f"Unexpected database error: {e}")

    def test_add_duplicate_address(self):
        self.routing_table.add_miner_address("hotkey1", "uid1", "address1")
        # Attempt to add a duplicate address under a different hotkey.
        # With current logic, a 1-row "old hotkey" is treated as orphaned and
        # the address may be reassigned.
        self.routing_table.add_miner_address("hotkey2", "uid2", "address1")

        with (
            self.routing_table.db.lock,
            closing(sqlite3.connect(self.routing_table.db.db_path)) as conn,
        ):
            cursor = conn.cursor()
            cursor.execute(
                "SELECT hotkey FROM miner_addresses WHERE address = ?",
                ("address1",),
            )
            rows = cursor.fetchall()
            self.assertEqual(rows, [("hotkey2",)])


if __name__ == "__main__":
    unittest.main()
