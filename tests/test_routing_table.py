import unittest
from db.routing_table_database import RoutingTableDatabase
from validator.routing_table import RoutingTable
import sqlite3
from contextlib import closing


class TestRoutingTableDatabase(unittest.TestCase):
    def setUp(self):
        # Use an in-memory database for testing
        self.db = RoutingTableDatabase(db_path="test_miner_tee_addresses.db")
        # Ensure the table is created
        self.db._create_table()

    def tearDown(self):
        # Clear the database after each test
        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM miner_addresses")
            cursor.execute("DELETE FROM worker_registry")
            cursor.execute("DELETE FROM unregistered_tees")
            conn.commit()

    def test_add_address(self):
        try:
            self.db.add_address("hotkey1", "uid1", "address1")
            # Verify the address was added
            with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM miner_addresses WHERE hotkey = ? AND uid = ?",
                    ("hotkey1", "uid1"),
                )
                result = cursor.fetchone()
                self.assertIsNotNone(result)
                self.assertEqual(result[2], "address1")
        except sqlite3.IntegrityError as e:
            self.fail(f"Unexpected database error: {e}")

    def test_update_address(self):
        try:
            self.db.add_address("hotkey1", "uid1", "address1")
            self.db.update_address("hotkey1", "uid1", "address2")
            # Verify the address was updated
            with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM miner_addresses WHERE hotkey = ? AND uid = ?",
                    ("hotkey1", "uid1"),
                )
                result = cursor.fetchone()
                self.assertIsNotNone(result)
                self.assertEqual(result[2], "address2")
        except sqlite3.Error as e:
            self.fail(f"Unexpected database error: {e}")

    def test_delete_address(self):
        try:
            self.db.add_address("hotkey1", "uid1", "address1")
            self.db.delete_address("hotkey1", "uid1")
            # Verify the address was deleted
            with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM miner_addresses WHERE hotkey = ? AND uid = ?",
                    ("hotkey1", "uid1"),
                )
                result = cursor.fetchone()
                self.assertIsNone(result)
        except sqlite3.Error as e:
            self.fail(f"Unexpected database error: {e}")

    def test_prune_hotkey_keep_newest_no_rows(self):
        deleted = self.db.prune_hotkey_addresses_keep_newest("missing_hotkey")
        self.assertEqual(deleted, 0)

    def test_prune_hotkey_keep_newest_one_row(self):
        self.db.add_address("hotkey1", "uid1", "address1")
        deleted = self.db.prune_hotkey_addresses_keep_newest("hotkey1")
        self.assertEqual(deleted, 0)

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT address FROM miner_addresses WHERE hotkey = ?",
                ("hotkey1",),
            )
            rows = cursor.fetchall()
            self.assertEqual(rows, [("address1",)])

    def test_prune_hotkey_keep_newest_two_rows(self):
        self.db.add_address("hotkey1", "uid1", "address1")
        self.db.add_address("hotkey1", "uid2", "address2")

        deleted = self.db.prune_hotkey_addresses_keep_newest("hotkey1")
        self.assertEqual(deleted, 1)

        with self.db.lock, closing(sqlite3.connect(self.db.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT address FROM miner_addresses WHERE hotkey = ?",
                ("hotkey1",),
            )
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 1)

    def test_prune_hotkey_keep_newest_timestamp_tie_uses_rowid(self):
        # If timestamps tie, pruning should keep the row with higher rowid.
        self.db.add_address("hotkey1", "uid1", "address1")
        self.db.add_address("hotkey1", "uid2", "address2")

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

        deleted = self.db.prune_hotkey_addresses_keep_newest("hotkey1")
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

    def test_prune_all_hotkeys_keep_newest(self):
        self.db.add_address("hotkey1", "uid1", "address1")
        self.db.add_address("hotkey1", "uid2", "address2")
        self.db.add_address("hotkey2", "uid1", "address3")
        self.db.add_address("hotkey2", "uid2", "address4")
        self.db.add_address("hotkey3", "uid1", "address5")

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


class TestRoutingTable(unittest.TestCase):
    def setUp(self):
        self.routing_table = RoutingTable(db_path="test_miner_tee_addresses")

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
        # Attempt to add a duplicate address
        try:
            self.routing_table.add_miner_address("hotkey2", "uid2", "address1")
        except sqlite3.IntegrityError:
            # Expected error when adding duplicate address
            pass

        with (
            self.routing_table.db.lock,
            closing(sqlite3.connect(self.routing_table.db.db_path)) as conn,
        ):
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM miner_addresses WHERE address = ?",
                ("address1",),
            )
            result = cursor.fetchall()
            self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
