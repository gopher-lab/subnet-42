import unittest
from db.routing_table_database import RoutingTableDatabase
from validator.routing_table import RoutingTable
import sqlite3


class TestRoutingTableDatabase(unittest.TestCase):
    def setUp(self):
        # Use an in-memory database for testing
        self.db = RoutingTableDatabase(db_path="test_miner_tee_addresses.db")
        # Ensure the table is created
        self.db._create_table()

    def tearDown(self):
        # Clear the database after each test
        with self.db.lock, sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM miner_addresses")
            conn.commit()

    def test_add_address(self):
        try:
            self.db.add_address("hotkey1", "uid1", "address1")
            # Verify the address was added
            with self.db.lock, sqlite3.connect(self.db.db_path) as conn:
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
            with self.db.lock, sqlite3.connect(self.db.db_path) as conn:
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
            with self.db.lock, sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM miner_addresses WHERE hotkey = ? AND uid = ?",
                    ("hotkey1", "uid1"),
                )
                result = cursor.fetchone()
                self.assertIsNone(result)
        except sqlite3.Error as e:
            self.fail(f"Unexpected database error: {e}")


class TestRoutingTable(unittest.TestCase):
    def setUp(self):
        self.routing_table = RoutingTable(db_path="test_miner_tee_addresses")

    def tearDown(self):
        # Clear the database after each test to avoid cross-test pollution
        with self.routing_table.db.lock, sqlite3.connect(
            self.routing_table.db.db_path
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
                sqlite3.connect(self.routing_table.db.db_path) as conn,
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
            sqlite3.connect(self.routing_table.db.db_path) as conn,
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
