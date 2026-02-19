import sqlite3
from threading import Lock
import random
from contextlib import closing


class RoutingTableDatabase:
    def __init__(self, db_path="./miner_tee_addresses.db"):
        self.db_path = db_path
        self.lock = Lock()
        self._create_table()
        self._create_worker_registry_table()
        self._create_unregistered_tees_table()

    def _create_table(self):
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS miner_addresses (
                    hotkey TEXT,
                    uid TEXT,
                    address TEXT UNIQUE,
                    worker_id TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            conn.commit()

    def _create_worker_registry_table(self):
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS worker_registry (
                    worker_id TEXT PRIMARY KEY,
                    hotkey TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            conn.commit()

    def _create_unregistered_tees_table(self):
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS unregistered_tees (
                    address TEXT PRIMARY KEY,
                    hotkey TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            conn.commit()

    def add_address(self, hotkey, uid, address, worker_id=None):
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO miner_addresses (hotkey, uid, address, worker_id) 
                VALUES (?, ?, ?, ?)
                """,
                (hotkey, uid, address, worker_id),
            )
            conn.commit()

    def update_address(self, hotkey, uid, new_address, worker_id=None):
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            if worker_id is not None:
                cursor.execute(
                    """
                    UPDATE miner_addresses SET address = ?, worker_id = ? 
                    WHERE hotkey = ? AND uid = ?
                    """,
                    (new_address, worker_id, hotkey, uid),
                )
            else:
                cursor.execute(
                    """
                    UPDATE miner_addresses SET address = ? 
                    WHERE hotkey = ? AND uid = ?
                    """,
                    (new_address, hotkey, uid),
                )
            conn.commit()

    def update_timestamp(self, hotkey, uid, address, worker_id=None):
        """
        Update the timestamp for an existing miner address record to current time.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE miner_addresses 
                SET timestamp = CURRENT_TIMESTAMP 
                WHERE hotkey = ? AND uid = ? AND address = ? 
                AND worker_id = ?
                """,
                (hotkey, uid, address, worker_id),
            )
            conn.commit()
            # Return True if a row was updated
            return cursor.rowcount > 0

    def delete_address(self, hotkey, uid):
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM miner_addresses 
                WHERE hotkey = ? AND uid = ?
                """,
                (hotkey, uid),
            )
            conn.commit()

    def prune_hotkey_addresses_keep_newest(self, hotkey):
        """
        Enforce "at most one address per hotkey" by keeping only the newest row
        (by timestamp, then rowid) and deleting all older rows for that hotkey.

        Returns the number of deleted rows.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT rowid
                FROM miner_addresses
                WHERE hotkey = ?
                ORDER BY datetime(timestamp) DESC, rowid DESC
                LIMIT 1
                """,
                (hotkey,),
            )
            keep = cursor.fetchone()
            if not keep:
                return 0

            keep_rowid = keep[0]
            cursor.execute(
                """
                DELETE FROM miner_addresses
                WHERE hotkey = ? AND rowid <> ?
                """,
                (hotkey, keep_rowid),
            )
            deleted = cursor.rowcount
            conn.commit()
            return deleted

    def prune_all_hotkeys_keep_newest(self):
        """
        Enforce "at most one address per hotkey" across the whole table.

        Returns the total number of deleted rows.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT hotkey
                FROM miner_addresses
                GROUP BY hotkey
                HAVING COUNT(*) > 1
                """
            )
            hotkeys = [row[0] for row in cursor.fetchall()]

            deleted_total = 0
            for hotkey in hotkeys:
                cursor.execute(
                    """
                    SELECT rowid
                    FROM miner_addresses
                    WHERE hotkey = ?
                    ORDER BY datetime(timestamp) DESC, rowid DESC
                    LIMIT 1
                    """,
                    (hotkey,),
                )
                keep = cursor.fetchone()
                if not keep:
                    continue
                keep_rowid = keep[0]
                cursor.execute(
                    """
                    DELETE FROM miner_addresses
                    WHERE hotkey = ? AND rowid <> ?
                    """,
                    (hotkey, keep_rowid),
                )
                deleted_total += cursor.rowcount

            conn.commit()
            return deleted_total


    def clean_old_entries(self):
        """
        Remove all entries where the timestamp is more than one hour older.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM miner_addresses 
                WHERE timestamp < datetime('now', '-1 hour')
                """
            )
            conn.commit()

    def clean_old_entries_conservative(self):
        """
        Remove entries where the timestamp is more than 6 hours older.
        More conservative cleanup for very old entries only.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM miner_addresses 
                WHERE timestamp < datetime('now', '-6 hours')
                """
            )
            conn.commit()

    def remove_miner_address_by_address(self, address):
        """
        Remove a miner address entry by address only.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM miner_addresses 
                WHERE address = ?
                """,
                (address,),
            )
            conn.commit()

    def register_worker(self, worker_id, hotkey):
        """
        Register a worker_id with a hotkey in the worker registry.
        If the worker_id already exists, it will update the hotkey.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO worker_registry (worker_id, hotkey) 
                VALUES (?, ?)
                """,
                (worker_id, hotkey),
            )
            conn.commit()

    def unregister_worker(self, worker_id):
        """
        Remove a worker_id from the worker registry.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM worker_registry 
                WHERE worker_id = ?
                """,
                (worker_id,),
            )
            conn.commit()

    def unregister_workers_by_hotkey(self, hotkey):
        """
        Remove all worker_ids associated with a hotkey from the registry.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM worker_registry 
                WHERE hotkey = ?
                """,
                (hotkey,),
            )
            conn.commit()

    def get_worker_hotkey(self, worker_id):
        """
        Get the hotkey associated with a worker_id from the registry.
        Returns None if the worker_id is not registered.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            # Ensure worker_id is treated as a string for comparison
            worker_id_str = str(worker_id)
            cursor.execute(
                """
                SELECT hotkey FROM worker_registry WHERE worker_id = ?;
                """,
                (worker_id_str,),
            )

            result = cursor.fetchone()

            return result[0] if result else None

    def get_workers_by_hotkey(self, hotkey):
        """
        Get all worker_ids associated with a hotkey from the registry.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT worker_id FROM worker_registry 
                WHERE hotkey = ?
                """,
                (hotkey,),
            )
            results = cursor.fetchall()
            return [row[0] for row in results]

    def get_all_worker_registrations(self):
        """
        Get all worker_id and hotkey pairs from the registry.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT worker_id, hotkey FROM worker_registry
                """
            )
            results = cursor.fetchall()
            # Convert to list and randomize in Python
            worker_list = [(row[0], row[1]) for row in results]
            random.shuffle(worker_list)
            return worker_list

    def clean_old_worker_registrations(self, hours=24):
        """
        Remove worker registrations older than the specified number of hours.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM worker_registry 
                WHERE timestamp < datetime('now', ?)
                """,
                (f"-{hours} hours",),
            )
            conn.commit()

    def add_unregistered_tee(self, address, hotkey):
        """
        Add a new unregistered TEE to the database.
        If the address already exists, it will update the hotkey.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO unregistered_tees (address, hotkey) 
                VALUES (?, ?)
                """,
                (address, hotkey),
            )
            conn.commit()

    def clean_old_unregistered_tees(self):
        """
        Remove all unregistered TEEs where the timestamp is more than one hour old.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM unregistered_tees 
                WHERE timestamp < datetime('now', '-1 hour')
                """
            )
            conn.commit()

    def get_all_unregistered_tees(self):
        """
        Get all unregistered TEEs from the database.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT address, hotkey FROM unregistered_tees
                """
            )
            results = cursor.fetchall()
            return [(address, hotkey) for address, hotkey in results]

    def get_all_unregistered_tee_addresses(self):
        """
        Get all addresses from the unregistered_tees table.
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT address FROM unregistered_tees
                """
            )
            results = cursor.fetchall()
            return [address[0] for address in results]

    def get_miner_addresses_by_hotkey(self, hotkey):
        """
        Get all miner addresses associated with a hotkey.

        :param hotkey: The hotkey to search for
        :return: A list of (uid, address, worker_id) tuples for the specified
                 hotkey
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT uid, address, worker_id 
                FROM miner_addresses 
                WHERE hotkey = ?
                """,
                (hotkey,),
            )
            results = cursor.fetchall()
            return [(row[0], row[1], row[2]) for row in results]

    def get_address_timestamp(self, address):
        """
        Get the timestamp of a specific address.

        :param address: The address to check
        :return: The timestamp string or None if not found
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT timestamp FROM miner_addresses WHERE address = ?
                """,
                (address,),
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def remove_unregistered_tee(self, address):
        """
        Remove a specific unregistered TEE by address.

        :param address: The address of the unregistered TEE to remove
        :return: True if an entry was removed, False if not found
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM unregistered_tees 
                WHERE address = ?
                """,
                (address,),
            )
            conn.commit()
            return cursor.rowcount > 0
