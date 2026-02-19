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

    def add_or_refresh_address_keep_newest(self, hotkey, uid, address, worker_id=None):
        """
        Atomically:
        - resolves orphaned address conflicts (same address, different hotkey)
        - inserts or refreshes the (hotkey, uid, address, worker_id) row
        - prunes all other addresses for this hotkey (keeps newest)

        This enforces the invariant: at most one miner_addresses row per hotkey.

        Returns (action, pruned_count) where action is one of:
          - "inserted"
          - "refreshed" (timestamp updated for identical row)
          - "skipped_conflict" (address belongs to another active hotkey)
        """
        with self.lock, closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()

            # 1) Resolve "address is already registered" conflicts.
            cursor.execute(
                "SELECT hotkey FROM miner_addresses WHERE address = ?",
                (address,),
            )
            existing = cursor.fetchone()
            if existing and existing[0] != hotkey:
                old_hotkey = existing[0]
                cursor.execute(
                    "SELECT COUNT(*) FROM miner_addresses WHERE hotkey = ?",
                    (old_hotkey,),
                )
                count = cursor.fetchone()[0]
                if count == 1:
                    # Likely orphaned from a deregistered miner; delete to allow reuse.
                    cursor.execute(
                        "DELETE FROM miner_addresses WHERE address = ?",
                        (address,),
                    )
                else:
                    # Active hotkey has multiple entries; do not steal the address.
                    conn.commit()
                    return "skipped_conflict", 0

            # 2) Check current rows for this hotkey.
            cursor.execute(
                """
                SELECT rowid, uid, address, worker_id
                FROM miner_addresses
                WHERE hotkey = ?
                """,
                (hotkey,),
            )
            rows = cursor.fetchall()

            # Identical row exists -> refresh timestamp and then prune to newest.
            for rowid, existing_uid, existing_address, existing_worker_id in rows:
                if (
                    existing_uid == uid
                    and existing_address == address
                    and existing_worker_id == worker_id
                ):
                    cursor.execute(
                        """
                        UPDATE miner_addresses
                        SET timestamp = CURRENT_TIMESTAMP
                        WHERE rowid = ?
                        """,
                        (rowid,),
                    )
                    pruned = self._prune_hotkey_keep_newest_with_cursor(cursor, hotkey)
                    conn.commit()
                    return "refreshed", pruned

            # If same hotkey+uid exists but points elsewhere, delete it (uid churn update).
            cursor.execute(
                """
                DELETE FROM miner_addresses
                WHERE hotkey = ? AND uid = ? AND address <> ?
                """,
                (hotkey, uid, address),
            )

            # 3) Insert the new row.
            cursor.execute(
                """
                INSERT INTO miner_addresses (hotkey, uid, address, worker_id)
                VALUES (?, ?, ?, ?)
                """,
                (hotkey, uid, address, worker_id),
            )

            # 4) Prune: keep only newest row for this hotkey.
            pruned = self._prune_hotkey_keep_newest_with_cursor(cursor, hotkey)
            conn.commit()
            return "inserted", pruned

    def _prune_hotkey_keep_newest_with_cursor(self, cursor, hotkey):
        """
        Keep only the newest row for a hotkey, using an existing cursor/txn.
        Returns deleted row count.
        """
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
        return cursor.rowcount

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
                deleted_total += self._prune_hotkey_keep_newest_with_cursor(cursor, hotkey)

            conn.commit()
            return deleted_total

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
