import sqlite3
from threading import Lock


class TelemetryDatabase:
    def __init__(self, db_path="./telemetry_data.db"):
        self.db_path = db_path
        self.lock = Lock()
        self._create_table()
        self._ensure_required_columns()

    def _create_table(self):
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry (
                    hotkey TEXT,
                    uid TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    boot_time INT,
                    last_operation_time INT,
                    current_time INT,
                    worker_id TEXT,
                    stats_json TEXT
                )
            """
            )
            conn.commit()

    def _ensure_required_columns(self):
        """
        Ensure required columns exist in the telemetry table.
        This handles database migrations for existing databases.
        """
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Check which columns exist
            cursor.execute("PRAGMA table_info(telemetry)")
            columns = [col[1] for col in cursor.fetchall()]

            # Add missing columns for JSON storage
            if "stats_json" not in columns:
                cursor.execute("ALTER TABLE telemetry ADD COLUMN stats_json TEXT")

            # Legacy columns removed: telemetry now uses JSON stats exclusively

            conn.commit()

    def add_telemetry(self, telemetry_data):
        import json
        from interfaces.types import NodeData

        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Get stats JSON - it should already be populated
            stats_json = telemetry_data.stats_json or {}
            if not NodeData.validate_stats_integrity(stats_json):
                print(
                    f"Warning: Invalid stats data for hotkey {telemetry_data.hotkey}, storing anyway"
                )

            cursor.execute(
                """
                INSERT INTO telemetry (hotkey, uid, boot_time, last_operation_time, current_time, 
                worker_id, stats_json) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telemetry_data.hotkey,
                    telemetry_data.uid,
                    telemetry_data.boot_time,
                    telemetry_data.last_operation_time,
                    telemetry_data.current_time,
                    telemetry_data.worker_id,
                    json.dumps(stats_json),
                ),
            )
            conn.commit()

    def _convert_row_to_nodedata(self, row):
        """Convert a database row to NodeData object."""
        import json
        from interfaces.types import NodeData

        try:
            # SQLite returns rows as tuples, map to actual column positions
            # Current schema: hotkey(0), uid(1), timestamp(2), boot_time(3), last_operation_time(4),
            # current_time(5), worker_id(6), stats_json(7), twitter_auth_errors(8)...tiktok_*(17,18)
            hotkey = row[0] if len(row) > 0 else ""
            uid = row[1] if len(row) > 1 else ""
            timestamp = row[2] if len(row) > 2 else 0
            boot_time = row[3] if len(row) > 3 else 0
            last_operation_time = row[4] if len(row) > 4 else 0
            current_time = row[5] if len(row) > 5 else 0
            worker_id = (
                row[6] if len(row) > 6 else ""
            )  # CORRECTED: worker_id is at index 6
            stats_json_str = (
                row[7] if len(row) > 7 else "{}"
            )  # CORRECTED: stats_json is at index 7

            # Parse stats_json
            stats_json = json.loads(stats_json_str) if stats_json_str else {}

            # Create NodeData with JSON stats
            # Handle timestamp conversion (could be datetime string or int)
            try:
                if isinstance(timestamp, str):
                    # Convert datetime string to timestamp (simplified)
                    import time
                    from datetime import datetime

                    dt = datetime.fromisoformat(timestamp.replace(" ", "T"))
                    timestamp_int = int(dt.timestamp())
                else:
                    timestamp_int = int(timestamp) if timestamp else 0
            except:
                timestamp_int = 0

            node_data = NodeData(
                hotkey=hotkey,
                uid=uid or "",
                worker_id=worker_id or "",
                timestamp=timestamp_int,
                boot_time=boot_time or 0,
                last_operation_time=last_operation_time or 0,
                current_time=current_time or 0,
                stats_json=stats_json,
            )

            # Populate legacy fields for backward compatibility
            node_data.populate_legacy_fields()

            return node_data
        except Exception as e:
            print(f"Failed to convert row to NodeData: {e}")
            import traceback

            traceback.print_exc()
            # Return a default NodeData object with minimal data
            return NodeData(
                hotkey=row[0] if len(row) > 0 else "",
                uid=row[1] if len(row) > 1 else "",
                worker_id=row[6] if len(row) > 6 else "",  # CORRECTED index
                timestamp=0,
                boot_time=0,
                last_operation_time=0,
                current_time=0,
                stats_json={},
            )

    def clean_old_entries(self, hours):
        """
        Remove all telemetry entries older than the specified number of hours.
        """
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM telemetry 
                WHERE timestamp < datetime('now', ?)
                """,
                (f"-{hours} hours",),
            )
            conn.commit()

    def get_telemetry_by_hotkey(self, hotkey):
        """Retrieve telemetry data for a specific hotkey."""
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM telemetry WHERE hotkey = ? ORDER BY timestamp DESC
                """,
                (hotkey,),
            )
            rows = cursor.fetchall()
            return [self._convert_row_to_nodedata(row) for row in rows]

    def get_all_hotkeys_with_telemetry(self):
        """Retrieve all unique hotkeys that have at least one telemetry entry."""
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT DISTINCT hotkey FROM telemetry
                """
            )
            hotkeys = [row[0] for row in cursor.fetchall()]
            return hotkeys

    def delete_telemetry_by_hotkey(self, hotkey):
        """Delete all telemetry entries for a specific hotkey."""
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM telemetry WHERE hotkey = ?
                """,
                (hotkey,),
            )
            conn.commit()
            return cursor.rowcount  # Return the number of rows deleted

    def get_all_telemetry(self):
        """Retrieve all telemetry data from the database."""
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM telemetry ORDER BY timestamp DESC
                """
            )
            rows = cursor.fetchall()
            return [self._convert_row_to_nodedata(row) for row in rows]
