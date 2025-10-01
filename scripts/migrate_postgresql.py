#!/usr/bin/env python3
"""
Manual PostgreSQL Migration Script

This script applies migrations to an existing PostgreSQL telemetry database.
It will add missing columns and indexes safely without affecting existing data.

Usage:
    python scripts/migrate_postgresql.py

Environment Variables Required:
    POSTGRES_HOST - PostgreSQL server host
    POSTGRES_PORT - PostgreSQL server port (default: 5432)
    POSTGRES_DB - Database name (default: telemetry)
    POSTGRES_USER - Database user (default: telemetry_user)
    POSTGRES_PASSWORD - Database password
"""

import os
import sys
import psycopg2
import psycopg2.extras
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class PostgreSQLMigrator:
    def __init__(self):
        """Initialize connection parameters from environment variables."""
        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_DB", "telemetry")
        self.user = os.getenv("POSTGRES_USER", "telemetry_user")
        self.password = os.getenv("POSTGRES_PASSWORD")

        if not self.host:
            raise ValueError("POSTGRES_HOST environment variable is required")
        if not self.password:
            raise ValueError("POSTGRES_PASSWORD environment variable is required")

        logger.info(
            f"Connecting to PostgreSQL: {self.host}:{self.port}/{self.database}"
        )

    def get_connection(self):
        """Get a database connection."""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def check_current_schema(self):
        """Check what columns currently exist in the telemetry table."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Check if telemetry table exists
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'telemetry'
                    );
                """
                )
                table_exists = cursor.fetchone()["exists"]

                if not table_exists:
                    logger.error("Telemetry table does not exist!")
                    return None

                # Get current columns
                cursor.execute(
                    """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'telemetry'
                    ORDER BY ordinal_position;
                """
                )
                columns = cursor.fetchall()

                logger.info("Current telemetry table schema:")
                for col in columns:
                    logger.info(
                        f"  {col['column_name']} ({col['data_type']}) - "
                        f"Nullable: {col['is_nullable']}, Default: {col['column_default']}"
                    )

                return [col["column_name"] for col in columns]

    def apply_migrations(self):
        """Apply all missing migrations to the database."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get current columns
                cursor.execute(
                    """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'telemetry'
                """
                )
                existing_columns = [row["column_name"] for row in cursor.fetchall()]
                logger.info(f"Existing columns: {existing_columns}")

                migrations_applied = []

                # Migration 1: Add stats_json column if missing
                if "stats_json" not in existing_columns:
                    logger.info("Applying migration: Add stats_json column...")
                    cursor.execute("ALTER TABLE telemetry ADD COLUMN stats_json JSONB;")
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_telemetry_stats_json ON telemetry USING GIN (stats_json);"
                    )
                    migrations_applied.append("stats_json column and index")
                else:
                    logger.info("stats_json column already exists, skipping...")

                # Migration 2: Add TikTok fields if missing
                tiktok_fields = [
                    ("tiktok_transcription_success", "INTEGER DEFAULT 0"),
                    ("tiktok_transcription_errors", "INTEGER DEFAULT 0"),
                ]

                for field_name, field_definition in tiktok_fields:
                    if field_name not in existing_columns:
                        logger.info(f"Applying migration: Add {field_name} column...")
                        cursor.execute(
                            f"ALTER TABLE telemetry ADD COLUMN {field_name} {field_definition};"
                        )
                        cursor.execute(
                            f"CREATE INDEX IF NOT EXISTS idx_telemetry_{field_name} ON telemetry({field_name});"
                        )
                        migrations_applied.append(f"{field_name} column and index")
                    else:
                        logger.info(f"{field_name} column already exists, skipping...")

                # Migration 3: Ensure other required indexes exist
                indexes_to_create = [
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_hotkey ON telemetry(hotkey);",
                    'CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry("timestamp");',
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_created_at ON telemetry(created_at);",
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_uid ON telemetry(uid);",
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_worker_id ON telemetry(worker_id);",
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_hotkey_created_at ON telemetry(hotkey, created_at DESC);",
                ]

                logger.info("Ensuring all indexes exist...")
                for index_sql in indexes_to_create:
                    cursor.execute(index_sql)
                migrations_applied.append("all required indexes")

                # Commit all changes
                conn.commit()

                if migrations_applied:
                    logger.info("Migrations applied successfully:")
                    for migration in migrations_applied:
                        logger.info(f"  ✅ {migration}")
                else:
                    logger.info("No migrations needed - database is up to date!")

    def run_migration_files(self):
        """Run the SQL migration files directly."""
        migration_files = [
            "db/migrations/refactor_to_json_stats.sql",
            "db/migrations/add_tiktok_fields.sql",
        ]

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for migration_file in migration_files:
                    migration_path = Path(__file__).parent.parent / migration_file
                    if migration_path.exists():
                        logger.info(f"Running migration file: {migration_file}")
                        with open(migration_path, "r") as f:
                            sql_content = f.read()
                            # Split by semicolon and execute each statement
                            statements = [
                                stmt.strip()
                                for stmt in sql_content.split(";")
                                if stmt.strip()
                            ]
                            for stmt in statements:
                                if stmt and not stmt.startswith("--"):
                                    try:
                                        cursor.execute(stmt)
                                        logger.debug(f"Executed: {stmt[:50]}...")
                                    except psycopg2.Error as e:
                                        logger.warning(
                                            f"Statement failed (might be expected): {e}"
                                        )
                    else:
                        logger.warning(f"Migration file not found: {migration_path}")

                conn.commit()
                logger.info("Migration files executed successfully!")


def main():
    """Main migration function."""
    try:
        migrator = PostgreSQLMigrator()

        logger.info("=== PostgreSQL Migration Tool ===")
        logger.info("Checking current database schema...")

        # Check current schema
        current_columns = migrator.check_current_schema()
        if current_columns is None:
            logger.error("Cannot proceed - telemetry table does not exist")
            return 1

        logger.info("\nApplying migrations...")
        migrator.apply_migrations()

        logger.info("\n=== Migration Complete ===")
        logger.info("Your PostgreSQL database has been updated with the latest schema!")

        return 0

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
