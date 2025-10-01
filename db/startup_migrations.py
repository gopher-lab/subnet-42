"""
Startup Migration Module

This module provides lightweight migration functions that can be called
during application startup to ensure database schema is up to date.
"""

import os
import psycopg2
import psycopg2.extras
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


def run_postgresql_migrations():
    """
    Run PostgreSQL migrations during application startup.

    This is a lightweight version of the migration script that can be
    called directly from Python code during application initialization.

    Returns:
        bool: True if migrations were successful or not needed, False if failed
    """
    # Check if PostgreSQL is configured
    postgres_host = os.getenv("POSTGRES_HOST")
    if not postgres_host:
        logger.debug("POSTGRES_HOST not set, skipping PostgreSQL migrations")
        return True

    # Get connection parameters
    host = postgres_host
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "telemetry")
    user = os.getenv("POSTGRES_USER", "telemetry_user")
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        logger.warning("POSTGRES_PASSWORD not set, skipping PostgreSQL migrations")
        return True

    try:
        logger.info("Running PostgreSQL startup migrations...")

        # Connect to database
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )

        with conn:
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
                    logger.info(
                        "Telemetry table does not exist, will be created by application"
                    )
                    return True

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

                migrations_applied = []

                # Migration 1: Add stats_json column if missing
                if "stats_json" not in existing_columns:
                    logger.info("Adding stats_json column...")
                    cursor.execute("ALTER TABLE telemetry ADD COLUMN stats_json JSONB;")
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_telemetry_stats_json ON telemetry USING GIN (stats_json);"
                    )
                    migrations_applied.append("stats_json")

                # Migration 2: Add TikTok fields if missing
                tiktok_fields = [
                    ("tiktok_transcription_success", "INTEGER DEFAULT 0"),
                    ("tiktok_transcription_errors", "INTEGER DEFAULT 0"),
                ]

                for field_name, field_definition in tiktok_fields:
                    if field_name not in existing_columns:
                        logger.info(f"Adding {field_name} column...")
                        cursor.execute(
                            f"ALTER TABLE telemetry ADD COLUMN {field_name} {field_definition};"
                        )
                        cursor.execute(
                            f"CREATE INDEX IF NOT EXISTS idx_telemetry_{field_name} ON telemetry({field_name});"
                        )
                        migrations_applied.append(field_name)

                # Ensure other indexes exist
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_hotkey ON telemetry(hotkey);",
                    'CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry("timestamp");',
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_created_at ON telemetry(created_at);",
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_uid ON telemetry(uid);",
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_worker_id ON telemetry(worker_id);",
                    "CREATE INDEX IF NOT EXISTS idx_telemetry_hotkey_created_at ON telemetry(hotkey, created_at DESC);",
                ]

                for index_sql in indexes:
                    cursor.execute(index_sql)

                if migrations_applied:
                    logger.info(
                        f"Applied PostgreSQL migrations: {', '.join(migrations_applied)}"
                    )
                else:
                    logger.debug("No PostgreSQL migrations needed")

        conn.close()
        return True

    except psycopg2.OperationalError as e:
        logger.warning(f"PostgreSQL connection failed during migrations: {e}")
        logger.info("Application will continue and attempt connection later")
        return True  # Don't fail startup for connection issues

    except Exception as e:
        logger.error(f"PostgreSQL migration failed: {e}")
        return False


def run_all_startup_migrations():
    """
    Run all startup migrations.

    Currently only handles PostgreSQL migrations, but can be extended
    for other databases or migration types.

    Returns:
        bool: True if all migrations successful, False if any failed
    """
    try:
        # Run PostgreSQL migrations
        postgres_success = run_postgresql_migrations()

        if not postgres_success:
            logger.error("PostgreSQL migrations failed")
            return False

        logger.debug("All startup migrations completed successfully")
        return True

    except Exception as e:
        logger.error(f"Startup migrations failed: {e}")
        return False
