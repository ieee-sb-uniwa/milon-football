#!/usr/bin/env python3
"""
Load football tracking and events data into TimescaleDB.
Supports idempotent loading of multiple games.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# DATABASE CONNECTION
# ============================================================================


def get_connection(
    host: str = "localhost",
    port: int = 5432,
    database: str = "football",
    user: str = "grafana",
    password: str = "grafana_pass",
) -> psycopg2.extensions.connection:
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        logger.info(f"Connected to database '{database}' at {host}:{port}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise


# ============================================================================
# TRACKING DATA TRANSFORMATION
# ============================================================================


def load_and_transform_tracking_data(
    file_path: Path, team: str, game_id: str
) -> pd.DataFrame:
    """
    Load tracking data CSV and transform from wide to long format.

    Args:
        file_path: Path to tracking CSV file
        team: 'Home' or 'Away'
        game_id: Game identifier

    Returns:
        DataFrame with columns: game_id, frame, time_s, period, entity_id, team, x, y
    """
    logger.info(f"Loading {team} tracking data from {file_path.name}")

    # Read with multi-level header
    df = pd.read_csv(file_path, skiprows=[0], header=[0, 1])

    # Build proper column names
    new_columns = []
    last_entity = None

    for col in df.columns:
        level_0, level_1 = col

        # Handle first three columns (Period, Frame, Time)
        if "Period" in str(level_1):
            new_columns.append("Period")
        elif "Frame" in str(level_1):
            new_columns.append("Frame")
        elif "Time" in str(level_1):
            new_columns.append("Time")
        # Handle columns with Player or Ball names (X coordinates)
        elif "Player" in str(level_1) or "Ball" in str(level_1):
            last_entity = level_1
            new_columns.append(f"{last_entity}_X")
        # Handle unnamed columns (Y coordinates)
        elif last_entity and "Unnamed" in str(level_0):
            new_columns.append(f"{last_entity}_Y")
        else:
            new_columns.append(f"Column_{len(new_columns)}")

    df.columns = new_columns

    # Separate metadata columns from position columns
    metadata_cols = ["Period", "Frame", "Time"]
    position_cols = [col for col in df.columns if col not in metadata_cols]

    # Extract entity names from position columns (remove _X and _Y suffix)
    entities = sorted(set(col.rsplit("_", 1)[0] for col in position_cols))

    logger.info(
        f"Found {len(entities)} entities: {len([e for e in entities if 'Player' in e])} players + {'Ball' if 'Ball' in entities else '0 balls'}"
    )

    # Transform to long format
    rows = []
    for _, row in df.iterrows():
        for entity in entities:
            x_col = f"{entity}_X"
            y_col = f"{entity}_Y"

            if x_col in df.columns and y_col in df.columns:
                x_val = row[x_col]
                y_val = row[y_col]

                # Skip rows with NaN coordinates
                if pd.notna(x_val) and pd.notna(y_val):
                    rows.append(
                        {
                            "game_id": game_id,
                            "frame": int(row["Frame"]),
                            "time_s": float(row["Time"]),
                            "period": int(row["Period"]),
                            "entity_id": entity,
                            "team": None if entity == "Ball" else team,
                            "x": float(x_val),
                            "y": float(y_val),
                        }
                    )

    result_df = pd.DataFrame(rows)
    logger.info(
        f"Transformed {len(df)} frames into {len(result_df)} entity position records"
    )

    return result_df


def merge_tracking_data(home_path: Path, away_path: Path, game_id: str) -> pd.DataFrame:
    """
    Load and merge home and away tracking data, handling ball deduplication.

    Args:
        home_path: Path to home team tracking CSV
        away_path: Path to away team tracking CSV
        game_id: Game identifier

    Returns:
        Merged DataFrame with all entities
    """
    home_df = load_and_transform_tracking_data(home_path, "Home", game_id)
    away_df = load_and_transform_tracking_data(away_path, "Away", game_id)

    # Combine both dataframes
    combined = pd.concat([home_df, away_df], ignore_index=True)

    # Remove duplicate ball positions (ball appears in both files)
    # Keep ball data from one file arbitrarily (first occurrence)
    combined = combined.drop_duplicates(
        subset=["game_id", "frame", "entity_id"], keep="first"
    )

    logger.info(
        f"Merged tracking data: {len(combined)} total records after deduplication"
    )

    return combined


# ============================================================================
# EVENTS DATA LOADING
# ============================================================================


def load_events_data(file_path: Path, game_id: str) -> pd.DataFrame:
    """
    Load events data CSV and map to database schema.

    Args:
        file_path: Path to events CSV file
        game_id: Game identifier

    Returns:
        DataFrame ready for database insert
    """
    logger.info(f"Loading events data from {file_path.name}")

    df = pd.read_csv(file_path)

    # Map columns to database schema
    events_df = pd.DataFrame(
        {
            "game_id": game_id,
            "team": df["Team"],
            "type": df["Type"],
            "subtype": df["Subtype"].replace({np.nan: None}),
            "period": df["Period"].astype(int),
            "start_frame": df["Start Frame"].astype(int),
            "start_time_s": df["Start Time [s]"].astype(float),
            "end_frame": df["End Frame"].astype(int),
            "end_time_s": df["End Time [s]"].astype(float),
            "from_player": df["From"].replace({np.nan: None}),
            "to_player": df["To"].replace({np.nan: None}),
            "start_x": df["Start X"].replace({np.nan: None}),
            "start_y": df["Start Y"].replace({np.nan: None}),
            "end_x": df["End X"].replace({np.nan: None}),
            "end_y": df["End Y"].replace({np.nan: None}),
        }
    )

    logger.info(f"Loaded {len(events_df)} events")

    return events_df


# ============================================================================
# DATABASE INSERT FUNCTIONS
# ============================================================================


def insert_tracking_data(
    conn: psycopg2.extensions.connection, df: pd.DataFrame, chunk_size: int = 10000
) -> Tuple[int, int]:
    """
    Insert tracking data into database with idempotent handling.

    Args:
        conn: Database connection
        df: DataFrame with tracking data
        chunk_size: Number of rows per insert batch

    Returns:
        Tuple of (rows_inserted, rows_skipped)
    """
    logger.info(f"Inserting {len(df)} tracking records in chunks of {chunk_size}")

    cursor = conn.cursor()

    # Prepare data for insertion - convert numpy types to native Python types
    records = df.to_records(index=False)
    data = [
        tuple(
            (
                int(x)
                if isinstance(x, (np.integer,))
                else float(x) if isinstance(x, (np.floating,)) else x
            )
            for x in r
        )
        for r in records
    ]

    total_inserted = 0
    total_skipped = 0

    # Insert in chunks
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]

        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM tracking")
        count_before = cursor.fetchone()[0]

        # Insert with ON CONFLICT DO NOTHING for idempotency
        insert_query = """
            INSERT INTO tracking (game_id, frame, time_s, period, entity_id, team, x, y)
            VALUES %s
            ON CONFLICT (game_id, frame, entity_id) DO NOTHING
        """

        execute_values(cursor, insert_query, chunk)
        conn.commit()

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM tracking")
        count_after = cursor.fetchone()[0]

        inserted = count_after - count_before
        skipped = len(chunk) - inserted

        total_inserted += inserted
        total_skipped += skipped

        logger.info(
            f"Chunk {i//chunk_size + 1}: {inserted} inserted, {skipped} skipped (duplicates)"
        )

    cursor.close()
    logger.info(
        f"Tracking data insert complete: {total_inserted} new records, {total_skipped} duplicates skipped"
    )

    return total_inserted, total_skipped


def insert_events_data(
    conn: psycopg2.extensions.connection, df: pd.DataFrame
) -> Tuple[int, int]:
    """
    Insert events data into database with idempotent handling.

    Args:
        conn: Database connection
        df: DataFrame with events data

    Returns:
        Tuple of (rows_inserted, rows_skipped)
    """
    logger.info(f"Inserting {len(df)} events records")

    cursor = conn.cursor()

    # Get the next event_id for this game
    cursor.execute(
        "SELECT COALESCE(MAX(event_id), 0) FROM events WHERE game_id = %s",
        (df["game_id"].iloc[0],),
    )
    max_event_id = cursor.fetchone()[0]

    # Prepare data (add sequential event_id within game)
    records = []
    for idx, row in df.iterrows():
        # Convert numpy types to native Python types
        def convert_value(val):
            if pd.isna(val):
                return None
            elif isinstance(val, (np.integer,)):
                return int(val)
            elif isinstance(val, (np.floating,)):
                return float(val)
            else:
                return val

        records.append(
            (
                row["game_id"],
                max_event_id + idx + 1,  # Sequential event_id
                row["team"],
                row["type"],
                convert_value(row["subtype"]),
                int(row["period"]),
                int(row["start_frame"]),
                float(row["start_time_s"]),
                int(row["end_frame"]),
                float(row["end_time_s"]),
                convert_value(row["from_player"]),
                convert_value(row["to_player"]),
                convert_value(row["start_x"]),
                convert_value(row["start_y"]),
                convert_value(row["end_x"]),
                convert_value(row["end_y"]),
            )
        )

    # Get initial count
    cursor.execute(
        "SELECT COUNT(*) FROM events WHERE game_id = %s", (df["game_id"].iloc[0],)
    )
    count_before = cursor.fetchone()[0]

    # Insert with ON CONFLICT DO NOTHING
    insert_query = """
        INSERT INTO events (
            game_id, event_id, team, type, subtype, period,
            start_frame, start_time_s, end_frame, end_time_s,
            from_player, to_player, start_x, start_y, end_x, end_y
        )
        VALUES %s
        ON CONFLICT (game_id, event_id) DO NOTHING
    """

    execute_values(cursor, insert_query, records)
    conn.commit()

    # Get final count
    cursor.execute(
        "SELECT COUNT(*) FROM events WHERE game_id = %s", (df["game_id"].iloc[0],)
    )
    count_after = cursor.fetchone()[0]

    inserted = count_after - count_before
    skipped = len(records) - inserted

    cursor.close()
    logger.info(
        f"Events insert complete: {inserted} new records, {skipped} duplicates skipped"
    )

    return inserted, skipped


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def validate_game_files(data_dir: Path, game_id: str) -> bool:
    """Check if all required game files exist."""
    required_files = [
        data_dir / game_id / f"{game_id}_RawTrackingData_Home_Team.csv",
        data_dir / game_id / f"{game_id}_RawTrackingData_Away_Team.csv",
        data_dir / game_id / f"{game_id}_RawEventsData.csv",
    ]

    for file_path in required_files:
        if not file_path.exists():
            logger.error(f"Required file not found: {file_path}")
            return False

    logger.info(f"All required files found for {game_id}")
    return True


def get_game_stats(conn: psycopg2.extensions.connection, game_id: Optional[str] = None):
    """Query and display database stats for loaded games."""
    cursor = conn.cursor()

    if game_id:
        # Stats for specific game
        cursor.execute("SELECT COUNT(*) FROM tracking WHERE game_id = %s", (game_id,))
        tracking_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM events WHERE game_id = %s", (game_id,))
        events_count = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(DISTINCT entity_id) FROM tracking WHERE game_id = %s",
            (game_id,),
        )
        entity_count = cursor.fetchone()[0]

        logger.info(f"\n=== Stats for {game_id} ===")
        logger.info(f"Tracking records: {tracking_count:,}")
        logger.info(f"Events: {events_count:,}")
        logger.info(f"Unique entities: {entity_count}")
    else:
        # Stats for all games
        cursor.execute(
            "SELECT game_id, COUNT(*) FROM tracking GROUP BY game_id ORDER BY game_id"
        )
        tracking_stats = cursor.fetchall()

        cursor.execute(
            "SELECT game_id, COUNT(*) FROM events GROUP BY game_id ORDER BY game_id"
        )
        events_stats = cursor.fetchall()

        logger.info("\n=== Database Stats (All Games) ===")
        logger.info("Tracking records by game:")
        for game, count in tracking_stats:
            logger.info(f"  {game}: {count:,}")

        logger.info("Events by game:")
        for game, count in events_stats:
            logger.info(f"  {game}: {count:,}")

    cursor.close()


def clear_game_data(conn: psycopg2.extensions.connection, game_id: str):
    """Delete all data for a specific game (for reloading)."""
    cursor = conn.cursor()

    cursor.execute("DELETE FROM tracking WHERE game_id = %s", (game_id,))
    tracking_deleted = cursor.rowcount

    cursor.execute("DELETE FROM events WHERE game_id = %s", (game_id,))
    events_deleted = cursor.rowcount

    conn.commit()
    cursor.close()

    logger.info(
        f"Cleared {game_id}: {tracking_deleted} tracking records, {events_deleted} events"
    )


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================


def load_game(
    game_id: str,
    data_dir: Path,
    conn: psycopg2.extensions.connection,
    force_reload: bool = False,
) -> bool:
    """
    Load a single game's tracking and events data into TimescaleDB.

    Args:
        game_id: Game identifier (e.g., 'Sample_Game_1')
        data_dir: Path to data directory
        conn: Database connection
        force_reload: If True, clear existing data before loading

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Loading {game_id}")
    logger.info(f"{'='*60}")

    # Validate files exist
    if not validate_game_files(data_dir, game_id):
        return False

    # Force reload if requested
    if force_reload:
        logger.info("Force reload enabled - clearing existing data")
        clear_game_data(conn, game_id)

    try:
        # Define file paths
        home_tracking = data_dir / game_id / f"{game_id}_RawTrackingData_Home_Team.csv"
        away_tracking = data_dir / game_id / f"{game_id}_RawTrackingData_Away_Team.csv"
        events_file = data_dir / game_id / f"{game_id}_RawEventsData.csv"

        # Load and transform tracking data
        tracking_df = merge_tracking_data(home_tracking, away_tracking, game_id)

        # Load events data
        events_df = load_events_data(events_file, game_id)

        # Insert into database
        logger.info("\nInserting into TimescaleDB...")
        tracking_inserted, tracking_skipped = insert_tracking_data(conn, tracking_df)
        events_inserted, events_skipped = insert_events_data(conn, events_df)

        # Show stats
        get_game_stats(conn, game_id)

        logger.info(f"\n✓ {game_id} loaded successfully!")
        return True

    except Exception as e:
        logger.error(f"Failed to load {game_id}: {e}", exc_info=True)
        conn.rollback()
        return False


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="Load football tracking and events data into TimescaleDB"
    )
    parser.add_argument(
        "--game-id", type=str, help="Game identifier (e.g., Sample_Game_1)"
    )
    parser.add_argument("--all", action="store_true", help="Load all available games")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Path to data directory (default: ./data)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Database host (default: localhost)",
    )
    parser.add_argument(
        "--port", type=int, default=5432, help="Database port (default: 5432)"
    )
    parser.add_argument(
        "--database",
        type=str,
        default="football",
        help="Database name (default: football)",
    )
    parser.add_argument(
        "--user", type=str, default="grafana", help="Database user (default: grafana)"
    )
    parser.add_argument(
        "--password", type=str, default="grafana_pass", help="Database password"
    )
    parser.add_argument(
        "--force-reload", action="store_true", help="Clear existing data before loading"
    )
    parser.add_argument(
        "--stats", action="store_true", help="Show database statistics and exit"
    )
    parser.add_argument(
        "--clear", type=str, help="Clear data for specified game_id and exit"
    )

    args = parser.parse_args()

    # Connect to database
    try:
        conn = get_connection(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
        )
    except Exception as e:
        logger.error(f"Cannot connect to database: {e}")
        sys.exit(1)

    # Handle utility commands
    if args.stats:
        get_game_stats(conn)
        conn.close()
        sys.exit(0)

    if args.clear:
        clear_game_data(conn, args.clear)
        conn.close()
        sys.exit(0)

    # Validate arguments
    if not args.game_id and not args.all:
        parser.error("Either --game-id or --all must be specified")

    # Load games
    success_count = 0
    fail_count = 0

    if args.all:
        # Find all Sample_Game_* directories
        game_dirs = sorted(
            [
                d.name
                for d in args.data_dir.iterdir()
                if d.is_dir()
                and d.name.startswith("Sample_Game_")
                and (d / f"{d.name}_RawEventsData.csv").exists()
            ]
        )

        if not game_dirs:
            logger.error(f"No game directories found in {args.data_dir}")
            sys.exit(1)

        logger.info(f"Found {len(game_dirs)} games to load: {', '.join(game_dirs)}")

        for game_id in game_dirs:
            if load_game(game_id, args.data_dir, conn, args.force_reload):
                success_count += 1
            else:
                fail_count += 1
    else:
        if load_game(args.game_id, args.data_dir, conn, args.force_reload):
            success_count = 1
        else:
            fail_count = 1

    # Show final summary
    logger.info(f"\n{'='*60}")
    logger.info(f"SUMMARY: {success_count} succeeded, {fail_count} failed")
    logger.info(f"{'='*60}")

    get_game_stats(conn)

    conn.close()

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
