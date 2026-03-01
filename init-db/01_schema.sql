-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- -----------------------------------------------
-- Tracking data: player & ball positions per frame
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS tracking (
    game_id TEXT NOT NULL,
    frame INTEGER NOT NULL,
    time_s DOUBLE PRECISION NOT NULL,
    period INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    -- e.g. 'Player01', 'Ball'
    team TEXT,
    -- 'Home', 'Away', NULL for ball
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    PRIMARY KEY (game_id, frame, entity_id)
);

-- Convert to hypertable partitioned by frame (use time_s for time-series queries)
SELECT
    create_hypertable(
        'tracking',
        'time_s',
        chunk_time_interval => 60,
        if_not_exists => TRUE
    );

-- -----------------------------------------------
-- Events: passes, set pieces, shots, etc.
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    game_id TEXT NOT NULL,
    event_id SERIAL,
    team TEXT,
    type TEXT,
    -- 'PASS', 'SET PIECE', 'SHOT', etc.
    subtype TEXT,
    period INTEGER,
    start_frame INTEGER,
    start_time_s DOUBLE PRECISION,
    end_frame INTEGER,
    end_time_s DOUBLE PRECISION,
    from_player TEXT,
    to_player TEXT,
    start_x DOUBLE PRECISION,
    start_y DOUBLE PRECISION,
    end_x DOUBLE PRECISION,
    end_y DOUBLE PRECISION,
    PRIMARY KEY (game_id, event_id)
);

-- -----------------------------------------------
-- Useful indexes for Grafana queries
-- -----------------------------------------------
CREATE INDEX IF NOT EXISTS idx_tracking_game_entity ON tracking (game_id, entity_id);

CREATE INDEX IF NOT EXISTS idx_events_game_team ON events (game_id, team);

CREATE INDEX IF NOT EXISTS idx_events_type ON events (game_id, type, team);

-- -----------------------------------------------
-- Continuous aggregate: possession per minute
-- (fraction of frames ball is in each half)
-- -----------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS possession_per_minute WITH (timescaledb.continuous) AS
SELECT
    game_id,
    time_bucket(60.0, time_s) AS minute,
    team,
    COUNT(*) AS frames_held
FROM
    tracking
WHERE
    entity_id = 'Ball'
    AND team IS NOT NULL
GROUP BY
    game_id,
    minute,
    team WITH NO DATA;