# Football Tracking Data Analytics ⚽📊

A football (soccer) tracking and events data visualization platform using TimescaleDB and Grafana. Analyze player movements, ball positions, and match events in real-time.

## 📋 Overview

This project processes raw football tracking data (player and ball positions at 25 FPS) and events data (passes, shots, set pieces) into a TimescaleDB database optimized for time-series analysis and visualization with Grafana.

**Features:**

- **Time-series tracking data**: 3.2M+ position records per match
- **Event analysis**: Passes, shots, set pieces with coordinates
- **Optimized schema**: TimescaleDB hypertables for fast queries
- **Idempotent loading**: Safe to re-run data imports
- **Multi-game support**: Load and compare multiple matches

## 🚀 Quickstart Guide

### Prerequisites

- Docker and Docker Compose
- Python 3.8+ (with pip)
- Git

### Step 1: Download Sample Data

Clone the Metrica Sports sample dataset:

```bash
git clone https://github.com/metrica-sports/sample-data.git
```

Copy game files to the data directory:

```bash
cp -r sample-data/data/Sample_Game_{1,2} data/
```

### Step 2: Start Database and Grafana

Start TimescaleDB and Grafana using Docker Compose:

```bash
docker compose up -d
```

Wait for services to be healthy (~10-15 seconds):

```bash
docker compose ps
```

You should see both containers running:

- `football_timescaledb` - PostgreSQL with TimescaleDB extension
- `football_grafana` - Grafana visualization platform

**Access URLs:**

| Service         | URL                   | Username  | Password       |
| --------------- | --------------------- | --------- | -------------- |
| **Grafana**     | http://localhost:3000 | `admin`   | `admin`        |
| **TimescaleDB** | `localhost:5432`      | `grafana` | `grafana_pass` |

**Database Details:**

- Host: `localhost`
- Port: `5432`
- Database: `football`
- User: `grafana`
- Password: `grafana_pass`

### Step 3: Install Python Dependencies

Install required packages for data loading:

```bash
pip install psycopg2-binary pandas numpy
```

Or if using the project's conda environment:

```bash
/path/to/milon-football/.conda/bin/pip install psycopg2-binary pandas numpy
```

### Step 4: Load Data into TimescaleDB

Load Sample_Game_1 into the database (~2-3 minutes):

```bash
python load_data.py --game-id Sample_Game_1
```

You'll see progress logs as 3.2M+ tracking records and 1,745 events are imported. The script is idempotent - safe to re-run.

**Other useful commands:**

- Load all games: `python load_data.py --all`
- Check stats: `python load_data.py --stats`
- See all options: `python load_data.py --help`

### Step 5: Create Your First Grafana Panel

Now that data is loaded, let's visualize it in Grafana.

#### 5.1 Login to Grafana

1. Open http://localhost:3000 in your browser
2. Login with username `admin` and password `admin`
3. Skip password change prompt (or set new password)

#### 5.2 Verify Data Source

The TimescaleDB data source is pre-configured. To verify:

1. Click **☰ menu** (top left) → **Connections** → **Data sources**
2. Click **TimescaleDB**
3. Scroll down and click **Save & test**
4. You should see: ✓ "Database Connection OK"

#### 5.3 Create a New Dashboard

1. Click **☰ menu** → **Dashboards**
2. Click **New** → **New Dashboard**
3. Click **+ Add visualization**
4. Select **TimescaleDB** as data source

#### 5.4 Create a Player Tracking Visualization

Let's visualize player positions over time.

**Query 1: Ball Position Over Time**

In the query editor at the bottom:

1. Switch from **Builder** to **Code** mode (toggle in top right)
2. Enter this SQL query:

```sql
SELECT
  time_s as time,
  x,
  y
FROM tracking
WHERE
  game_id = 'Sample_Game_1'
  AND entity_id = 'Ball'
  AND period = 1
ORDER BY time_s
LIMIT 5000
```

3. Click **Run query** (or use Ctrl+Enter)

**Configure Visualization:**

1. In the right panel, select **Visualization**: **Time series** or **Table**
2. For **Time series**:
   - Time field: `time`
   - Series: `x` and `y` coordinates
3. Add panel title: "Ball Position - First Half"
4. Click **Apply** to save

#### 5.5 Create Event Count Panel

Add another panel to show event types:

**Query 2: Event Type Distribution**

```sql
SELECT
  type as event_type,
  COUNT(*) as count
FROM events
WHERE game_id = 'Sample_Game_1'
GROUP BY type
ORDER BY count DESC
```

**Visualization:**

- Type: **Bar chart** or **Pie chart**
- Show event distribution (PASS, SHOT, etc.)

#### 5.6 Create Player Heatmap Query

For more advanced analysis:

```sql
SELECT
  entity_id as player,
  AVG(x) as avg_x,
  AVG(y) as avg_y,
  COUNT(*) as touches
FROM tracking
WHERE
  game_id = 'Sample_Game_1'
  AND team = 'Home'
  AND period = 1
GROUP BY entity_id
ORDER BY touches DESC
```

**Visualization**: **Table** showing average positions per player

#### 5.7 Save Dashboard

1. Click **💾 Save dashboard** (top right)
2. Name it: "Football Match Analysis"
3. Click **Save**

## 📊 Database Schema

### `tracking` table

Stores player and ball positions per frame (25 FPS):

| Column      | Type             | Description                                 |
| ----------- | ---------------- | ------------------------------------------- |
| `game_id`   | TEXT             | Game identifier                             |
| `frame`     | INTEGER          | Frame number                                |
| `time_s`    | DOUBLE PRECISION | Time in seconds (TimescaleDB partition key) |
| `period`    | INTEGER          | Match period (1 or 2)                       |
| `entity_id` | TEXT             | Player ID or 'Ball'                         |
| `team`      | TEXT             | 'Home', 'Away', or NULL for ball            |
| `x`         | DOUBLE PRECISION | X coordinate (0-1 normalized)               |
| `y`         | DOUBLE PRECISION | Y coordinate (0-1 normalized)               |

**Primary Key**: `(game_id, frame, entity_id)`  
**Hypertable**: Partitioned by `time_s` with 60-second chunks

### `events` table

Stores match events (passes, shots, set pieces):

| Column               | Type             | Description                   |
| -------------------- | ---------------- | ----------------------------- |
| `game_id`            | TEXT             | Game identifier               |
| `event_id`           | SERIAL           | Sequential event ID           |
| `team`               | TEXT             | Team performing event         |
| `type`               | TEXT             | Event type (PASS, SHOT, etc.) |
| `subtype`            | TEXT             | Event subtype                 |
| `period`             | INTEGER          | Match period                  |
| `start_frame`        | INTEGER          | Starting frame                |
| `start_time_s`       | DOUBLE PRECISION | Start time in seconds         |
| `end_frame`          | INTEGER          | Ending frame                  |
| `end_time_s`         | DOUBLE PRECISION | End time in seconds           |
| `from_player`        | TEXT             | Player initiating event       |
| `to_player`          | TEXT             | Target player (for passes)    |
| `start_x`, `start_y` | DOUBLE PRECISION | Start coordinates             |
| `end_x`, `end_y`     | DOUBLE PRECISION | End coordinates               |

**Primary Key**: `(game_id, event_id)`

## 📁 Project Structure

```
milon-football/
├── data/                       # Raw game data (CSV files)
│   ├── Sample_Game_1/
│   ├── Sample_Game_2/
│   └── Sample_Game_3/
├── grafana/                    # Grafana configuration
│   └── provisioning/
│       ├── dashboards/
│       └── datasources/
├── init-db/                    # Database initialization
│   └── 01_schema.sql          # TimescaleDB schema
├── docker-compose.yml          # Docker services config
├── Dockerfile.grafana          # Custom Grafana image
├── load_data.py               # Data loading script
├── main.ipynb                 # Jupyter notebook for analysis
└── README.md                  # This file
```

## 🔧 Useful Commands

### Docker Management

```bash
# Start services
docker compose up -d

# Stop services
docker compose down

# View logs
docker compose logs -f

# Restart services
docker compose restart

# Remove all data (destructive!)
docker compose down -v
```

### Database Queries

Connect to database:

```bash
docker exec -it football_timescaledb psql -U grafana -d football
```

Example queries:

```sql
-- Count tracking records per game
SELECT game_id, COUNT(*) FROM tracking GROUP BY game_id;

-- Get unique players
SELECT DISTINCT entity_id FROM tracking WHERE team = 'Home' AND game_id = 'Sample_Game_1';

-- Events by type
SELECT type, COUNT(*) FROM events GROUP BY type ORDER BY count DESC;

-- Ball position at specific time
SELECT * FROM tracking WHERE entity_id = 'Ball' AND game_id = 'Sample_Game_1' AND time_s BETWEEN 60 AND 70;
```

### Data Management

```bash
# Reload specific game
python load_data.py --game-id Sample_Game_1 --force-reload

# Clear game data
python load_data.py --clear Sample_Game_1

# View detailed logs
python load_data.py --game-id Sample_Game_1 --data-dir ./data 2>&1 | tee load.log
```

## 🎯 Example Use Cases

1. **Player Heat Maps**: Query average positions for tactical analysis
2. **Pass Networks**: Analyze passing patterns between players
3. **Event Timeline**: Visualize match events chronologically
4. **Ball Possession**: Calculate possession percentage by half
5. **Speed Analysis**: Compute player velocities from position changes
6. **Zone Analysis**: Divide field into zones and analyze activity

## 🐛 Troubleshooting

### Database Connection Failed

```bash
# Check if containers are running
docker compose ps

# Check logs
docker compose logs timescaledb

# Manually run schema
docker exec -i football_timescaledb psql -U grafana -d football < init-db/01_schema.sql
```

### Grafana Data Source Error

1. Verify TimescaleDB is healthy: `docker compose ps`
2. In Grafana data source settings, use:
   - Host: `timescaledb:5432` (inside Docker) or `localhost:5432` (outside)
   - Database: `football`
   - User: `grafana`
   - Password: `grafana_pass`
   - SSL Mode: `disable`

### Data Loading Errors

```bash
# Check Python dependencies
pip list | grep -E "psycopg2|pandas|numpy"

# Verify data files exist
ls -la data/Sample_Game_1/

# Check database connectivity
docker exec football_timescaledb psql -U grafana -d football -c "SELECT COUNT(*) FROM tracking;"
```

### Performance Issues

For large datasets:

- Increase chunk size: Edit `chunk_size` parameter in `load_data.py` (default: 10000)
- Add more indexes if needed
- Use TimescaleDB compression for older data

## 📚 Resources

- [Metrica Sports Sample Data](https://github.com/metrica-sports/sample-data)
- [TimescaleDB Documentation](https://docs.timescale.com/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Football Analytics with Python](https://github.com/Friends-of-Tracking-Data-FoTD)

## 📝 License

This project uses sample data from Metrica Sports. Please refer to their repository for data licensing terms.

## 🤝 Contributing

Contributions welcome! Areas for improvement:

- Additional visualization templates
- Advanced analytics queries
- Support for FIFA EPTS format (Sample_Game_3)
- Real-time data ingestion
- Player tracking algorithms (speed, distance, formations)
