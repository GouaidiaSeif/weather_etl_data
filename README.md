# Weather ETL Pipeline

## Project Structure

```
weather_etl/
├── config/
│   ├── __init__.py
│   ├── settings.py          # Configuration management
│   └── towns.py             # Top 10 French towns with coordinates
├── clients/
│   ├── __init__.py
│   ├── base_client.py       # Abstract base for API clients
│   ├── openweather_client.py
│   └── aqicn_client.py
├── storage/
│   ├── __init__.py
│   └── hive_storage.py      # Hive-style partitioned storage
├── etl/
│   ├── __init__.py
│   └── pipeline.py          # Main ETL orchestrator
├── utils/
│   ├── __init__.py
│   ├── logger.py            # Logging configuration
│   └── retry.py             # Retry decorator
├── data/
│   ├── raw/                 # Raw API responses (auto-created)
│   └── processed/           # Processed data (auto-created)
├── scheduler.py             # APScheduler (hourly for both APIs)
├── fetch_data.py            # CLI script for on-demand execution
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Data Storage Structure

Raw data is stored in Hive-style partitions:

```
data/raw/
├── city=paris/
│   └── year=2026/
│       └── month=02/
│           └── day=08/
│               ├── weather_13_raw.json       (1 PM weather)
│               ├── weather_14_raw.json       (2 PM weather)
│               ├── weather_15_raw.json       (3 PM weather)
│               ├── air_quality_13_raw.json   (1 PM air quality)
│               ├── air_quality_14_raw.json   (2 PM air quality)
│               └── air_quality_15_raw.json   (3 PM air quality)
├── city=lyon/
│   └── ...
└── city=marseille/
    └── ...
```

**File naming convention:**
- Weather data: `weather_{HH}_raw.json` (e.g., `weather_13_raw.json` for 1 PM)
- Air quality data: `air_quality_{HH}_raw.json` (e.g., `air_quality_13_raw.json` for 1 PM)

**Note:** The hour is extracted from each API's response timestamp to ensure synchronization. If weather returns hour 15 and air quality returns hour 14, they will be saved to different files (`weather_15_raw.json` and `air_quality_14_raw.json`).

## Installation

1. Clone the repository:
```bash
git clone https://github.com/A5apFloki/weather_etl.git
cd weather_etl
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys
```

## Configuration

Create a `.env` file with your API keys:

```bash
OPENWEATHER_API_KEY=your_openweather_api_key
AQICN_API_KEY=your_aqicn_api_token
```

### Getting API Keys

- **OpenWeatherMap**: Sign up at https://openweathermap.org/api
- **AQICN**: Get a token at https://aqicn.org/data-platform/token/

## Usage

### Run Scheduler (Recommended)

Start the scheduler to run automatically every hour:
```bash
python scheduler.py
```

The scheduler will:
1. Run an initial ETL job immediately
2. Schedule ETL job to run every hour (both weather and air quality)
3. Log all activities

### Run On-Demand

Fetch both weather and air quality data for all towns:
```bash
python fetch_data.py
```

Fetch weather data only:
```bash
python fetch_data.py --weather
```

Fetch air quality data only:
```bash
python fetch_data.py --air-quality
```

Fetch data for a specific town:
```bash
python fetch_data.py --town paris
```

Extract past 3 hours of weather data:
```bash
python fetch_data.py --hours 3
```

List available towns:
```bash
python fetch_data.py --list-towns
```

### Use as a Library

```python
from etl.pipeline import run_hourly_etl_job

# Run hourly ETL (both weather and air quality)
summary = run_hourly_etl_job(hours_back=1)

# Run for specific towns
from config.towns import FRENCH_TOWNS
paris = [t for t in FRENCH_TOWNS if t.name == "paris"]
summary = run_hourly_etl_job(towns=paris, hours_back=1)

print(f"Saved {summary['total_files_saved']} files")
print(f"Success rate: {summary['success_rate']:.1%}")
```

### Query Stored Data

```python
from storage.hive_storage import HivePartitionedStorage
from pathlib import Path

storage = HivePartitionedStorage(Path("data/raw"))

# List all weather files for Paris
weather_files = storage.list_files(city_name="paris", api_source="openweather")

# List all air quality files for Paris
aq_files = storage.list_files(city_name="paris", api_source="aqicn")

# Load specific hour weather data
data = storage.load(
    city_name="paris",
    year="2026",
    month="02",
    day="08",
    hour="15",
    api_source="openweather"
)

# Load specific hour air quality data
data = storage.load(
    city_name="paris",
    year="2026",
    month="02",
    day="08",
    hour="15",
    api_source="aqicn"
)
```

## API Response Format

### Weather Data (`weather_{HH}_raw.json`)

```json
{
  "hourly": {
    "dt": 1739014800,
    "temp": 8.5,
    "feels_like": 6.2,
    "pressure": 1024,
    "humidity": 76,
    "uvi": 1.2,
    "clouds": 40,
    "visibility": 10000,
    "wind_speed": 3.5,
    "wind_deg": 250,
    "pop": 0.1,
    "weather": [...]
  },
  "lat": 48.8566,
  "lon": 2.3522,
  "timezone": "Europe/Paris",
  "_metadata": {...},
  "_storage": {
    "saved_at": "2026-02-08T15:30:00",
    "filepath": "...",
    "api_source": "openweather",
    "city": "paris",
    "hour_timestamp": "2026-02-08T15:00:00",
    "data_type": "hourly"
  }
}
```

### Air Quality Data (`air_quality_{HH}_raw.json`)

```json
{
  "status": "ok",
  "data": {
    "aqi": 45,
    "idx": 1234,
    "time": {"s": "2026-02-08 15:00:00", "tz": "+01:00"},
    "city": {...},
    "iaqi": {...}
  },
  "_metadata": {...},
  "_storage": {
    "saved_at": "2026-02-08T15:30:00",
    "filepath": "...",
    "api_source": "aqicn",
    "city": "paris",
    "hour_timestamp": "2026-02-08T15:00:00",
    "data_type": "hourly"
  }
}
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENWEATHER_API_KEY` | Yes | - | OpenWeatherMap API key |
| `AQICN_API_KEY` | Yes | - | AQICN API token |
| `DATA_RAW_PATH` | No | `./data/raw` | Path for raw data |
| `DATA_PROCESSED_PATH` | No | `./data/processed` | Path for processed data |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `REQUEST_TIMEOUT` | No | `30` | HTTP timeout in seconds |
| `MAX_RETRIES` | No | `3` | Max retries for API calls |

## Cities Covered

| City | Latitude | Longitude |
|------|----------|-----------|
| Paris | 48.8566 | 2.3522 |
| Marseille | 43.2965 | 5.3698 |
| Lyon | 45.7640 | 4.8357 |
| Toulouse | 43.6047 | 1.4442 |
| Nice | 43.7102 | 7.2620 |
| Nantes | 47.2184 | -1.5536 |
| Montpellier | 43.6108 | 3.8767 |
| Strasbourg | 48.5734 | 7.7521 |
| Bordeaux | 44.8378 | -0.5792 |
| Lille | 50.6292 | 3.0573 |

## Scheduling Details

### Hourly ETL (Every Hour)
- Runs every hour
- Fetches both weather and air quality data
- Weather: Extracts past 1 hour of data by default
- Air Quality: Extracts current hour from API response
- Saves each with hour extracted from API response timestamp
- Example: At 15:00 run, saves `weather_15_raw.json` and `air_quality_15_raw.json`

**Note on Hour Synchronization:**
The hour for each file is extracted from the API response timestamp:
- OpenWeather: Uses the `dt` field (Unix timestamp)
- AQICN: Uses the `data.time.s` field (ISO timestamp)

If the APIs return different hours (e.g., weather=15, air_quality=14), they will be saved to separate files. This ensures data accuracy and prevents mixing data from different hours.

## License

MIT License
