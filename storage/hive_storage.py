"""Hive-style partitioned storage for raw API data.

Stores data as JSON files organized by city/year/month/day partitions.
- Weather: Hour in filename (weather_{HH}_raw.json)
- Air Quality: Hour in filename (air_quality_{HH}_raw.json)

Both APIs run hourly. Hours are normalized to UTC to ensure synchronization.
"""

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


class HivePartitionedStorage:
    """Storage backend with Hive-style partitioning.
    
    Organizes files in a hierarchical structure:
    {base_path}/city={city_name}/year={YYYY}/month={MM}/day={DD}/
    
    Weather files: weather_{HH}_raw.json (hourly, UTC)
    Air Quality files: air_quality_{HH}_raw.json (hourly, UTC)
    
    Hours are normalized to UTC to ensure synchronization between APIs.
    """
    
    def __init__(self, base_path: Path):
        """Initialize the storage backend.
        
        Args:
            base_path: Root directory for storing data
        """
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized HivePartitionedStorage at {self._base_path}")
    
    def _parse_timezone_offset(self, tz_str: str) -> Optional[timezone]:
        """Parse timezone offset string to timezone object.
        
        Handles formats: '+01:00', '+0100', '-05:00', '-0500', '+00:00', 'Z'
        
        Args:
            tz_str: Timezone offset string
            
        Returns:
            Optional[timezone]: Parsed timezone or None if invalid
        """
        if not tz_str or tz_str == 'Z':
            return timezone.utc
        
        tz_str = tz_str.strip()
        
        # Match pattern: optional + or -, followed by 1-2 digits for hours, 
        # optional colon, followed by 2 digits for minutes
        pattern = r'^([+-]?)(\d{1,2}):?(\d{2})$'
        match = re.match(pattern, tz_str)
        
        if not match:
            logger.warning(f"Unrecognized timezone format: {tz_str}, defaulting to UTC")
            return timezone.utc
        
        sign_str, hours_str, minutes_str = match.groups()
        sign = -1 if sign_str == '-' else 1
        hours = int(hours_str)
        minutes = int(minutes_str)
        
        offset = timedelta(hours=sign * hours, minutes=sign * minutes)
        return timezone(offset)
    
    def _extract_timestamp_from_data(
        self, 
        data: Dict[str, Any], 
        api_source: str
    ) -> Optional[datetime]:
        """Extract timestamp from API response data and convert to UTC.
        
        Args:
            data: Raw API response data
            api_source: Name of the API source
            
        Returns:
            Optional[datetime]: Extracted timestamp in UTC
        """
        try:
            if api_source == "openweather":
                # OpenWeather uses Unix timestamp (UTC) in multiple possible locations
                dt_value = None
                
                if isinstance(data, dict):
                    # Priority 1: hourly.dt (for hourly forecast data)
                    if "hourly" in data and isinstance(data["hourly"], list) and len(data["hourly"]) > 0:
                        # Get the first hourly record's timestamp
                        dt_value = data["hourly"][0].get("dt")
                    
                    # Priority 2: current.dt (for current weather)
                    elif "current" in data and isinstance(data["current"], dict):
                        dt_value = data["current"].get("dt")
                    
                    # Priority 3: dt at root level
                    elif "dt" in data:
                        dt_value = data["dt"]
                
                if dt_value:
                    return datetime.fromtimestamp(int(dt_value), tz=timezone.utc)
                return datetime.now(timezone.utc)
                
            elif api_source == "aqicn":
                # AQICN API structure: data.time.v contains Unix timestamp (UTC)
                api_data = data.get("data", {})
                time_info = api_data.get("time", {})
                
                # First priority: Unix timestamp in time.v (always UTC)
                if "v" in time_info:
                    try:
                        unix_ts = int(time_info["v"])
                        return datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse AQICN unix timestamp {time_info.get('v')}: {e}")
                
                # Fallback: Parse local time string with timezone
                if "s" in time_info:
                    time_str = time_info["s"]  # e.g., "2026-02-08 14:00:00"
                    tz_str = time_info.get("tz", "+00:00")  # e.g., "+01:00"
                    
                    try:
                        local_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                        local_tz = self._parse_timezone_offset(tz_str)
                        local_time = local_time.replace(tzinfo=local_tz)
                        utc_time = local_time.astimezone(timezone.utc)
                        return utc_time
                    except Exception as e:
                        logger.warning(f"Failed to parse AQICN local time '{time_str}' with tz '{tz_str}': {e}")
                
                return datetime.now(timezone.utc)
                
        except Exception as e:
            logger.warning(f"Failed to extract timestamp from {api_source} data: {e}")
        
        return datetime.now(timezone.utc)
    
    def _build_partition_path(
        self, 
        city_name: str, 
        timestamp: datetime
    ) -> Path:
        """Build the partition directory path (up to day level).
        
        Args:
            city_name: Name of the city
            timestamp: Timestamp for partitioning (UTC)
            
        Returns:
            Path: Partition directory path (day level)
        """
        safe_city = self._sanitize_name(city_name)
        
        return (
            self._base_path /
            f"city={safe_city}" /
            f"year={timestamp.strftime('%Y')}" /
            f"month={timestamp.strftime('%m')}" /
            f"day={timestamp.strftime('%d')}"
        )
    
    def _build_filename(self, api_source: str, hour_timestamp: datetime) -> str:
        """Build the filename with hour (UTC).
        
        Args:
            api_source: Name of the API source
            hour_timestamp: Timestamp for the hour (UTC)
            
        Returns:
            str: Filename with hour (e.g., "weather_13_raw.json" or "air_quality_13_raw.json")
        """
        hour_str = hour_timestamp.strftime('%H')
        
        if api_source == "openweather":
            return f"weather_{hour_str}_raw.json"
        elif api_source == "aqicn":
            return f"air_quality_{hour_str}_raw.json"
        else:
            return f"{api_source}_{hour_str}_raw.json"
    
    def save_hourly_data(
        self,
        data: Dict[str, Any],
        api_source: str,
        city_name: str,
        hour_timestamp: Optional[datetime] = None
    ) -> Path:
        """Save hourly data to a partitioned JSON file.
        
        Args:
            data: Raw API response data for a single hour
            api_source: Name of the API source ("openweather" or "aqicn")
            city_name: Name of the city
            hour_timestamp: Optional timestamp for the hour (extracted from data if not provided, converted to UTC)
            
        Returns:
            Path: Path to the saved file
        """
        if hour_timestamp is None:
            hour_timestamp = self._extract_timestamp_from_data(data, api_source)
        
        # Ensure we have UTC time for consistent hour extraction
        if hour_timestamp.tzinfo is None:
            hour_timestamp = hour_timestamp.replace(tzinfo=timezone.utc)
        
        directory = self._build_partition_path(city_name, hour_timestamp)
        directory.mkdir(parents=True, exist_ok=True)
        
        filename = self._build_filename(api_source, hour_timestamp)
        filepath = directory / filename
        
        data_with_meta = dict(data) if isinstance(data, dict) else {"data": data}
        if "_storage" not in data_with_meta:
            data_with_meta["_storage"] = {
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "filepath": str(filepath),
                "api_source": api_source,
                "city": city_name,
                "hour_timestamp": hour_timestamp.isoformat(),
                "data_type": "hourly"
            }
        
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data_with_meta, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {api_source} hourly data (UTC hour {hour_timestamp.strftime('%H')}) to {filepath}")
            return filepath
            
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize data to JSON: {e}")
            raise ValueError(f"Data cannot be serialized to JSON: {e}") from e
            
        except IOError as e:
            logger.error(f"Failed to write file {filepath}: {e}")
            raise IOError(f"Cannot write to {filepath}: {e}") from e
    
    def save_weather_hourly_records(
        self,
        api_response: Dict[str, Any],
        city_name: str,
        target_hour: Optional[datetime] = None,
        hours_back: int = 1
    ) -> List[Path]:
        """Save hourly weather records from OpenWeather API response.
        
        Extracts the past N hours of data and saves each hour separately.
        
        Args:
            api_response: Full API response from OpenWeather
            city_name: Name of the city
            target_hour: Optional target hour to extract (defaults to current UTC time)
            hours_back: Number of past hours to save (default: 1 for hourly runs)
            
        Returns:
            List[Path]: List of saved file paths
        """
        saved_paths = []
        hourly_data = api_response.get("hourly", [])
        
        if not hourly_data:
            logger.warning(f"No hourly data found for {city_name}")
            return saved_paths
        
        # Use provided target hour or extract from API response
        if target_hour is None:
            current_dt = api_response.get("current", {}).get("dt")
            if current_dt:
                target_hour = datetime.fromtimestamp(int(current_dt), tz=timezone.utc)
            else:
                target_hour = datetime.now(timezone.utc)
        
        cutoff_time = target_hour - timedelta(hours=hours_back)
        
        logger.info(f"Extracting hourly data for {city_name} from {cutoff_time} to {target_hour} (UTC)")
        
        for hour_record in hourly_data:
            hour_dt = hour_record.get("dt")
            if not hour_dt:
                continue
            
            hour_time = datetime.fromtimestamp(int(hour_dt), tz=timezone.utc)
            
            if cutoff_time <= hour_time <= target_hour:
                full_record = {
                    "hourly": hour_record,
                    "lat": api_response.get("lat"),
                    "lon": api_response.get("lon"),
                    "timezone": api_response.get("timezone"),
                    "timezone_offset": api_response.get("timezone_offset"),
                    "_metadata": api_response.get("_metadata", {})
                }
                
                try:
                    filepath = self.save_hourly_data(
                        data=full_record,
                        api_source="openweather",
                        city_name=city_name,
                        hour_timestamp=hour_time
                    )
                    saved_paths.append(filepath)
                except Exception as e:
                    logger.error(f"Failed to save hour {hour_time} for {city_name}: {e}")
        
        logger.info(f"Saved {len(saved_paths)} hourly weather records for {city_name}")
        return saved_paths
    
    def save_air_quality_data(
        self,
        api_response: Dict[str, Any],
        city_name: str,
        target_hour: Optional[datetime] = None
    ) -> Path:
        """Save air quality data from AQICN API response.
        
        Args:
            api_response: Full API response from AQICN
            city_name: Name of the city
            target_hour: Optional target hour to use for filename (extracted from API response if not provided)
            
        Returns:
            Path: Path to saved file
        """
        # Extract timestamp from AQICN data
        hour_timestamp = self._extract_timestamp_from_data(api_response, "aqicn")
        
        # If target_hour is provided, use it to override (for synchronization)
        if target_hour is not None:
            # Keep the date from the extracted timestamp, but use the hour from target_hour
            hour_timestamp = hour_timestamp.replace(hour=target_hour.hour)
        
        return self.save_hourly_data(
            data=api_response,
            api_source="aqicn",
            city_name=city_name,
            hour_timestamp=hour_timestamp
        )
    
    def load(
        self,
        city_name: str,
        year: str,
        month: str,
        day: str,
        hour: str,
        api_source: str = "openweather"
    ) -> Dict[str, Any]:
        """Load data from a partitioned JSON file.
        
        Args:
            city_name: Name of the city
            year: Year (YYYY)
            month: Month (MM)
            day: Day (DD)
            hour: Hour (HH, UTC)
            api_source: API source name ("openweather" or "aqicn")
            
        Returns:
            Dict[str, Any]: Loaded data
        """
        safe_city = self._sanitize_name(city_name)
        
        # Create a datetime for filename generation
        hour_dt = datetime(int(year), int(month), int(day), int(hour), tzinfo=timezone.utc)
        filename = self._build_filename(api_source, hour_dt)
        
        filepath = (
            self._base_path /
            f"city={safe_city}" /
            f"year={year}" /
            f"month={month}" /
            f"day={day}" /
            filename
        )
        
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.debug(f"Loaded {api_source} data from {filepath}")
            return data
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            raise
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {filepath}: {e}")
            raise ValueError(f"Invalid JSON in {filepath}") from e
    
    def list_files(
        self,
        city_name: Optional[str] = None,
        year: Optional[str] = None,
        month: Optional[str] = None,
        day: Optional[str] = None,
        api_source: Optional[str] = None
    ) -> List[Path]:
        """List all data files matching criteria.
        
        Args:
            city_name: Filter by city
            year: Filter by year
            month: Filter by month
            day: Filter by day
            api_source: Filter by API source ("openweather" or "aqicn")
            
        Returns:
            List[Path]: List of file paths
        """
        pattern_parts = []
        
        if city_name:
            pattern_parts.append(f"city={self._sanitize_name(city_name)}")
            if year:
                pattern_parts.append(f"year={year}")
                if month:
                    pattern_parts.append(f"month={month}")
                    if day:
                        pattern_parts.append(f"day={day}")
        
        if pattern_parts:
            search_path = self._base_path / "/".join(pattern_parts)
        else:
            search_path = self._base_path
        
        files = []
        
        if not search_path.exists():
            return files
        
        # Determine pattern based on api_source
        if api_source == "openweather":
            pattern = "weather_*_raw.json"
        elif api_source == "aqicn":
            pattern = "air_quality_*_raw.json"
        else:
            pattern = "*.json"
        
        # Look in day directories
        day_dirs = list(search_path.glob("**/day=*")) if "day=" not in str(search_path) else [search_path]
        
        for day_dir in day_dirs:
            if day_dir.is_dir():
                files.extend(day_dir.glob(pattern))
        
        # Also check if search_path itself contains matching files
        if search_path.is_dir():
            files.extend(search_path.glob(pattern))
        
        return sorted(files)
    
    def _sanitize_name(self, name: str) -> str:
        """Sanitize a name for use in directory names."""
        sanitized = name.replace(" ", "_").replace("/", "_").replace("\\", "_")
        return sanitized.lower()