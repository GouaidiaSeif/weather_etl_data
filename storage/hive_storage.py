"""Hive-style partitioned storage for raw API data.

Stores data as JSON objects organized by city/year/month/day partitions.
- Weather: Hour in filename (weather_{HH}_raw.json)
- Air Quality: Hour in filename (air_quality_{HH}_raw.json)

Both APIs run hourly. Hours are normalized to UTC to ensure synchronization.
"""

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from storage.data_store import DataStore
from utils.logger import get_logger
from utils.timezone_utils import PARIS_TZ, floor_to_paris_hour

logger = get_logger(__name__)


class HivePartitionedStorage:
    """Storage backend with Hive-style partitioning.

    Organizes objects in a hierarchical structure:
    {prefix}/city={city_name}/year={YYYY}/month={MM}/day={DD}/

    Weather files: weather_{HH}_raw.json (hourly, UTC)
    Air Quality files: air_quality_{HH}_raw.json (hourly, UTC)
    """

    def __init__(self, store: DataStore, prefix: str = "raw"):
        self._store = store
        self._prefix = prefix.strip("/")
        logger.info(f"Initialized HivePartitionedStorage with prefix '{self._prefix}'")

    def _object_key(self, *parts: str) -> str:
        segments = [self._prefix, *parts] if self._prefix else list(parts)
        return "/".join(segments)

    def _parse_timezone_offset(self, tz_str: str) -> Optional[timezone]:
        if not tz_str or tz_str == "Z":
            return timezone.utc

        tz_str = tz_str.strip()
        pattern = r"^([+-]?)(\d{1,2}):?(\d{2})$"
        match = re.match(pattern, tz_str)

        if not match:
            logger.warning(f"Unrecognized timezone format: {tz_str}, defaulting to UTC")
            return timezone.utc

        sign_str, hours_str, minutes_str = match.groups()
        sign = -1 if sign_str == "-" else 1
        hours = int(hours_str)
        minutes = int(minutes_str)

        offset = timedelta(hours=sign * hours, minutes=sign * minutes)
        return timezone(offset)

    def _extract_timestamp_from_data(
        self,
        data: Dict[str, Any],
        api_source: str,
    ) -> Optional[datetime]:
        try:
            if api_source == "openweather" or api_source == "openweather_air_forecast":
                dt_value = None

                if isinstance(data, dict):
                    if "hourly" in data and isinstance(data["hourly"], list) and len(data["hourly"]) > 0:
                        dt_value = data["hourly"][0].get("dt")
                    elif "current" in data and isinstance(data["current"], dict):
                        dt_value = data["current"].get("dt")
                    elif "dt" in data:
                        dt_value = data["dt"]
                    # Get dt for forecast air quality data
                    elif "list" in data and isinstance(data["list"], list) :
                        dt_value = data["list"][0].get("dt")

                if dt_value:
                    return datetime.fromtimestamp(int(dt_value), tz=timezone.utc)
                return datetime.now(timezone.utc)

            if api_source == "aqicn":
                api_data = data.get("data", {})
                time_info = api_data.get("time", {})

                if "v" in time_info:
                    try:
                        unix_ts = int(time_info["v"])
                        return datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse AQICN unix timestamp {time_info.get('v')}: {e}")

                if "s" in time_info:
                    time_str = time_info["s"]
                    tz_str = time_info.get("tz", "+00:00")

                    try:
                        local_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                        local_tz = self._parse_timezone_offset(tz_str)
                        local_time = local_time.replace(tzinfo=local_tz)
                        return local_time.astimezone(timezone.utc)
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse AQICN local time '{time_str}' with tz '{tz_str}': {e}"
                        )

                return datetime.now(timezone.utc)

        except Exception as e:
            logger.warning(f"Failed to extract timestamp from {api_source} data: {e}")

        return datetime.now(timezone.utc)

    def _build_partition_parts(self, city_name: str, timestamp: datetime) -> List[str]:
        safe_city = self._sanitize_name(city_name)
        return [
            f"city={safe_city}",
            f"year={timestamp.strftime('%Y')}",
            f"month={timestamp.strftime('%m')}",
            f"day={timestamp.strftime('%d')}",
        ]

    def _build_filename(self, api_source: str, hour_timestamp: datetime) -> str:
        if hour_timestamp.tzinfo is None:
            hour_timestamp = hour_timestamp.replace(tzinfo=timezone.utc)
        hour_str = hour_timestamp.astimezone(PARIS_TZ).strftime("%H")

        if api_source == "openweather":
            return f"weather_{hour_str}_raw.json"
        if api_source == "aqicn":
            return f"air_quality_{hour_str}_raw.json"
        elif api_source == "openweather_air_forecast":
            return f"air_forecast_{hour_str}_raw.json"
        return f"{api_source}_{hour_str}_raw.json"
    
    def hourly_object_key(
        self,
        city_name: str,
        hour_timestamp: datetime,
        api_source: str,
    ) -> str:
        """Object key for an hourly raw file (same layout as save_hourly_data)."""
        if hour_timestamp.tzinfo is None:
            hour_timestamp = hour_timestamp.replace(tzinfo=timezone.utc)
        partition_parts = self._build_partition_parts(city_name, hour_timestamp)
        filename = self._build_filename(api_source, hour_timestamp)
        return self._object_key(*partition_parts, filename)

    def list_raw_keys_for_city_day(
        self,
        city_name: str,
        day: datetime,
        api_source: str,
    ) -> List[str]:
        """List hourly raw object keys for a city on the calendar day of ``day``."""
        if day.tzinfo is None:
            day = day.replace(tzinfo=timezone.utc)
        keys: List[str] = []
        seen: set = set()
        for ts in (day.astimezone(PARIS_TZ), day.astimezone(timezone.utc)):
            for key in self.list_files(
                city_name=city_name,
                year=ts.strftime("%Y"),
                month=ts.strftime("%m"),
                day=ts.strftime("%d"),
                api_source=api_source,
            ):
                if key not in seen:
                    seen.add(key)
                    keys.append(key)
        return keys

    def save_hourly_data(
        self,
        data: Dict[str, Any],
        api_source: str,
        city_name: str,
        hour_timestamp: Optional[datetime] = None,
    ) -> str:
        if hour_timestamp is None:
            hour_timestamp = self._extract_timestamp_from_data(data, api_source)

        if hour_timestamp.tzinfo is None:
            hour_timestamp = hour_timestamp.replace(tzinfo=timezone.utc)

        partition_parts = self._build_partition_parts(city_name, hour_timestamp)
        filename = self._build_filename(api_source, hour_timestamp)
        object_key = self._object_key(*partition_parts, filename)

        data_with_meta = dict(data) if isinstance(data, dict) else {"data": data}
        if hour_timestamp.tzinfo is None:
            hour_timestamp = hour_timestamp.replace(tzinfo=timezone.utc)
        paris_ts = hour_timestamp.astimezone(PARIS_TZ)

        if "_storage" not in data_with_meta:
            data_with_meta["_storage"] = {
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "filepath": object_key,
                "api_source": api_source,
                "city": city_name,
                "hour_timestamp": paris_ts.isoformat(),
                "hour_timestamp_utc": hour_timestamp.astimezone(timezone.utc).isoformat(),
                "hour_timestamp_paris": paris_ts.isoformat(),
                "data_type": "hourly",
            }

        try:
            self._store.put_json(object_key, data_with_meta)
            logger.info(
                f"Saved {api_source} hourly data (UTC hour {hour_timestamp.strftime('%H')}) to {object_key}"
            )
            return object_key

        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize data to JSON: {e}")
            raise ValueError(f"Data cannot be serialized to JSON: {e}") from e
        except Exception as e:
            logger.error(f"Failed to write object {object_key}: {e}")
            raise IOError(f"Cannot write to {object_key}: {e}") from e

    def save_weather_hourly_records(
        self,
        api_response: Dict[str, Any],
        city_name: str,
        target_hour: Optional[datetime] = None,
        hours_back: int = 1,
    ) -> List[str]:
        saved_keys: List[str] = []
        hourly_data = api_response.get("hourly", [])

        if not hourly_data:
            logger.warning(f"No hourly data found for {city_name}")
            return saved_keys

        if target_hour is None:
            current_dt = api_response.get("current", {}).get("dt")
            if current_dt:
                target_hour = datetime.fromtimestamp(int(current_dt), tz=timezone.utc)
            else:
                target_hour = datetime.now(timezone.utc)

        ref_paris = floor_to_paris_hour(target_hour)
        logger.info(
            f"Extracting weather for {city_name} at Paris hour "
            f"{ref_paris.strftime('%Y-%m-%d %H:%M %Z')}"
        )

        # for hour_record in hourly_data:
        #     hour_dt = hour_record.get("dt")
        #     if not hour_dt:
        #         continue

           # get the first hour record (actual hour)
        hour_dt = hourly_data[0].get("dt")
        hour_time = datetime.fromtimestamp(int(hour_dt), tz=timezone.utc)
        weather_paris = floor_to_paris_hour(hour_time)
        
        if weather_paris != ref_paris:
            logger.warning(
                    f"Weather hour {weather_paris.isoformat()} != reference Paris hour "
                    f"{ref_paris.isoformat()} for {city_name}; using reference hour"
                )
            # continue

        full_record = {
            "hourly": hourly_data,
            "lat": api_response.get("lat"),
            "lon": api_response.get("lon"),
            "timezone": api_response.get("timezone"),
            "timezone_offset": api_response.get("timezone_offset"),
            "_metadata": api_response.get("_metadata", {}),
        }

        hour_time = weather_paris
        try:
            object_key = self.save_hourly_data(
                data=full_record,
                api_source="openweather",
                city_name=city_name,
                hour_timestamp=hour_time,
            )
            saved_keys.append(object_key)
        except Exception as e:
            logger.error(f"Failed to save hour {hour_time} for {city_name}: {e}")


        logger.info(f"Saved {len(saved_keys)} hourly weather records for {city_name}")
        return saved_keys

    def save_air_quality_data(
        self,
        api_response: Dict[str, Any],
        city_name: str,
        target_hour: Optional[datetime] = None,
    ) -> str:
        hour_timestamp = self._extract_timestamp_from_data(api_response, "aqicn")

        if target_hour is not None:
            ref_paris = floor_to_paris_hour(target_hour)
            aq_paris = floor_to_paris_hour(hour_timestamp)
            if aq_paris != ref_paris:
                logger.warning(
                    f"AQICN hour {aq_paris.isoformat()} != reference Paris hour "
                    f"{ref_paris.isoformat()} for {city_name}; using reference hour"
                )
            hour_timestamp = ref_paris

        return self.save_hourly_data(
            data=api_response,
            api_source="aqicn",
            city_name=city_name,
            hour_timestamp=hour_timestamp,
        )

    def save_air_forecast_data(
        self,
        api_response: Dict[str, Any],
        city_name: str,
        target_hour: Optional[datetime] = None,
    ) -> str:
        """Save hourly air quality forecast records from OpenWeather API response.
                            
        Args:
            api_response: Full API response from OpenWeather
            city_name: Name of the city
            target_hour: Optional target hour to extract (defaults to current UTC time)
            
        Returns:
            raw data object key 
        """
        hourly_data = api_response.get("list", [])
        
        if not hourly_data:
            logger.warning(f"No forecast data found for {city_name}")
            return ""
        
        # Use provided target hour or extract from API response
        if target_hour is None:
            current_dt = api_response.get("list", [])[0].get("dt")
            if current_dt:
                target_hour = datetime.fromtimestamp(int(current_dt), tz=timezone.utc)
            else:
                target_hour = datetime.now(timezone.utc)
        
        ref_paris = floor_to_paris_hour(target_hour) 
        logger.info(
            f"Extracting forecast pollution for {city_name} at Paris hour "
            f"{ref_paris.strftime('%Y-%m-%d %H:%M %Z')}"
        )            
           
        # get the first hour record (actual hour)
        hour_dt = hourly_data[0].get("dt")
        hour_time = datetime.fromtimestamp(int(hour_dt), tz=timezone.utc)
        hour_forecast_paris = floor_to_paris_hour(hour_time)
        
        if hour_forecast_paris != ref_paris:
            logger.warning(
                    f"Forecast hour {hour_forecast_paris.isoformat()} != reference Paris hour "
                    f"{ref_paris.isoformat()} for {city_name}; using reference hour"
                )
        
        raw_data = self.save_hourly_data(
                data=api_response,
                api_source="openweather_air_forecast",
                city_name=city_name,
                hour_timestamp=hour_forecast_paris
            )
        # saved_keys.append(object_key)
        
        logger.info(f"Saved hourly pollution forecast records at {hour_time} for {city_name}")
        return raw_data

    def load(
        self,
        city_name: str,
        year: str,
        month: str,
        day: str,
        hour: str,
        api_source: str = "openweather",
    ) -> Dict[str, Any]:
        safe_city = self._sanitize_name(city_name)
        hour_dt = datetime(int(year), int(month), int(day), int(hour), tzinfo=timezone.utc)
        filename = self._build_filename(api_source, hour_dt)
        object_key = self._object_key(
            f"city={safe_city}",
            f"year={year}",
            f"month={month}",
            f"day={day}",
            filename,
        )

        try:
            data = self._store.get_json(object_key)
            logger.debug(f"Loaded {api_source} data from {object_key}")
            return data
        except FileNotFoundError:
            logger.error(f"Object not found: {object_key}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {object_key}: {e}")
            raise ValueError(f"Invalid JSON in {object_key}") from e

    def list_files(
        self,
        city_name: Optional[str] = None,
        year: Optional[str] = None,
        month: Optional[str] = None,
        day: Optional[str] = None,
        api_source: Optional[str] = None,
    ) -> List[str]:
        prefix_parts = [self._prefix] if self._prefix else []

        if city_name:
            prefix_parts.append(f"city={self._sanitize_name(city_name)}")
            if year:
                prefix_parts.append(f"year={year}")
                if month:
                    prefix_parts.append(f"month={month}")
                    if day:
                        prefix_parts.append(f"day={day}")

        prefix = "/".join(prefix_parts)
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        if api_source == "openweather":
            pattern = "weather_*_raw.json"
        elif api_source == "aqicn":
            pattern = "air_quality_*_raw.json"
        else:
            pattern = "*.json"

        return self._store.list_keys(prefix=prefix, pattern=pattern)

    def _sanitize_name(self, name: str) -> str:
        sanitized = name.replace(" ", "_").replace("/", "_").replace("\\", "_")
        return sanitized.lower()
