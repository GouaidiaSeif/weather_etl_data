"""
Improved Weather data transformation and standardization.
Converts raw OpenWeather API responses into cleaned analytical format.

FIXED v3: 
- Correctly handles flat structure in hourly data (temp, wind_speed at root of hourly)
- Extracts hour from _storage.hour_timestamp
- Includes all available fields from API
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
from utils.logger import get_logger
from transformations.transformationscommon_cleaning import optional_float
from utils.timezone_utils import (
    PARIS_TZ,
    format_hour_paris,
    parse_storage_timestamp,
    paris_date_str,
    paris_hour,
)

logger = get_logger(__name__)


class WeatherTransformer:
    """Transform and clean weather raw data with full field extraction."""

    # Physical limits for validation
    VALID_RANGES = {
        "temperature_celsius": (-100, 100),
        "humidity_percent": (0, 100),
        "pressure_hpa": (800, 1100),
        "wind_speed_mps": (0, 150),
        "wind_direction_deg": (0, 360),
        "cloud_coverage_percent": (0, 100),
        "uvi": (0, 20),
        "visibility_m": (0, 100000),
        "pop_percent": (0, 100),
        "dew_point_celsius": (-100, 50),
    }

    @staticmethod
    def _validate_field(name: str, value: Any, default: Any = None) -> Any:
        """Validate a field value is within physical limits."""
        if value is None or value == "":
            return default
        try:
            value = float(value)
        except (TypeError, ValueError):
            return default

        if name in WeatherTransformer.VALID_RANGES:
            min_val, max_val = WeatherTransformer.VALID_RANGES[name]
            if not (min_val <= value <= max_val):
                logger.warning(f"Field {name}={value} outside valid range [{min_val}, {max_val}]")
                return default
        return value

    @staticmethod
    def _extract_paris_time(
        raw_record: Dict[str, Any], hourly: Dict[str, Any]
    ) -> Tuple[datetime, datetime, str]:
        """Resolve event time in Europe/Paris from storage metadata or hourly.dt."""
        storage = raw_record.get("_storage", {})
        for key in ("hour_timestamp_paris", "hour_timestamp_utc", "hour_timestamp"):
            dt = parse_storage_timestamp(storage.get(key, ""))
            if dt is not None:
                return dt.astimezone(PARIS_TZ),dt.astimezone(timezone.utc), "storage"

        dt_value = hourly.get("dt")
        if dt_value:
            return (
                datetime.fromtimestamp(int(dt_value), tz=timezone.utc).astimezone(PARIS_TZ),
                datetime.fromtimestamp(int(dt_value), tz=timezone.utc).astimezone(timezone.utc),
                "hourly_dt",
            )

        return datetime.now(timezone.utc).astimezone(PARIS_TZ), datetime.now(timezone.utc).astimezone(timezone.utc), "fallback_now"

    @staticmethod
    def _calculate_heat_index(temp: float, humidity: float) -> Dict[str, Any]:
        """Calculate heat index (feels-like temperature based on temp + humidity).
         - v4 : add warning
        """
        if temp < 27 or humidity < 40:
            return {
                "hi": None,
                "warning": "safe"
        }
            
        # Simplified heat index formula
        hi = (-8.784694755 + 1.61139411 * temp + 2.338548839 * humidity 
              - 0.14611605 * temp * humidity - 0.012308094 * temp**2 
              - 0.016424828 * humidity**2 + 0.002211732 * temp**2 * humidity 
              + 0.00072546 * temp * humidity**2 - 0.000003582 * temp**2 * humidity**2)
        
        hi = round(hi, 2)
        
        if hi < 27:
            warning = "safe"
        elif 27 <= hi <= 32:
            warning = "caution"
        elif 33 <= hi <= 40:
            warning = "extreme caution"
        elif 41 <= hi <= 51:
            warning = "danger"
        else: 
            warning = "extreme danger"
        
        return {
            "hi": round(hi,2),
            "warning": warning
        }

    @staticmethod
    def _classify_weather_severity(
        temp: float, 
        wind_speed: float, 
        wind_gust: Optional[float],
        uvi: Optional[float]
    ) -> str:
        """Classify overall weather severity level."""
        severity_score = 0
        
        # Temperature extremes
        if temp <= -20 or temp >= 40:
            severity_score += 3
        elif temp <= -10 or temp >= 35:
            severity_score += 2
        elif temp <= 0 or temp >= 30:
            severity_score += 1
        
        # Wind severity
        max_wind = wind_gust if wind_gust else wind_speed
        if max_wind >= 30:
            severity_score += 3
        elif max_wind >= 20:
            severity_score += 2
        elif max_wind >= 10:
            severity_score += 1
        
        # UV severity
        if uvi and uvi >= 11:
            severity_score += 2
        elif uvi and uvi >= 8:
            severity_score += 1
        
        if severity_score >= 5:
            return "extreme"
        elif severity_score >= 3:
            return "severe"
        elif severity_score >= 1:
            return "moderate"
        return "normal"

    @staticmethod
    def _calculate_data_quality(
        record: Dict[str, Any], timestamp_source: str
    ) -> Dict[str, Any]:
        """Calculate data quality metrics for the record."""
        core_fields = [
            "temperature_celsius", "humidity_percent", "pressure_hpa",
            "wind_speed_mps", "cloud_coverage_percent",
        ]
        available = sum(1 for f in core_fields if record.get(f) is not None)
        missing = [f for f in core_fields if record.get(f) is None]

        return {
            "completeness_score": round(available / len(core_fields), 2),
            "available_core_fields": available,
            "total_core_fields": len(core_fields),
            "timestamp_source": timestamp_source,
            "missing_core_fields": missing,
        }

    @staticmethod
    def transform(raw_record: Dict[str, Any], city_name: Optional[str] = None) -> Dict[str, Any]:
        """Transform a single hourly weather record with full field extraction.
        
        FIXED v3: Handles flat structure where fields are directly in 'hourly' object.
        
        Args:
            raw_record: The raw JSON record from storage
            city_name: Optional city name override
        
        Returns:
            Dict with cleaned and standardized weather data
        """
        # Extract the hourly data (flat structure)
        hourly = raw_record.get("hourly", {})
        
        if not hourly:
            logger.error("No 'hourly' data found in raw record")
            raise ValueError("Missing hourly data in weather record")
        
        # Separate first element (actual where when api called) and remaining (forecast)
        current, *forecast = hourly
        
        def _process_item(item_hourly : Dict[str, Any]) -> Dict[str, Any] :
        
            # Weather info (array with one element)
            weather_list = item_hourly.get("weather", [])
            weather = weather_list[0] if weather_list else {}
            
            paris_dt, utc_dt, timestamp_source = WeatherTransformer._extract_paris_time(
                item_hourly, item_hourly
            )
            if timestamp_source == "fallback_now":
                raise ValueError(
                    "Unreliable timestamp: no storage metadata or hourly.dt in weather record"
                )

            timestamp = paris_dt.astimezone(timezone.utc).isoformat()
            hour_utc = utc_dt.hour
            hour = paris_hour(paris_dt)
            date_paris = paris_date_str(paris_dt)
            
            # # City name
            # if city_name:
            #     city = city_name.lower()
            # else:
            #     city = raw_record.get("_storage", {}).get("city", "unknown")
            
            # Extract and validate fields (flat structure in hourly)
            temp = WeatherTransformer._validate_field(
                "temperature_celsius", 
                item_hourly.get("temp"),
                default=None
            )
            feels_like = WeatherTransformer._validate_field(
                "temperature_celsius",
                item_hourly.get("feels_like"),
                default=temp
            )
            humidity = WeatherTransformer._validate_field(
                "humidity_percent",
                item_hourly.get("humidity"),
                default=None
            )
            pressure = WeatherTransformer._validate_field(
                "pressure_hpa",
                item_hourly.get("pressure"),
                default=None
            )
            dew_point = WeatherTransformer._validate_field(
                "dew_point_celsius",
                item_hourly.get("dew_point"),
                default=None
            )
            wind_speed = WeatherTransformer._validate_field(
                "wind_speed_mps",
                item_hourly.get("wind_speed"),
                default=None
            )
            wind_gust = WeatherTransformer._validate_field(
                "wind_speed_mps",
                item_hourly.get("wind_gust"),
                default=None
            )
            wind_deg = WeatherTransformer._validate_field(
                "wind_direction_deg",
                item_hourly.get("wind_deg"),
                default=None
            )
            uvi = WeatherTransformer._validate_field(
                "uvi",
                item_hourly.get("uvi"),
                default=None
            )
            clouds = WeatherTransformer._validate_field(
                "cloud_coverage_percent",
                item_hourly.get("clouds"),
                default=None
            )
            visibility = WeatherTransformer._validate_field(
                "visibility_m", item_hourly.get("visibility"), default=None
            )
            raw_pop = item_hourly.get("pop")
            if raw_pop is None:
                pop_percent = None
            else:
                pop_val = optional_float(raw_pop)
                if pop_val is not None:
                    pop_scaled = pop_val * 100 if pop_val <= 1 else pop_val
                    pop_percent = WeatherTransformer._validate_field(
                        "pop_percent", pop_scaled, default=None
                    )
                else:
                    pop_percent = None
            
            # Build transformed record with ALL available fields
            transformed_item = {
                # Core identification
                "timestamp_utc": timestamp,
                "timestamp_paris": paris_dt.isoformat(),
                "date_paris": date_paris,
                "hour": hour_utc,
                "hour_paris": hour,
                "hour_formatted": format_hour_paris(paris_dt),
                "city": city,
                
                # Temperature fields
                "temperature_celsius": round(temp, 2) if temp is not None else None,
                "feels_like_celsius": round(feels_like, 2) if feels_like is not None else None,
                "dew_point_celsius": round(dew_point, 2) if dew_point is not None else None,
                
                # Atmospheric fields
                "humidity_percent": int(humidity) if humidity is not None else None,
                "pressure_hpa": int(pressure) if pressure is not None else None,
                
                # Wind fields
                "wind_speed_mps": round(wind_speed, 2) if wind_speed is not None else None,
                "wind_gust_mps": round(wind_gust, 2) if wind_gust is not None else None,
                "wind_direction_deg": int(wind_deg) if wind_deg is not None else None,
                "wind_direction_cardinal": WeatherTransformer._deg_to_cardinal(wind_deg) if wind_deg else None,
                
                # Sky and visibility
                "cloud_coverage_percent": int(clouds) if clouds is not None else None,
                "visibility_m": visibility,
                
                # Weather condition
                "weather_main": weather.get("main", "unknown").lower() if weather else "unknown",
                "weather_description": weather.get("description", "unknown").lower() if weather else "unknown",
                "weather_icon": weather.get("icon"),
                "weather_id": weather.get("id"),
                
                # Precipitation
                "precipitation_probability_percent": (
                    int(pop_percent) if pop_percent is not None else None
                ),

                # UV index
                "uvi": round(uvi, 2) if uvi is not None else None,
                "uvi_category": WeatherTransformer._categorize_uvi(uvi) if uvi is not None else None,

                # Derived metrics (only when inputs are present)
                "heat_index_celsius": (
                    WeatherTransformer._calculate_heat_index(temp, humidity)["hi"]
                    if temp is not None and humidity is not None
                    else None
                ),
                "weather_severity": (
                    WeatherTransformer._classify_weather_severity(temp, wind_speed, wind_gust, uvi)
                    if temp is not None and wind_speed is not None
                    else None
                ),
                
                "heat_index_warning": (
                    WeatherTransformer._calculate_heat_index(temp, humidity)["warning"]
                    if temp is not None and humidity is not None
                    else None
                ),

                
                # Location metadata
                # "latitude": raw_record.get("lat"),
                # "longitude": raw_record.get("lon"),
                # "timezone": raw_record.get("timezone"),
                # "timezone_offset_seconds": raw_record.get("timezone_offset"),
            }
            return transformed_item
        
        # City name
        if city_name:
            city = city_name.lower()
        else:
            city = raw_record.get("_storage", {}).get("city", "unknown")
        
        _,_, timestamp_source = WeatherTransformer._extract_paris_time(
                raw_record, current
            )
        if timestamp_source == "fallback_now":
            raise ValueError(
                "Unreliable timestamp: no storage metadata or hourly.dt in weather record"
            )
        
        transformed = {
            # Location metadata
            "city" : city,
            "latitude": raw_record.get("lat"),
            "longitude": raw_record.get("lon"),
            "timezone": raw_record.get("timezone"),
            "timezone_offset_seconds": raw_record.get("timezone_offset"),
        }
        
        # add current weather and forecast
        current_data = _process_item(current)
        transformed.update(current_data)
        transformed["forecast"] = [_process_item(item) for item in forecast]
        
        # Add data quality metrics
        transformed["_data_quality"] = WeatherTransformer._calculate_data_quality(
            transformed, timestamp_source
        )
        
        # Add lineage tracking
        transformed["_lineage"] = {
            "transformer": "WeatherTransformer",
            "version": "3.0",
            "transformed_at": datetime.now(timezone.utc).isoformat(),
            "raw_source": raw_record.get("_storage", {}).get("filepath", "unknown")
        }
        
        return transformed
    
    @staticmethod
    def _deg_to_cardinal(deg: float) -> str:
        """Convert wind degrees to cardinal direction."""
        directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                      "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        index = round(deg / 22.5) % 16
        return directions[index]
    
    @staticmethod
    def _categorize_uvi(uvi: float) -> str:
        """Categorize UV index."""
        if uvi <= 2:
            return "low"
        elif uvi <= 5:
            return "moderate"
        elif uvi <= 7:
            return "high"
        elif uvi <= 10:
            return "very_high"
        return "extreme"
