"""
Improved Weather data transformation and standardization.
Converts raw OpenWeather API responses into cleaned analytical format.

FIXED v3: 
- Correctly handles flat structure in hourly data (temp, wind_speed at root of hourly)
- Extracts hour from _storage.hour_timestamp
- Includes all available fields from API
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional
from utils.logger import get_logger

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
    def _validate_field(name: str, value: Optional[float], default: Any = None) -> Any:
        """Validate a field value is within physical limits."""
        if value is None:
            return default
        
        if name in WeatherTransformer.VALID_RANGES:
            min_val, max_val = WeatherTransformer.VALID_RANGES[name]
            if not (min_val <= value <= max_val):
                logger.warning(f"Field {name}={value} outside valid range [{min_val}, {max_val}]")
                return default
        return value

    @staticmethod
    def _extract_hour(raw_record: Dict[str, Any]) -> Optional[int]:
        """Extract hour from _storage.hour_timestamp."""
        storage = raw_record.get("_storage", {})
        hour_ts = storage.get("hour_timestamp", "")
        if hour_ts:
            try:
                # Parse ISO format timestamp
                dt = datetime.fromisoformat(hour_ts.replace("Z", "+00:00"))
                return dt.hour
            except Exception as e:
                logger.warning(f"Failed to parse hour_timestamp: {e}")
        return None

    @staticmethod
    def _calculate_heat_index(temp: float, humidity: float) -> Optional[float]:
        """Calculate heat index (feels-like temperature based on temp + humidity)."""
        if temp < 27 or humidity < 40:
            return None
        
        # Simplified heat index formula
        hi = (-8.784694755 + 1.61139411 * temp + 2.338548839 * humidity 
              - 0.14611605 * temp * humidity - 0.012308094 * temp**2 
              - 0.016424828 * humidity**2 + 0.002211732 * temp**2 * humidity 
              + 0.00072546 * temp * humidity**2 - 0.000003582 * temp**2 * humidity**2)
        return round(hi, 2)

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
    def _calculate_data_quality(record: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate data quality metrics for the record."""
        # Count core weather fields
        core_fields = [
            "temperature_celsius", "humidity_percent", "pressure_hpa",
            "wind_speed_mps", "cloud_coverage_percent"
        ]
        available = sum(1 for f in core_fields if record.get(f) is not None)
        
        return {
            "completeness_score": round(available / len(core_fields), 2),
            "available_core_fields": available,
            "total_core_fields": len(core_fields)
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
        
        # Weather info (array with one element)
        weather_list = hourly.get("weather", [])
        weather = weather_list[0] if weather_list else {}
        
        # Timestamp from hourly.dt (Unix timestamp)
        dt_value = hourly.get("dt")
        if dt_value:
            timestamp = datetime.fromtimestamp(int(dt_value), tz=timezone.utc).isoformat()
        else:
            timestamp = datetime.now(timezone.utc).isoformat()
            logger.warning("No dt found in hourly data, using current time")
        
        # Extract hour from _storage for consistency
        hour = WeatherTransformer._extract_hour(raw_record)
        
        # City name
        if city_name:
            city = city_name.lower()
        else:
            city = raw_record.get("_storage", {}).get("city", "unknown")
        
        # Extract and validate fields (flat structure in hourly)
        temp = WeatherTransformer._validate_field(
            "temperature_celsius", 
            hourly.get("temp"),
            default=None
        )
        feels_like = WeatherTransformer._validate_field(
            "temperature_celsius",
            hourly.get("feels_like"),
            default=temp
        )
        humidity = WeatherTransformer._validate_field(
            "humidity_percent",
            hourly.get("humidity"),
            default=None
        )
        pressure = WeatherTransformer._validate_field(
            "pressure_hpa",
            hourly.get("pressure"),
            default=None
        )
        dew_point = WeatherTransformer._validate_field(
            "dew_point_celsius",
            hourly.get("dew_point"),
            default=None
        )
        wind_speed = WeatherTransformer._validate_field(
            "wind_speed_mps",
            hourly.get("wind_speed"),
            default=None
        )
        wind_gust = WeatherTransformer._validate_field(
            "wind_speed_mps",
            hourly.get("wind_gust"),
            default=None
        )
        wind_deg = WeatherTransformer._validate_field(
            "wind_direction_deg",
            hourly.get("wind_deg"),
            default=None
        )
        uvi = WeatherTransformer._validate_field(
            "uvi",
            hourly.get("uvi"),
            default=None
        )
        clouds = WeatherTransformer._validate_field(
            "cloud_coverage_percent",
            hourly.get("clouds"),
            default=None
        )
        visibility = hourly.get("visibility")
        pop = hourly.get("pop", 0)
        
        # Build transformed record with ALL available fields
        transformed = {
            # Core identification
            "timestamp_utc": timestamp,
            "hour": hour,
            "hour_formatted": f"{hour:02d}:00" if hour is not None else None,
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
            "precipitation_probability_percent": int(pop * 100) if pop is not None else 0,
            
            # UV index
            "uvi": round(uvi, 2) if uvi is not None else None,
            "uvi_category": WeatherTransformer._categorize_uvi(uvi) if uvi is not None else None,
            
            # Derived metrics
            "heat_index_celsius": WeatherTransformer._calculate_heat_index(temp or 0, humidity or 0),
            "weather_severity": WeatherTransformer._classify_weather_severity(
                temp or 0, wind_speed or 0, wind_gust, uvi
            ),
            
            # Location metadata
            "latitude": raw_record.get("lat"),
            "longitude": raw_record.get("lon"),
            "timezone": raw_record.get("timezone"),
            "timezone_offset_seconds": raw_record.get("timezone_offset"),
        }
        
        # Add data quality metrics
        transformed["_data_quality"] = WeatherTransformer._calculate_data_quality(transformed)
        
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
