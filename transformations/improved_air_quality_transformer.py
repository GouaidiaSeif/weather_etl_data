"""
Improved Air quality data transformation and standardization.
Converts raw AQICN API response into analytical format.

FIXED v3:
- Handles short field names in iaqi (h, t, w, p, wg)
- Extracts hour from _storage.hour_timestamp
- Includes forecast data (daily UVI)
- Adds all available pollutant and weather fields
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from utils.logger import get_logger

logger = get_logger(__name__)


class AirQualityTransformer:
    """Transform and clean air quality raw data with comprehensive metrics."""

    # AQI breakpoints by pollutant (EPA standards)
    AQI_BREAKPOINTS = {
        "pm25": [(0, 12, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
                 (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500)],
        "pm10": [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
                 (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500)],
        "o3": [(0, 54, 0, 50), (55, 70, 51, 100), (71, 85, 101, 150),
               (86, 105, 151, 200), (106, 200, 201, 300)],
        "no2": [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
                (361, 649, 151, 200), (650, 1249, 201, 300), (1250, 2049, 301, 500)],
        "so2": [(0, 35, 0, 50), (36, 75, 51, 100), (76, 185, 101, 150),
                (186, 304, 151, 200), (305, 604, 201, 300), (605, 1004, 301, 500)],
        "co": [(0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150),
               (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300), (30.5, 50.4, 301, 500)],
    }

    @staticmethod
    def calculate_alert_level(aqi: int) -> str:
        """Categorize AQI into alert levels."""
        if aqi <= 50:
            return "good"
        elif aqi <= 100:
            return "moderate"
        elif aqi <= 150:
            return "unhealthy_sensitive"
        elif aqi <= 200:
            return "unhealthy"
        elif aqi <= 300:
            return "very_unhealthy"
        return "hazardous"

    @staticmethod
    def calculate_health_risk_score(aqi: int, alert_level: str) -> Dict[str, Any]:
        """Calculate comprehensive health risk score."""
        base_risk = min(aqi / 5, 100)
        
        recommendations = {
            "good": {"outdoor": "safe", "sensitive": "safe", "mask": "not_needed"},
            "moderate": {"outdoor": "safe", "sensitive": "caution", "mask": "optional"},
            "unhealthy_sensitive": {"outdoor": "caution", "sensitive": "avoid", "mask": "recommended"},
            "unhealthy": {"outdoor": "avoid", "sensitive": "avoid", "mask": "required"},
            "very_unhealthy": {"outdoor": "avoid", "sensitive": "avoid", "mask": "required"},
            "hazardous": {"outdoor": "avoid", "sensitive": "avoid", "mask": "required"},
        }
        
        return {
            "score": round(base_risk, 1),
            "level": alert_level,
            "outdoor_activity": recommendations.get(alert_level, {}).get("outdoor", "unknown"),
            "sensitive_groups": recommendations.get(alert_level, {}).get("sensitive", "unknown"),
            "mask_recommended": recommendations.get(alert_level, {}).get("mask") in ["recommended", "required"],
        }

    @staticmethod
    def _extract_hour(raw_record: Dict[str, Any]) -> Optional[int]:
        """Extract hour from _storage.hour_timestamp."""
        storage = raw_record.get("_storage", {})
        hour_ts = storage.get("hour_timestamp", "")
        if hour_ts:
            try:
                dt = datetime.fromisoformat(hour_ts.replace("Z", "+00:00"))
                return dt.hour
            except Exception as e:
                logger.warning(f"Failed to parse hour_timestamp: {e}")
        return None

    @staticmethod
    def _get_primary_pollutant(iaqi: Dict[str, Any]) -> Optional[str]:
        """Determine which pollutant has the highest individual AQI."""
        pollutants = ["pm25", "pm10", "o3", "no2", "so2", "co"]
        max_aqi = 0
        primary = None
        
        for pol in pollutants:
            val = iaqi.get(pol, {}).get("v")
            if val and val > max_aqi:
                max_aqi = val
                primary = pol
        
        return primary

    @staticmethod
    def _calculate_pollutant_ratios(iaqi: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """Calculate useful pollutant ratios."""
        pm25 = iaqi.get("pm25", {}).get("v")
        pm10 = iaqi.get("pm10", {}).get("v")
        no2 = iaqi.get("no2", {}).get("v")
        o3 = iaqi.get("o3", {}).get("v")
        
        ratios = {}
        
        if pm25 and pm10 and pm10 > 0:
            ratios["pm25_to_pm10_ratio"] = round(pm25 / pm10, 2)
        
        if no2 and o3 and o3 > 0:
            ratios["no2_to_o3_ratio"] = round(no2 / o3, 2)
        
        return ratios

    @staticmethod
    def _extract_forecast_uvi(forecast: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract daily UVI forecast."""
        daily = forecast.get("daily", {}).get("uvi", [])
        return [
            {
                "date": item.get("day"),
                "avg": item.get("avg"),
                "min": item.get("min"),
                "max": item.get("max")
            }
            for item in daily
        ]

    @staticmethod
    def transform(raw_record: Dict[str, Any], city_name: Optional[str] = None) -> Dict[str, Any]:
        """Transform AQICN raw response with comprehensive metrics.
        
        FIXED v3: Handles short field names in iaqi (h=humidity, t=temp, etc.)
        
        Args:
            raw_record: The raw JSON record from storage
            city_name: Optional city name override
        
        Returns:
            Dict with cleaned and standardized air quality data
        """
        data = raw_record.get("data", {})
        iaqi = data.get("iaqi", {})
        time_info = data.get("time", {})
        city_info = data.get("city", {})
        forecast = data.get("forecast", {})
        
        # Extract timestamp from API
        timestamp = None
        if "v" in time_info:
            try:
                unix_ts = int(time_info["v"])
                timestamp = datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse AQICN timestamp: {e}")
        
        if timestamp is None and "s" in time_info:
            try:
                time_str = time_info["s"]
                local_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                timestamp = local_time.replace(tzinfo=timezone.utc).isoformat()
            except Exception as e:
                logger.warning(f"Failed to parse AQICN local time: {e}")
        
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()
        
        # Extract hour from _storage
        hour = AirQualityTransformer._extract_hour(raw_record)
        
        # City name
        if city_name:
            city = city_name.lower()
        else:
            city = raw_record.get("_storage", {}).get("city", "unknown")
        
        # AQI and alert level
        aqi = data.get("aqi", 0)
        alert_level = AirQualityTransformer.calculate_alert_level(aqi)
        
        # Extract pollutants (standard names)
        pm25 = iaqi.get("pm25", {}).get("v")
        pm10 = iaqi.get("pm10", {}).get("v")
        no2 = iaqi.get("no2", {}).get("v")
        o3 = iaqi.get("o3", {}).get("v")
        co = iaqi.get("co", {}).get("v")
        so2 = iaqi.get("so2", {}).get("v")
        no = iaqi.get("no", {}).get("v")
        nh3 = iaqi.get("nh3", {}).get("v")
        
        # Extract weather-related fields from iaqi (short names)
        temp = iaqi.get("t", {}).get("v")  # temperature
        humidity = iaqi.get("h", {}).get("v")  # humidity
        pressure = iaqi.get("p", {}).get("v")  # pressure
        wind_speed = iaqi.get("w", {}).get("v")  # wind speed
        wind_gust = iaqi.get("wg", {}).get("v")  # wind gust
        dew_point = iaqi.get("dew", {}).get("v")  # dew point
        
        # Calculate ratios
        ratios = AirQualityTransformer._calculate_pollutant_ratios(iaqi)
        
        # Extract forecast data
        uvi_forecast = AirQualityTransformer._extract_forecast_uvi(forecast)
        
        # Build transformed record
        transformed = {
            # Core identification
            "timestamp_utc": timestamp,
            "hour": hour,
            "hour_formatted": f"{hour:02d}:00" if hour is not None else None,
            "city": city,
            
            # Station info
            "station_id": data.get("idx"),
            "station_name": city_info.get("name"),
            "station_coordinates": {
                "lat": city_info.get("geo", [None, None])[0] if city_info.get("geo") else None,
                "lon": city_info.get("geo", [None, None])[1] if city_info.get("geo") else None,
            },
            "station_url": city_info.get("url"),
            
            # AQI metrics
            "aqi": int(aqi) if aqi else 0,
            "alert_level": alert_level,
            "aqi_category": alert_level.replace("_", " "),
            "dominant_pollutant_api": data.get("dominentpol"),
            
            # Individual pollutants (µg/m³ except CO in mg/m³)
            "pm25": pm25,
            "pm10": pm10,
            "no2": no2,
            "o3": o3,
            "co": co,
            "so2": so2,
            "no": no,
            "nh3": nh3,
            
            # Pollutant analysis
            "primary_pollutant": AirQualityTransformer._get_primary_pollutant(iaqi),
            "pm25_to_pm10_ratio": ratios.get("pm25_to_pm10_ratio"),
            "no2_to_o3_ratio": ratios.get("no2_to_o3_ratio"),
            
            # Weather data from AQICN (when available)
            "temperature_celsius": temp,
            "humidity_percent": humidity,
            "pressure_hpa": pressure,
            "wind_speed_mps": wind_speed,
            "wind_gust_mps": wind_gust,
            "dew_point_celsius": dew_point,
            
            # Health information
            "health_risk": AirQualityTransformer.calculate_health_risk_score(aqi, alert_level),
            
            # Forecast data
            "uvi_forecast_daily": uvi_forecast,
            
            # Station metadata
            "station_timezone": time_info.get("tz"),
            "attributions": [a.get("name") for a in data.get("attributions", []) if a.get("name")],
        }
        
        # Calculate data quality
        numeric_fields = ["pm25", "pm10", "no2", "o3", "co", "so2", "no", "nh3"]
        available_pollutants = sum(1 for p in numeric_fields if transformed[p] is not None)
        transformed["_data_quality"] = {
            "completeness_score": round(available_pollutants / len(numeric_fields), 2),
            "available_pollutants": available_pollutants,
            "total_pollutants": len(numeric_fields),
        }
        
        # Add lineage tracking
        transformed["_lineage"] = {
            "transformer": "AirQualityTransformer",
            "version": "3.0",
            "transformed_at": datetime.now(timezone.utc).isoformat(),
            "raw_source": raw_record.get("_storage", {}).get("filepath", "unknown"),
        }
        
        return transformed
