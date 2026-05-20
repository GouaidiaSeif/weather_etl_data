"""
Improved GOLD layer aggregation pipeline.
Aggregates cleaned SILVER data into analytical datasets with comprehensive KPIs.

FIXED v3:
- Includes hour-based aggregations (hourly breakdowns)
- Adds more comprehensive KPIs
- Handles all new fields from transformers
"""

import statistics
import re
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Any, TYPE_CHECKING

from utils.logger import get_logger
from utils.dedupe import dedupe_records_by_hour
from transformations.transformationscommon_cleaning import daily_trust_flags

if TYPE_CHECKING:
    from storage.mongodb_storage import MongoDBStorage

logger = get_logger(__name__)


class GoldPipeline:
    """Gold layer aggregation with comprehensive KPIs."""

    def __init__(
        self,
        mongodb: "MongoDBStorage",
        aggregation_date_paris: Optional[str] = None,
    ):
        self._mongodb = mongodb
        self._aggregation_date_paris = aggregation_date_paris

    @staticmethod
    def _sanitize_city_name(city: str) -> str:
        """Sanitize city name for use in directory paths."""
        sanitized = city.lower()
        sanitized = re.sub(r'[^a-z0-9]', '_', sanitized)
        sanitized = re.sub(r'_+', '_', sanitized)
        sanitized = sanitized.strip('_')
        return sanitized

    @staticmethod
    def _calculate_trend(values: List[float]) -> str:
        """Calculate trend direction from a series of values."""
        if len(values) < 2:
            return "stable"
        
        first_half = sum(values[:len(values)//2]) / max(len(values)//2, 1)
        second_half = sum(values[len(values)//2:]) / max(len(values) - len(values)//2, 1)
        
        diff = second_half - first_half
        threshold = abs(first_half) * 0.05 if first_half != 0 else 0.5
        
        if diff > threshold:
            return "rising"
        elif diff < -threshold:
            return "falling"
        return "stable"

    @staticmethod
    def _calculate_volatility(values: List[float]) -> Optional[float]:
        """Calculate standard deviation (volatility) of values."""
        if len(values) < 2:
            return None
        try:
            return round(statistics.stdev(values), 2)
        except statistics.StatisticsError:
            return 0.0

    @staticmethod
    def _get_dominant_value(values: List[str]) -> str:
        """Get the most frequent value in a list."""
        if not values:
            return "unknown"
        counts = defaultdict(int)
        for v in values:
            counts[v] += 1
        return max(counts.items(), key=lambda x: x[1])[0]

    @staticmethod
    def _calculate_comfort_index(temp: float, humidity: float) -> Dict[str, Any]:
        """Calculate thermal comfort index."""
        temp_score = 100 - abs(temp - 23) * 5
        humidity_score = 100 - abs(humidity - 50) * 2
        overall = (temp_score * 0.6 + humidity_score * 0.4)
        
        if overall >= 80:
            level = "comfortable"
        elif overall >= 60:
            level = "moderate"
        elif overall >= 40:
            level = "uncomfortable"
        else:
            level = "extreme"
        
        return {
            "score": round(max(0, overall), 1),
            "level": level,
            "temp_comfort": round(max(0, temp_score), 1),
            "humidity_comfort": round(max(0, humidity_score), 1),
        }

    # ---------------------------------------------------
    # WEATHER GOLD - HOURLY AGGREGATION
    # ---------------------------------------------------

    def aggregate_weather_daily(self):
        """Create daily aggregated weather dataset with hourly breakdowns."""
        logger.info("Starting weather daily aggregation...")
        
        weather_data = defaultdict(list)
        
        for record in self._mongodb.iter_silver_weather_records(self._aggregation_date_paris):
            try:
                city = record.get("city", "unknown")
                date = record.get("date_paris") or (
                    record.get("timestamp_utc", "")[:10] if record.get("timestamp_utc") else None
                )
                if date:
                    weather_data[(city, date)].append(record)
            except Exception as e:
                logger.warning(f"Failed to process silver weather record: {e}")
        
        logger.info(f"Found {len(weather_data)} city/date combinations for weather aggregation")
        
        for (city, date), records in weather_data.items():
            try:
                records = dedupe_records_by_hour(records)
                records.sort(key=lambda r: r.get("hour_paris", r.get("hour", 0)))
                
                # Extract value lists
                temps = [r["temperature_celsius"] for r in records if r.get("temperature_celsius") is not None]
                humidities = [r["humidity_percent"] for r in records if r.get("humidity_percent") is not None]
                pressures = [r["pressure_hpa"] for r in records if r.get("pressure_hpa") is not None]
                wind_speeds = [r["wind_speed_mps"] for r in records if r.get("wind_speed_mps") is not None]
                wind_gusts = [r["wind_gust_mps"] for r in records if r.get("wind_gust_mps") is not None]
                uvis = [r["uvi"] for r in records if r.get("uvi") is not None]
                visibilities = [r["visibility_m"] for r in records if r.get("visibility_m") is not None]
                pops = [r["precipitation_probability_percent"] for r in records if r.get("precipitation_probability_percent") is not None]
                dew_points = [r["dew_point_celsius"] for r in records if r.get("dew_point_celsius") is not None]
                
                weather_mains = [r["weather_main"] for r in records if r.get("weather_main")]
                severities = [
                    r["weather_severity"] for r in records if r.get("weather_severity")
                ]
                uvi_categories = [r["uvi_category"] for r in records if r.get("uvi_category")]
                
                # Hourly breakdown
                hourly_breakdown = []
                for r in records:
                    hourly_breakdown.append({
                        "hour": r.get("hour"),
                        "hour_formatted": r.get("hour_formatted"),
                        "temperature": r.get("temperature_celsius"),
                        "humidity": r.get("humidity_percent"),
                        "wind_speed": r.get("wind_speed_mps"),
                        "weather": r.get("weather_main"),
                        "uvi": r.get("uvi"),
                    })
                
                if not temps:
                    logger.warning(f"No temperature data for {city} on {date}")
                    continue

                data_quality_score = round(
                    sum(
                        r.get("_data_quality", {}).get("completeness_score", 0.0)
                        for r in records
                    )
                    / len(records),
                    2,
                )
                trust = daily_trust_flags(len(temps), len(records), data_quality_score)

                # Build gold record
                gold_record = {
                    # Basic info
                    "city": city,
                    "date": date,
                    "records_count": len(records),
                    "hours_covered": [r.get("hour") for r in records if r.get("hour") is not None],
                    
                    # Hourly breakdown
                    "hourly_data": hourly_breakdown,
                    
                    # Temperature KPIs
                    "avg_temperature": round(sum(temps) / len(temps), 2),
                    "min_temperature": min(temps),
                    "max_temperature": max(temps),
                    "temp_range": round(max(temps) - min(temps), 2),
                    "temp_volatility": self._calculate_volatility(temps),
                    "temp_trend": self._calculate_trend(temps),
                    
                    # Feels-like temperature
                    "avg_feels_like": round(sum(r["feels_like_celsius"] for r in records if r.get("feels_like_celsius") is not None) / max(len([r for r in records if r.get("feels_like_celsius") is not None]), 1), 2) if any(r.get("feels_like_celsius") for r in records) else None,
                    
                    # Dew point
                    "avg_dew_point": round(sum(dew_points) / len(dew_points), 2) if dew_points else None,
                    
                    # Humidity KPIs
                    "avg_humidity": round(sum(humidities) / len(humidities), 2) if humidities else None,
                    "min_humidity": min(humidities) if humidities else None,
                    "max_humidity": max(humidities) if humidities else None,
                    "humidity_trend": self._calculate_trend(humidities) if humidities else None,
                    
                    # Pressure KPIs
                    "avg_pressure": round(sum(pressures) / len(pressures), 2) if pressures else None,
                    "pressure_trend": self._calculate_trend(pressures) if pressures else None,
                    
                    # Wind KPIs
                    "avg_wind_speed": round(sum(wind_speeds) / len(wind_speeds), 2) if wind_speeds else None,
                    "max_wind_speed": max(wind_speeds) if wind_speeds else None,
                    "max_wind_gust": max(wind_gusts) if wind_gusts else None,
                    "wind_gust_present": len(wind_gusts) > 0,
                    
                    # Sky and visibility
                    "avg_cloud_coverage": round(sum(r["cloud_coverage_percent"] for r in records if r.get("cloud_coverage_percent") is not None) / max(len([r for r in records if r.get("cloud_coverage_percent") is not None]), 1), 2) if any(r.get("cloud_coverage_percent") for r in records) else None,
                    "avg_visibility": round(sum(visibilities) / len(visibilities), 2) if visibilities else None,
                    "min_visibility": min(visibilities) if visibilities else None,
                    
                    # UV and precipitation
                    "max_uvi": max(uvis) if uvis else None,
                    "avg_uvi": round(sum(uvis) / len(uvis), 2) if uvis else None,
                    "uvi_present": len(uvis) > 0,
                    "uvi_categories": list(set(uvi_categories)) if uvi_categories else [],
                    "max_precipitation_probability": max(pops) if pops else 0,
                    "precipitation_detected": any(
                        (r.get("precipitation_probability_percent") or 0) > 0
                        for r in records
                        if r.get("precipitation_probability_percent") is not None
                    ),
                    
                    # Weather condition analysis
                    "dominant_weather_condition": self._get_dominant_value(weather_mains),
                    "weather_conditions": list(set(weather_mains)),
                    "max_severity": (
                        max(
                            severities,
                            key=lambda s: ["normal", "moderate", "severe", "extreme"].index(s),
                        )
                        if severities
                        else None
                    ),

                    # Comfort index
                    "comfort_index": (
                        self._calculate_comfort_index(
                            sum(temps) / len(temps),
                            sum(humidities) / len(humidities),
                        )
                        if humidities
                        else None
                    ),

                    # Data quality
                    "data_quality_score": data_quality_score,
                    **trust,
                    
                    # Metadata
                    "aggregated_at": datetime.now().isoformat(),
                }
                
                # Extreme weather detection
                gold_record["extreme_weather_flag"] = (
                    gold_record["max_temperature"] >= 35
                    or gold_record["min_temperature"] <= -10
                    or (
                        gold_record["max_wind_speed"] is not None
                        and gold_record["max_wind_speed"] >= 20
                    )
                    or gold_record["max_severity"] in ["severe", "extreme"]
                )
                
                self._save_gold_record("weather_daily", city, date, gold_record)
                
            except Exception as e:
                logger.error(f"Failed to aggregate weather for {city}/{date}: {e}")

        logger.info("Weather daily aggregation completed")

    # ---------------------------------------------------
    # AIR QUALITY GOLD - HOURLY AGGREGATION
    # ---------------------------------------------------

    def aggregate_air_quality_daily(self):
        """Create daily aggregated air quality dataset with hourly breakdowns."""
        logger.info("Starting air quality daily aggregation...")
        
        air_data = defaultdict(list)
        
        for record in self._mongodb.iter_silver_air_quality_records(self._aggregation_date_paris):
            try:
                city = record.get("city", "unknown")
                date = record.get("date_paris") or (
                    record.get("timestamp_utc", "")[:10] if record.get("timestamp_utc") else None
                )
                if date:
                    air_data[(city, date)].append(record)
            except Exception as e:
                logger.warning(f"Failed to process silver air quality record: {e}")
        
        logger.info(f"Found {len(air_data)} city/date combinations for air quality aggregation")
        
        for (city, date), records in air_data.items():
            try:
                records = dedupe_records_by_hour(records)
                records.sort(key=lambda r: r.get("hour_paris", r.get("hour", 0)))
                
                # Extract value lists
                aqis = [r["aqi"] for r in records if r.get("aqi") is not None]
                pm25s = [r["pm25"] for r in records if r.get("pm25") is not None]
                pm10s = [r["pm10"] for r in records if r.get("pm10") is not None]
                no2s = [r["no2"] for r in records if r.get("no2") is not None]
                o3s = [r["o3"] for r in records if r.get("o3") is not None]
                cos = [r["co"] for r in records if r.get("co") is not None]
                so2s = [r["so2"] for r in records if r.get("so2") is not None]
                
                # Weather data from AQICN (when available)
                temps = [r["temperature_celsius"] for r in records if r.get("temperature_celsius") is not None]
                humidities = [r["humidity_percent"] for r in records if r.get("humidity_percent") is not None]
                pressures = [r["pressure_hpa"] for r in records if r.get("pressure_hpa") is not None]
                wind_speeds = [r["wind_speed_mps"] for r in records if r.get("wind_speed_mps") is not None]
                
                alert_levels = [r["alert_level"] for r in records if r.get("alert_level")]
                primary_pollutants = [r["primary_pollutant"] for r in records if r.get("primary_pollutant")]
                
                # Hourly breakdown
                hourly_breakdown = []
                for r in records:
                    hourly_breakdown.append({
                        "hour": r.get("hour"),
                        "hour_formatted": r.get("hour_formatted"),
                        "aqi": r.get("aqi"),
                        "alert_level": r.get("alert_level"),
                        "primary_pollutant": r.get("primary_pollutant"),
                        "pm25": r.get("pm25"),
                        "pm10": r.get("pm10"),
                    })
                
                if not aqis:
                    logger.warning(f"No AQI data for {city} on {date}")
                    continue

                data_quality_score = round(
                    sum(
                        r.get("_data_quality", {}).get("completeness_score", 0.0)
                        for r in records
                    )
                    / len(records),
                    2,
                )
                trust = daily_trust_flags(len(aqis), len(records), data_quality_score)

                unhealthy_hours = sum(1 for aqi in aqis if aqi > 100)
                health_risks = [
                    r["health_risk"]["score"]
                    for r in records
                    if r.get("health_risk") and r["health_risk"].get("score") is not None
                ]
                
                # Collect forecast data (from first record that has it)
                uvi_forecast = None
                for r in records:
                    if r.get("uvi_forecast_daily"):
                        uvi_forecast = r["uvi_forecast_daily"]
                        break
                
                gold_record = {
                    # Basic info
                    "city": city,
                    "date": date,
                    "records_count": len(records),
                    "hours_covered": [r.get("hour") for r in records if r.get("hour") is not None],
                    
                    # Hourly breakdown
                    "hourly_data": hourly_breakdown,
                    
                    # AQI KPIs
                    "avg_aqi": round(sum(aqis) / len(aqis), 2),
                    "min_aqi": min(aqis),
                    "max_aqi": max(aqis),
                    "aqi_volatility": self._calculate_volatility(aqis),
                    "aqi_trend": self._calculate_trend(aqis),
                    
                    # Alert level analysis
                    "max_alert_level": max(alert_levels, key=lambda a: [
                        "good", "moderate", "unhealthy_sensitive", "unhealthy", "very_unhealthy", "hazardous"
                    ].index(a)) if alert_levels else "unknown",
                    "alert_levels_distribution": {level: alert_levels.count(level) for level in set(alert_levels)},
                    "unhealthy_hours_count": unhealthy_hours,
                    "unhealthy_hours_percent": round(unhealthy_hours / len(aqis) * 100, 1) if aqis else 0,
                    
                    # Pollutant averages
                    "avg_pm25": round(sum(pm25s) / len(pm25s), 2) if pm25s else None,
                    "avg_pm10": round(sum(pm10s) / len(pm10s), 2) if pm10s else None,
                    "avg_no2": round(sum(no2s) / len(no2s), 2) if no2s else None,
                    "avg_o3": round(sum(o3s) / len(o3s), 2) if o3s else None,
                    "avg_co": round(sum(cos) / len(cos), 2) if cos else None,
                    "avg_so2": round(sum(so2s) / len(so2s), 2) if so2s else None,
                    
                    # Pollutant peaks
                    "max_pm25": max(pm25s) if pm25s else None,
                    "max_pm10": max(pm10s) if pm10s else None,
                    
                    # Primary pollutant analysis
                    "dominant_primary_pollutant": self._get_dominant_value(primary_pollutants) if primary_pollutants else None,
                    "primary_pollutants": list(set(primary_pollutants)),
                    
                    # Health risk
                    "avg_health_risk_score": round(sum(health_risks) / len(health_risks), 1) if health_risks else None,
                    "max_health_risk_score": max(health_risks) if health_risks else None,
                    
                    # Weather correlation (from AQICN when available)
                    "avg_temperature": round(sum(temps) / len(temps), 2) if temps else None,
                    "avg_humidity": round(sum(humidities) / len(humidities), 2) if humidities else None,
                    "avg_pressure": round(sum(pressures) / len(pressures), 2) if pressures else None,
                    "avg_wind_speed": round(sum(wind_speeds) / len(wind_speeds), 2) if wind_speeds else None,
                    
                    # Forecast data
                    "uvi_forecast_daily": uvi_forecast,
                    
                    # Data quality
                    "data_quality_score": data_quality_score,
                    **trust,

                    # Metadata
                    "aggregated_at": datetime.now().isoformat(),
                }

                # Flag for significant air quality issues
                gold_record["significant_pollution_flag"] = (
                    gold_record["max_aqi"] >= 150 or
                    gold_record["unhealthy_hours_count"] >= 4
                )
                
                self._save_gold_record("air_quality_daily", city, date, gold_record)
                
            except Exception as e:
                logger.error(f"Failed to aggregate air quality for {city}/{date}: {e}")

        logger.info("Air quality daily aggregation completed")

    # ---------------------------------------------------
    # CROSS-DOMAIN ANALYTICS
    # ---------------------------------------------------

    def aggregate_combined_daily(self):
        """Create combined weather + air quality analytics."""
        logger.info("Starting combined daily aggregation...")
        
        weather_gold = self._mongodb.iter_gold_weather_analytics()
        air_gold = self._mongodb.iter_gold_air_quality_analytics()
        
        # Find common city/date combinations
        common_keys = set(weather_gold.keys()) & set(air_gold.keys())
        logger.info(f"Found {len(common_keys)} city/date combinations with both weather and air quality data")
        
        for key in common_keys:
            city, date = key
            weather = weather_gold[key]
            air = air_gold[key]
            
            try:
                avg_aqi = air.get("avg_aqi")
                weather_comfort = (weather.get("comfort_index") or {}).get("score")
                has_weather_comfort = weather_comfort is not None
                has_aqi = avg_aqi is not None

                if has_aqi:
                    air_quality_score = round(max(0, 100 - avg_aqi / 5), 1)
                else:
                    air_quality_score = None

                if has_weather_comfort and has_aqi:
                    outdoor_score = round(
                        weather_comfort * 0.4 + air_quality_score * 0.6, 1
                    )
                    recommendation = (
                        "excellent" if outdoor_score >= 80 else
                        "good" if outdoor_score >= 60 else
                        "moderate" if outdoor_score >= 40 else
                        "poor" if outdoor_score >= 20 else
                        "avoid"
                    )
                else:
                    outdoor_score = None
                    recommendation = "insufficient_data"

                max_aqi = air.get("max_aqi")
                if max_aqi is None:
                    health_advisory = "insufficient_data"
                elif max_aqi <= 50:
                    health_advisory = "No restrictions"
                elif max_aqi <= 100:
                    health_advisory = "Sensitive groups caution"
                elif max_aqi <= 150:
                    health_advisory = "Limit outdoor activities"
                else:
                    health_advisory = "Avoid outdoor activities"

                combined_record = {
                    "city": city,
                    "date": date,
                    
                    # Weather summary
                    "avg_temperature": weather.get("avg_temperature"),
                    "temp_trend": weather.get("temp_trend"),
                    "max_wind_speed": weather.get("max_wind_speed"),
                    "precipitation_detected": weather.get("precipitation_detected"),
                    "max_uvi": weather.get("max_uvi"),
                    
                    # Air quality summary
                    "avg_aqi": air.get("avg_aqi"),
                    "aqi_trend": air.get("aqi_trend"),
                    "max_alert_level": air.get("max_alert_level"),
                    "unhealthy_hours_count": air.get("unhealthy_hours_count"),
                    "primary_pollutant": air.get("dominant_primary_pollutant"),
                    
                    # Combined indices
                    "weather_comfort_score": (
                        round(weather_comfort, 1) if has_weather_comfort else None
                    ),
                    "air_quality_score": air_quality_score,
                    "outdoor_activity_score": outdoor_score,

                    # Hour coverage
                    "weather_hours": weather.get("hours_covered", []),
                    "air_quality_hours": air.get("hours_covered", []),
                    "weather_is_trusted": weather.get("is_trusted", False),
                    "air_quality_is_trusted": air.get("is_trusted", False),

                    # Recommendations
                    "outdoor_activity_recommendation": recommendation,
                    "health_advisory": health_advisory,
                    
                    # Metadata
                    "aggregated_at": datetime.now().isoformat(),
                }
                
                self._save_gold_record("combined_daily", city, date, combined_record)
                
            except Exception as e:
                logger.error(f"Failed to create combined record for {city}/{date}: {e}")
        
        logger.info("Combined daily aggregation completed")

    def _save_gold_record(self, dataset_type: str, city: str, date: str, record: Dict):
        """Save aggregated GOLD record to MongoDB."""
        inserters = {
            "weather_daily": self._mongodb.insert_gold_weather_daily,
            "air_quality_daily": self._mongodb.insert_gold_air_quality_daily,
            "combined_daily": self._mongodb.insert_gold_daily,
        }
        inserter = inserters.get(dataset_type)
        if not inserter:
            logger.error(f"Unknown gold dataset type: {dataset_type}")
            return
        doc_id = inserter(record, city, date)
        logger.info(f"Saved {dataset_type} gold record to MongoDB for {city}/{date}: {doc_id}")

    def run(self):
        """Run all gold aggregations."""
        logger.info("Starting GOLD layer aggregation...")
        self.aggregate_weather_daily()
        self.aggregate_air_quality_daily()
        self.aggregate_combined_daily()
        logger.info("GOLD layer aggregation completed")
