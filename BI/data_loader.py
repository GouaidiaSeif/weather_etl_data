# data loader
import streamlit as st
import pandas as pd
from pymongo import MongoClient
import json
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config.settings import Settings, get_settings
from storage.mongodb_storage import MongoDBStorage

settings: Optional[Settings] = None
settings = settings or get_settings()
mongodb = MongoDBStorage(settings)

@st.cache_resource
def get_db() :
    '''Connection the mongobase database'''
    uri = mongodb._build_connection_uri()
    client = MongoClient(uri)
    return client["weather_etl"]


def _local_date_to_utc_bounds(local_date_str: str, timezone: str = "Europe/Paris") -> dict:
    """
    Convert local date to UTC bounds (date_str + hour).
    
    Ex: "2026-05-02" Paris (UTC+2) 
        start: ("2026-05-01", 22)
        end:   ("2026-05-02", 22)  
    """
    tz = ZoneInfo(timezone)
    local_start = datetime.fromisoformat(local_date_str).replace(tzinfo=tz)
    local_end   = local_start + timedelta(days=1)

    start_utc = local_start.astimezone(ZoneInfo("UTC"))
    end_utc   = local_end.astimezone(ZoneInfo("UTC"))

    return {
        "start_date": start_utc.strftime("%Y-%m-%d"),
        "start_hour": start_utc.hour,           # inclusif
        "end_date":   end_utc.strftime("%Y-%m-%d"),
        "end_hour":   end_utc.hour,             # exclusif
    }

def _build_hour_filter(bounds: dict) -> dict:
    """
    Build a post-unwind MongoDB filter on hour_utc from UTC bounds.

    Handles two cases:
    - Same UTC date (e.g. UTC+0 timezone): simple single-date filter.
    - Spanning two UTC dates (e.g. UTC+2 Paris): $or filter covering
      the tail of the first UTC date and the head of the second.

    Args:
        bounds: Output of local_date_to_utc_bounds(), containing:
                start_date, start_hour (inclusive),
                end_date,   end_hour   (exclusive).

    Returns:
        A MongoDB filter dict to apply after $unwind and $replaceRoot,
        when hour_utc and date are at the document root.
    """
    same_day = bounds["start_date"] == bounds["end_date"]

    if same_day:
        # Rare (timezone UTC+0)
        return {
            "date": bounds["start_date"],
            "hour_utc": {
                "$gte": bounds["start_hour"],
                "$lt":  bounds["end_hour"]
            }
        }
    else:
        # Standard case (e.g. Paris UTC+2) : local day spans two UTC dates
        return {
            "$or": [
                {
                    "date":     bounds["start_date"],
                    "hour_utc": {"$gte": bounds["start_hour"]}  
                },
                {
                    "date":     bounds["end_date"],
                    "hour_utc": {"$lt": bounds["end_hour"]} 
                }
            ]
        }


def _build_pipeline_agg(date: Optional[str] = None,
                        date_start: Optional[str] = None,
                        date_end: Optional[str] = None,
                        city: Optional[str] = None
                        ) -> list:
    """
    Build the pipeline for daily data. Filter on date and city
    Merge air_quality collecion and weather collection
    """
    
    # Filtre date : date exacte ou plage 
    if date:
        date_filter = date
    elif date_start and date_end:
        date_filter = {"$gte": date_start, "$lte": date_end}
    elif date_start:
        date_filter = {"$gte": date_start}
    elif date_end:
        date_filter = {"$lte": date_end}
    else:
        raise ValueError("Au moins un paramètre de date est requis : date, date_start ou date_end")

    match_filter = {"date": date_filter}
    if city:
        match_filter["city"] = city

    unset_cols = [
    "analytics.hourly_data",
    "analytics.data_quality_score",
    "analytics.aggregated_at",
    "analytics.records_count",
    "analytics.hours_covered",
    "analytics.last_forecast",
    "analytics.uvi_forecast_daily",
    "analytics.alert_levels_distribution",
    "analytics.max_health_risk_score",
    "analytics.weather_conditions",
    "analytics.uvi_categories"
    ]
       
    pip = [
        {"$match": match_filter},
        {"$unset":unset_cols},
        {"$lookup": {
            "from": "gold_weather_daily",
            "let": {"date": "$date", "city": "$city"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$date", "$$date"]},
                    {"$eq": ["$city", "$$city"]}
                ]}}},
                {"$unset": unset_cols}, 
                {"$project": {"_id": 0, "analytics": 1}}
            ],
            "as": "weather"
        }},
        {"$unwind": {"path": "$weather", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {
            "pol_prefixed": {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": "$analytics"},
                        "as": "f",
                        "in": {"k": {"$concat": ["pol_", "$$f.k"]}, "v": "$$f.v"}
                    }
                }
            },
            "w_prefixed": {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": {"$ifNull": ["$weather.analytics", {}]}},
                        "as": "f",
                        "in": {"k": {"$concat": ["w_", "$$f.k"]}, "v": "$$f.v"}
                    }
                }
            }
        }},
        {"$replaceRoot": {
            "newRoot": {
                "$mergeObjects": [
                    {"city": "$city", "date": "$date"},
                    "$pol_prefixed",
                    "$w_prefixed"
                ]
            }
        }}
    ]

    return pip


@st.cache_data(ttl=300)
def load_agg_data(date: Optional[str] = None,date_start: Optional[str] = None
                  ,date_end: Optional[str] = None, city: Optional[str] = None) -> pd.DataFrame:
    """Load aggregation pipeline for gold_air_quality_daily and weather_quality_daily collections
    
    Args : 
        date : selected date for filter in the pipeline. None if date_start and date_end
        date_start : selected start date for filter in the pipeline. None if date
        date_end : selected end date for filter in the pipeline. Noe if date
        city : selected city from filter in the pipeline
        
    Returns : 
        pandas dataframe
    """
    db = get_db()
    pipeline = _build_pipeline_agg(date, date_start, date_end, city)
    return pd.DataFrame(list(db.gold_air_quality_daily.aggregate(pipeline)))

# df = load_agg_data(date="2026-04-15")                                        # date exacte
# df = load_agg_data(date_start="2026-04-01", date_end="2026-04-15")          # plage
# df = load_agg_data(date_start="2026-04-01", date_end="2026-04-15", city="paris")  # plage + ville
# df = load_agg_data(date_start="2026-04-01")                                  # depuis une date


# ajouter le forecast meteo
@st.cache_data(ttl=300)
def load_hourly_collection(collection :str ,forecast: bool =False ,date: Optional[str] = None, date_start :Optional[str] = None
                     , date_end :Optional[str] = None, city: Optional[str] = None, timezone : str = "Europe/Paris") -> pd.DataFrame:
    """
    Aggregation pipeline and load hourly data for the selected collection
    Load and flatten hourly data from a MongoDB collection into a DataFrame.

    Converts local dates to UTC bounds to ensure correct document retrieval
    when documents are indexed by UTC date. The $match stage targets UTC dates
    to leverage existing indexes, while a post-unwind $match filters the exact
    local hours
    
    Args : 
        collection : MongoDB collection name to query
        forecast : If True, gets forecast instead oh hourly data
        date : local date to query
        date start : start of local date range
        date_end :  end of local date range
        city :
        
    Return : 
        dataFrame with one row per hourly record
    """
    db = get_db()
    hour_filter = None  # Post-unwind hour filter; None if no timezone conversion needed

    # unique date 
    if date:
        date_filter = date
    # date range
    elif date_start and date_end:
        date_filter = {"$gte": date_start, "$lte": date_end}
    # load data since date start
    elif date_start:
        date_filter = {"$gte": date_start}
    # load data until date end
    elif date_end:
        date_filter = {"$lte": date_end}
    else:
        raise ValueError("Au moins un paramètre de date est requis : date, date_start ou date_end")

    match_filter = {"date": date_filter}
    if city:
        match_filter["city"] = city

    # Unwind last_forecast for predictive data, hourly_data for observed data
    unwind_field = "$analytics.last_forecast" if forecast else "$analytics.hourly_data"

    pipeline = [
        {"$match": match_filter}, 
        {"$unwind": {"path": unwind_field, "preserveNullAndEmptyArrays": True}},
        {"$replaceRoot": {
            "newRoot": {
                "$mergeObjects": [
                    {"city": "$city", "date": "$date","last_forecast" : "$analytics.last_forecast"},
                    "$analytics.last_forecast" if forecast else "$analytics.hourly_data"
                ]
            }
        }},
    ]

    if hour_filter and forecast == False:
        pipeline.append({"$match": hour_filter})

    return pd.DataFrame(list(db[collection].aggregate(pipeline)))

# @st.cache_data(ttl=300)
def _merge_hourly_data(city : Optional[str], date = None ,d_start =None, d_end =None) -> pd.DataFrame:
    '''
    Load and merge air and weather hourly data if both available. else load only air data or weather datas
    
    Args:
        city:     City to filter on. If None, all cities are returned
        date:     Exact local date to query (YYYY-MM-DD)
        d_start:  Start of local date range
        d_end:    End of local date range
    
    Returns:
        DataFrame with one row per hour, containing merged air quality and
        weather fields. Falls back to a single-source DataFrame if the other
        collection returns no data for the requested period.
    '''
    
    if date is None : 
        df_air   = load_hourly_collection("gold_air_quality_daily",forecast = False, date=None,
                                    date_start=d_start, date_end=d_end, city=city)
        df_meteo = load_hourly_collection("gold_weather_daily",forecast = False, date=None,
                                    date_start=d_start, date_end=d_end, city=city)
    else : 
        df_air   = load_hourly_collection("gold_air_quality_daily",forecast =False, date=date,
                                    date_start=None, date_end=None, city=city)
        df_meteo = load_hourly_collection("gold_weather_daily",forecast =False,  date=date,
                                    date_start=None, date_end=None, city=city)
    
    if not df_air.empty and not df_meteo.empty:
        return pd.merge(df_air, df_meteo,
                        on=["date", "city", "hour", "hour_formatted"],
                        how="outer",suffixes = ("_pol","_w"))
    elif df_air.empty:
        return df_meteo
    return df_air

@st.cache_data(ttl=300)
def load_hourly_data(city : Optional[str], date = None ,d_start =None, d_end =None) -> pd.DataFrame:
    return _merge_hourly_data(city =city, date = date ,d_start =d_start, d_end =d_end)