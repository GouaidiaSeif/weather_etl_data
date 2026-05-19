"""
Only Silver (cleaned) and Gold (aggregated) layers are stored in MongoDB.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError, ConnectionFailure

from config.settings import Settings, get_settings
from utils.logger import get_logger

logger = get_logger(__name__)


class MongoDBStorage:
    """MongoDB storage handler for weather ETL data.
    
    Stores silver (cleaned) and gold (aggregated) layer data.
    Bronze/Raw layer is NOT stored in MongoDB (filesystem only).
    
    Creates indexes for efficient querying.
    
    Collections:
        - silver_weather: Cleaned weather data
        - silver_air_quality: Cleaned air quality data
        - gold_daily: Aggregated daily analytics
        - gold_weather_daily : Daily weather analytics
        - gold_air_quality_daily : Daily air quality analytics
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        """Initialize MongoDB storage.
        
        Args:
            settings: Application settings with MongoDB credentials
        """
        self._settings = settings or get_settings()
        self._client: Optional[MongoClient] = None
        self._db = None
        
    def connect(self) -> bool:
        """Establish connection to MongoDB.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            uri = self._build_connection_uri()
            
            self._client = MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
            )
            
            self._db = self._client[self._settings.mongodb_database]
            self._db.command("ping")
            
            logger.info(f"Connected to MongoDB: {self._settings.mongodb_database}")
            
            # Create indexes for efficient querying
            self._create_indexes()
            
            return True
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {e}")
            return False
    
    def _build_connection_uri(self) -> str:
        """Build MongoDB connection URI from settings.
        
        Returns:
            str: MongoDB connection URI
        """
        if self._settings.mongodb_uri:
            return self._settings.mongodb_uri

        host = self._settings.mongodb_host
        port = self._settings.mongodb_port
        database = self._settings.mongodb_database
        username = self._settings.mongodb_username
        password = self._settings.mongodb_password
        auth_source = self._settings.mongodb_auth_source or database

        if username and password:
            user = quote_plus(username)
            pwd = quote_plus(password)
            return (
                f"mongodb://{user}:{pwd}@{host}:{port}/{database}"
                f"?authSource={quote_plus(auth_source)}"
            )

        return f"mongodb://{host}:{port}/{database}"

    def _create_indexes(self) -> None:
        """Create indexes for efficient querying."""
        if self._db is None:
            return
            
        try:
            self._db.silver_weather.create_index(
                [("city", ASCENDING), ("date_paris", ASCENDING), ("hour_paris", ASCENDING)],
                unique=True,
            )
            self._db.silver_weather.create_index([("etl_timestamp", ASCENDING)])

            self._db.silver_air_quality.create_index(
                [("city", ASCENDING), ("date_paris", ASCENDING), ("hour_paris", ASCENDING)],
                unique=True,
            )
            self._db.silver_air_quality.create_index([("etl_timestamp", ASCENDING)])
            
            self._db.gold_daily.create_index([("city", ASCENDING), ("date", ASCENDING)], unique=True)
            self._db.gold_daily.create_index([("etl_timestamp", ASCENDING)])
            
            self._db.gold_weather_daily.create_index([("city", ASCENDING), ("date", ASCENDING)], unique=True)
            self._db.gold_weather_daily.create_index([("etl_timestamp", ASCENDING)])
            
            self._db.gold_air_quality_daily.create_index([("city", ASCENDING), ("date", ASCENDING)], unique=True)
            self._db.gold_air_quality_daily.create_index([("etl_timestamp", ASCENDING)])
            
            logger.debug("MongoDB indexes created successfully")
            
        except PyMongoError as e:
            logger.warning(f"Failed to create some indexes: {e}")
    
    def close(self) -> None:
        """Close MongoDB connection."""
        if self._client is not None:
            self._client.close()
            logger.debug("MongoDB connection closed")
    
    def insert_silver_weather(self, data: Dict[str, Any], city: str) -> Optional[str]:
        """Insert cleaned (silver) weather data.
        
        Args:
            data: Cleaned weather data
            city: City name
            
        Returns:
            Optional[str]: Inserted document ID or None if failed
        """
        if self._db is None:
            logger.warning("MongoDB not connected - skipping silver weather insert")
            return None
        
        try:
            document = {
                "city": city.lower(),
                "timestamp_utc": data.get("timestamp_utc"),
                "timestamp_paris": data.get("timestamp_paris"),
                "date_paris": data.get("date_paris"),
                "hour": data.get("hour_paris", data.get("hour")),
                "hour_paris": data.get("hour_paris", data.get("hour")),
                "cleaned_data": data,
                "datetime": data.get("datetime"),
                "etl_timestamp": datetime.now(timezone.utc),
            }

            result = self._db.silver_weather.replace_one(
                {
                    "city": city.lower(),
                    "date_paris": document["date_paris"],
                    "hour_paris": document["hour_paris"],
                },
                document,
                upsert=True,
            )
            doc_id = result.upserted_id or "updated"
            logger.debug(f"Upserted silver weather for {city}: {doc_id}")
            return str(doc_id)
            
        except PyMongoError as e:
            logger.error(f"Failed to insert silver weather for {city}: {e}")
            return None
    
    def insert_silver_air_quality(self, data: Dict[str, Any], city: str) -> Optional[str]:
        """Insert cleaned (silver) air quality data.
        
        Args:
            data: Cleaned air quality data
            city: City name
            
        Returns:
            Optional[str]: Inserted document ID or None if failed
        """
        if self._db is None:
            logger.warning("MongoDB not connected - skipping silver air quality insert")
            return None
        
        try:
            document = {
                "city": city.lower(),
                "timestamp_utc": data.get("timestamp_utc"),
                "timestamp_paris": data.get("timestamp_paris"),
                "date_paris": data.get("date_paris"),
                "hour": data.get("hour_paris", data.get("hour")),
                "hour_paris": data.get("hour_paris", data.get("hour")),
                "cleaned_data": data,
                "datetime": data.get("datetime"),
                "etl_timestamp": datetime.now(timezone.utc),
            }

            result = self._db.silver_air_quality.replace_one(
                {
                    "city": city.lower(),
                    "date_paris": document["date_paris"],
                    "hour_paris": document["hour_paris"],
                },
                document,
                upsert=True,
            )
            doc_id = result.upserted_id or "updated"
            logger.debug(f"Upserted silver air quality for {city}: {doc_id}")
            return str(doc_id)
            
        except PyMongoError as e:
            logger.error(f"Failed to insert silver air quality for {city}: {e}")
            return None
    
    # Modified to create 3 collections : gold_weather_daily, gold_air_quality_daily, gold_daily. Add collection argument
    # insert_gold_daily -> insert_gold
    def _insert_gold(self,collection_name : str, data: Dict[str, Any], city: str, date: str) -> Optional[str]:
        """Insert aggregated (gold) daily data.
        
        Args:
            collection_name : Collection name
            data: Aggregated daily analytics
            city: City name
            date: Date string (YYYY-MM-DD)
            
        Returns:
            Optional[str]: Inserted document ID or None if failed
        """
        if self._db is None:
            logger.warning("MongoDB not connected - skipping gold daily insert")
            return None
        
        try:
            document = {
                "city": city.lower(),
                "date": date,
                "analytics": data,
                "etl_timestamp": datetime.now(timezone.utc),
            }
            
            # Use upsert to avoid duplicates
            result = self._db[collection_name].replace_one(
                {"city": city.lower(), "date": date},
                document,
                upsert=True
            )
            
            doc_id = result.upserted_id or "updated"
            logger.debug(f"Inserted/Updated {collection_name} for {city} on {date}: {doc_id}")
            return str(doc_id)
            
        except PyMongoError as e:
            logger.error(f"Failed to insert {collection_name} for {city}: {e}")
            return None
    
    # Add 3 functions to insert in each gold collections
    def insert_gold_weather_daily(self, data: Dict[str, Any], city: str, date: str) -> Optional[str]:
        return self._insert_gold("gold_weather_daily", data, city, date)

    def insert_gold_air_quality_daily(self, data: Dict[str, Any], city: str, date: str) -> Optional[str]:
        return self._insert_gold("gold_air_quality_daily", data, city, date)

    def insert_gold_daily(self, data: Dict[str, Any], city: str, date: str) -> Optional[str]:
        return self._insert_gold("gold_daily", data, city, date)

    def iter_silver_weather_records(
        self, date_paris: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Return cleaned weather records from the silver collection."""
        if self._db is None:
            return []
        query = {"date_paris": date_paris} if date_paris else {}
        return [doc["cleaned_data"] for doc in self._db.silver_weather.find(query)]

    def iter_silver_air_quality_records(
        self, date_paris: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Return cleaned air quality records from the silver collection."""
        if self._db is None:
            return []
        query = {"date_paris": date_paris} if date_paris else {}
        return [doc["cleaned_data"] for doc in self._db.silver_air_quality.find(query)]

    def iter_gold_weather_analytics(self) -> Dict[tuple, Dict[str, Any]]:
        """Return weather gold analytics keyed by (city, date)."""
        if self._db is None:
            return {}
        records: Dict[tuple, Dict[str, Any]] = {}
        for doc in self._db.gold_weather_daily.find():
            analytics = doc.get("analytics", {})
            key = (analytics.get("city", doc.get("city")), doc.get("date"))
            records[key] = analytics
        return records

    def iter_gold_air_quality_analytics(self) -> Dict[tuple, Dict[str, Any]]:
        """Return air quality gold analytics keyed by (city, date)."""
        if self._db is None:
            return {}
        records: Dict[tuple, Dict[str, Any]] = {}
        for doc in self._db.gold_air_quality_daily.find():
            analytics = doc.get("analytics", {})
            key = (analytics.get("city", doc.get("city")), doc.get("date"))
            records[key] = analytics
        return records

    def get_stats(self) -> Dict[str, int]:
        """Get collection statistics.
        
        Returns:
            Dict[str, int]: Document counts per collection
        """
        if self._db is None:
            return {}
        
        try:
            return {
                "silver_weather": self._db.silver_weather.count_documents({}),
                "silver_air_quality": self._db.silver_air_quality.count_documents({}),
                "gold_weather_daily": self._db.gold_weather_daily.count_documents({}),
                "gold_air_quality_daily": self._db.gold_air_quality_daily.count_documents({}),
                "gold_daily": self._db.gold_daily.count_documents({}),
            }
        except PyMongoError as e:
            logger.error(f"Failed to get stats: {e}")
            return {}
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()