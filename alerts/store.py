"""Persist alert dedupe keys in MongoDB."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError

from utils.logger import get_logger

if TYPE_CHECKING:
    from storage.mongodb_storage import MongoDBStorage

logger = get_logger(__name__)

COLLECTION = "alert_notifications"


class AlertStore:
    """Record sent alert keys to prevent duplicate Discord messages."""

    def __init__(self, mongodb: "MongoDBStorage"):
        self._mongodb = mongodb

    def ensure_indexes(self) -> None:
        db = self._mongodb._db
        if db is None:
            return
        try:
            db[COLLECTION].create_index([("alert_key", ASCENDING)], unique=True)
        except Exception as e:
            logger.warning("Could not create alert_notifications index: %s", e)

    def was_sent(self, alert_key: str) -> bool:
        db = self._mongodb._db
        if db is None:
            return False
        return db[COLLECTION].find_one({"alert_key": alert_key}) is not None

    def mark_sent(self, alert_key: str, channel: str) -> bool:
        """Return True if newly recorded; False if already sent (duplicate)."""
        db = self._mongodb._db
        if db is None:
            return True

        doc = {
            "alert_key": alert_key,
            "channel": channel,
            "sent_at": datetime.now(timezone.utc),
        }
        try:
            db[COLLECTION].insert_one(doc)
            return True
        except DuplicateKeyError:
            return False
