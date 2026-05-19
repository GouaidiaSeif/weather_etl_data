"""Object storage abstraction for the weather ETL pipeline."""

from __future__ import annotations

import fnmatch
import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.logger import get_logger

logger = get_logger(__name__)


class DataStore(ABC):
    """Read/write JSON objects by logical key (e.g. raw/city=paris/...)."""

    @abstractmethod
    def ensure_ready(self) -> None:
        """Prepare the backend (create bucket/dirs)."""

    @abstractmethod
    def put_json(self, key: str, data: Dict[str, Any], indent: int = 2) -> str:
        """Serialize and store JSON. Returns the object key."""

    @abstractmethod
    def get_json(self, key: str) -> Dict[str, Any]:
        """Load and deserialize JSON."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if the object exists."""

    @abstractmethod
    def list_keys(
        self,
        prefix: str = "",
        pattern: str = "*",
    ) -> List[str]:
        """List object keys under prefix matching a glob pattern."""

    def key_name(self, key: str) -> str:
        """Return the last path segment of a key."""
        return key.replace("\\", "/").rstrip("/").split("/")[-1]

    def key_stem(self, key: str) -> str:
        """Return the filename without extension."""
        name = self.key_name(key)
        if "." in name:
            return name.rsplit(".", 1)[0]
        return name


class LocalDataStore(DataStore):
    """Filesystem-backed object store."""

    def __init__(self, base_path: Path):
        self._base_path = Path(base_path)

    def ensure_ready(self) -> None:
        self._base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalDataStore ready at {self._base_path}")

    def _resolve(self, key: str) -> Path:
        normalized = key.replace("\\", "/").lstrip("/")
        return self._base_path / normalized

    def put_json(self, key: str, data: Dict[str, Any], indent: int = 2) -> str:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
        return key

    def get_json(self, key: str) -> Dict[str, Any]:
        path = self._resolve(key)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def exists(self, key: str) -> bool:
        return self._resolve(key).is_file()

    def list_keys(self, prefix: str = "", pattern: str = "*") -> List[str]:
        search_root = self._resolve(prefix) if prefix else self._base_path
        if not search_root.exists():
            return []

        keys: List[str] = []
        if search_root.is_file():
            rel = search_root.relative_to(self._base_path).as_posix()
            if fnmatch.fnmatch(search_root.name, pattern):
                keys.append(rel)
            return keys

        for path in search_root.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(self._base_path).as_posix()
            if fnmatch.fnmatch(path.name, pattern):
                keys.append(rel)
        return sorted(keys)


class MinioDataStore(DataStore):
    """MinIO / S3-compatible object store."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
    ):
        from minio import Minio

        self._bucket = bucket
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def ensure_ready(self) -> None:
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)
            logger.info(f"Created MinIO bucket: {self._bucket}")
        else:
            logger.info(f"MinIO bucket ready: {self._bucket}")

    def put_json(self, key: str, data: Dict[str, Any], indent: int = 2) -> str:
        from io import BytesIO

        normalized = key.replace("\\", "/").lstrip("/")
        payload = json.dumps(data, indent=indent, ensure_ascii=False, default=str).encode("utf-8")
        self._client.put_object(
            self._bucket,
            normalized,
            BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        return normalized

    def get_json(self, key: str) -> Dict[str, Any]:
        from minio.error import S3Error

        normalized = key.replace("\\", "/").lstrip("/")
        try:
            response = self._client.get_object(self._bucket, normalized)
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                raise FileNotFoundError(normalized) from exc
            raise
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()

    def exists(self, key: str) -> bool:
        from minio.error import S3Error

        normalized = key.replace("\\", "/").lstrip("/")
        try:
            self._client.stat_object(self._bucket, normalized)
            return True
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                return False
            raise

    def list_keys(self, prefix: str = "", pattern: str = "*") -> List[str]:
        normalized_prefix = prefix.replace("\\", "/").lstrip("/")
        keys: List[str] = []
        for obj in self._client.list_objects(
            self._bucket,
            prefix=normalized_prefix,
            recursive=True,
        ):
            name = obj.object_name.split("/")[-1]
            if fnmatch.fnmatch(name, pattern):
                keys.append(obj.object_name)
        return sorted(keys)


def create_raw_store(settings: Any) -> DataStore:
    """Build object storage for the bronze/raw layer only (MinIO or local)."""
    return create_data_store(settings)


def create_data_store(settings: Any) -> DataStore:
    """Build a DataStore from application settings."""
    backend = os.getenv("STORAGE_BACKEND", "local").strip().lower()

    if backend == "minio":
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        bucket = os.getenv("MINIO_BUCKET", "weather-etl")
        secure = os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes")
        store: DataStore = MinioDataStore(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=bucket,
            secure=secure,
        )
    else:
        store = LocalDataStore(settings.data_base_path)

    store.ensure_ready()
    return store
