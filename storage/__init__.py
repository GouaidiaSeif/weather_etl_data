"""Storage module for the weather ETL pipeline."""

from storage.hive_storage import HivePartitionedStorage

__all__ = ["HivePartitionedStorage"]
