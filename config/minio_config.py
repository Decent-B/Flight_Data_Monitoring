"""
MinIO Configuration Module
Defines configuration for MinIO S3-compatible object storage
"""
import os
from typing import Dict


class MinioConfig:
    """Configuration class for MinIO distributed object storage"""
    
    # MinIO Connection Settings
    ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    
    # Bucket Names
    FLIGHT_RAW_BUCKET = os.getenv('MINIO_FLIGHT_RAW_BUCKET', 'flight-raw')
    FLIGHT_DATA_BUCKET = os.getenv('MINIO_FLIGHT_DATA_BUCKET', 'flight-data')
    FLIGHT_TRACKS_BUCKET = os.getenv('MINIO_FLIGHT_TRACKS_BUCKET', 'flight-tracks')
    CHECKPOINTS_BUCKET = os.getenv('MINIO_CHECKPOINTS_BUCKET', 'checkpoints')
    
    # S3A Configuration for Spark
    S3A_CONFIG: Dict = {
        'fs.s3a.endpoint': f'http://{ENDPOINT}',
        'fs.s3a.access.key': ACCESS_KEY,
        'fs.s3a.secret.key': SECRET_KEY,
        'fs.s3a.path.style.access': 'true',
        'fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'fs.s3a.connection.ssl.enabled': 'false',
    }
    
    # Archival Settings
    PARTITION_FORMAT = 'year={year}/month={month:02d}/day={day:02d}'
    PARQUET_COMPRESSION = 'snappy'
    ARCHIVAL_TRIGGER_INTERVAL = '10 minutes'  # Micro-batch interval
    
    @classmethod
    def get_bucket_path(cls, bucket_name: str, prefix: str = '') -> str:
        """
        Returns the S3A path for a bucket with optional prefix
        
        Args:
            bucket_name: Name of the MinIO bucket
            prefix: Optional prefix path within the bucket
            
        Returns:
            Full S3A path (e.g., 's3a://bucket-name/prefix')
        """
        if prefix:
            return f's3a://{bucket_name}/{prefix.strip("/")}'
        return f's3a://{bucket_name}'
    
    @classmethod
    def get_partitioned_path(cls, bucket_name: str, year: int, month: int, day: int) -> str:
        """
        Returns date-partitioned S3A path
        
        Args:
            bucket_name: Name of the MinIO bucket
            year: Year value
            month: Month value (1-12)
            day: Day value (1-31)
            
        Returns:
            Partitioned S3A path (e.g., 's3a://bucket/year=2026/month=01/day=07')
        """
        partition = cls.PARTITION_FORMAT.format(year=year, month=month, day=day)
        return f's3a://{bucket_name}/{partition}'
