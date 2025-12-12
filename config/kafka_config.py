"""
Kafka Configuration Module
Defines configuration for distributed Kafka brokers and client settings
"""
import os
from typing import Dict, List


class KafkaConfig:
    """Configuration class for Kafka distributed brokers"""
    
    # Distributed Kafka Brokers (modify based on your setup)
    BOOTSTRAP_SERVERS: List[str] = [
        os.getenv('KAFKA_BROKER_1', 'localhost:29092'),
        os.getenv('KAFKA_BROKER_2', 'localhost:29093'),
        os.getenv('KAFKA_BROKER_3', 'localhost:29094'),
    ]
    
    # Topics
    FLIGHTS_RAW_TOPIC = 'flights_raw'
    FLIGHTS_DATA_TOPIC = 'flight_data'
    FLIGHTS_TRACK_TOPIC = 'flight_track'
    CURRENT_WEATHER_TOPIC = 'current_airport_weather'
    FUTURE_WEATHER_TOPIC = 'future_airport_weather'
    WEATHER_WARNING_TOPIC = 'weather_warnings'
    
    # Producer Configuration
    PRODUCER_CONFIG: Dict = {
        'bootstrap.servers': ','.join(BOOTSTRAP_SERVERS),
        'client.id': 'flight-data-producer',
        'acks': 'all',  # Wait for all replicas to acknowledge
        'retries': 3,   # Number of times to retry sending a record on transient errors
        'max.in.flight.requests.per.connection': 5, # # Maximum number of unacknowledged requests per connection
        'compression.type': 'snappy',   # Compress messages with Snappy to reduce network and disk usage
        'linger.ms': 10,  # Batch messages for 10ms for better throughput
        'batch.size': 16384,  # 16KB batch size
        'enable.idempotence': True,  # Exactly-once semantics
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 120000,  # Compress messages with Snappy to reduce network and disk usage
    }
    
    # Consumer Configuration
    CONSUMER_CONFIG: Dict = {
        'bootstrap.servers': ','.join(BOOTSTRAP_SERVERS),
        'group.id': 'flight-data-consumers',
        'client.id': 'flight-data-consumer',
        'auto.offset.reset': 'earliest',  # Start from beginning if no offset
        'enable.auto.commit': False,  # Manual commit for exactly-once
        'max.poll.interval.ms': 300000,  # 5 minutes
        'session.timeout.ms': 45000,
        'heartbeat.interval.ms': 3000,  # Interval (3s) for consumer to send heartbeat to coordinator
        'fetch.min.bytes': 1024,  # Wait for at least 1KB
        'fetch.wait.max.ms': 500,  # Wait max 500ms for data
        'max.partition.fetch.bytes': 1048576,  # 1MB per partition in a single request
    }
    
    # Admin Configuration
    ADMIN_CONFIG: Dict = {
        'bootstrap.servers': ','.join(BOOTSTRAP_SERVERS),
        'client.id': 'flight-data-admin',
        'request.timeout.ms': 30000,
    }
    
    # Topic Configuration
    TOPIC_CONFIG: Dict = {
        'num_partitions': 6,  # Distribute across partitions
        'replication_factor': 3,  # 3 replicas for fault tolerance
        'config': {
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
            'compression.type': 'snappy',
            'min.insync.replicas': '2',  # At least 2 replicas must acknowledge
            'cleanup.policy': 'delete',
        }
    }
    
    # OpenSky Network API Configuration
    OPENSKY_API_URL = 'https://opensky-network.org/api/states/all'
    FETCH_INTERVAL = 5  # Fetch data every 5 seconds
    
    @classmethod
    def get_bootstrap_servers_string(cls) -> str:
        """Get bootstrap servers as comma-separated string"""
        return ','.join(cls.BOOTSTRAP_SERVERS)
    
    @classmethod
    def update_bootstrap_servers(cls, servers: List[str]) -> None:
        """Update bootstrap servers dynamically"""
        cls.BOOTSTRAP_SERVERS = servers
        cls.PRODUCER_CONFIG['bootstrap.servers'] = ','.join(servers)
        cls.CONSUMER_CONFIG['bootstrap.servers'] = ','.join(servers)
        cls.ADMIN_CONFIG['bootstrap.servers'] = ','.join(servers)
