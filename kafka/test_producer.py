#!/usr/bin/env python3
"""
Test Kafka Producer Script
Generates sample flight data and sends to Kafka for end-to-end pipeline testing.

This script is used as a replacement for NiFi during testing to verify the
Kafka -> Spark -> Cassandra -> Trino -> Superset data pipeline works correctly.

Usage:
    # First port-forward Kafka:
    kubectl port-forward -n kafka svc/kafka 9092:9092 &
    
    # Then run the producer:
    python kafka/test_producer.py --messages 10
"""

import json
import time
import random
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Optional
from confluent_kafka import Producer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sample data for generating realistic flight data
SAMPLE_ICAO24 = [
    "a1b2c3", "d4e5f6", "789abc", "def012", "345678",
    "abcdef", "123456", "fedcba", "987654", "456789"
]

SAMPLE_CALLSIGNS = [
    "UAL123", "AAL456", "DAL789", "SWA321", "JBU654",
    "VRD987", "ASA432", "FDX876", "UPS543", "SKW210"
]

SAMPLE_COUNTRIES = [
    "United States", "United Kingdom", "Germany", "France", "Canada",
    "Japan", "Australia", "Netherlands", "Singapore", "UAE"
]

SAMPLE_AIRPORTS = [
    "KJFK", "KLAX", "KORD", "KATL", "EGLL",
    "EDDF", "LFPG", "CYYZ", "RJTT", "WSSS"
]


def generate_flight_state() -> Dict:
    """
    Generate a single realistic flight state record.
    
    Returns:
        Dictionary with flight state data matching OpenSky schema
    """
    current_time = int(time.time())
    
    # Generate realistic coordinates (mostly over US/Europe/Pacific)
    lat = random.uniform(25.0, 60.0)
    lon = random.uniform(-130.0, 30.0)
    
    return {
        "icao24": random.choice(SAMPLE_ICAO24),
        "callsign": random.choice(SAMPLE_CALLSIGNS),
        "origin_country": random.choice(SAMPLE_COUNTRIES),
        "time_position": current_time,
        "last_contact": current_time,
        "longitude": round(lon, 6),
        "latitude": round(lat, 6),
        "baro_altitude": round(random.uniform(1000.0, 12000.0), 2),
        "on_ground": random.choice([True, False, False, False]),  # 25% on ground
        "velocity": round(random.uniform(100.0, 300.0), 2),
        "true_track": round(random.uniform(0.0, 360.0), 2),
        "vertical_rate": round(random.uniform(-10.0, 10.0), 2),
        "sensors": None,
        "geo_altitude": round(random.uniform(1000.0, 12000.0), 2),
        "squawk": str(random.randint(1000, 7777)),
        "spi": False,
        "position_source": random.choice([0, 1, 2]),
        "fetch_timestamp": current_time,
        "ingestion_timestamp": current_time
    }


def generate_flight_info() -> Dict:
    """
    Generate flight info record with departure/arrival data.
    
    Returns:
        Dictionary with flight info matching flight_data schema
    """
    current_time = int(time.time())
    first_seen = current_time - random.randint(3600, 14400)  # 1-4 hours ago
    
    return {
        "icao24": random.choice(SAMPLE_ICAO24),
        "firstSeen": first_seen,
        "estDepartureAirport": random.choice(SAMPLE_AIRPORTS),
        "lastSeen": current_time,
        "estArrivalAirport": random.choice(SAMPLE_AIRPORTS),
        "callsign": random.choice(SAMPLE_CALLSIGNS),
        "estDepartureAirportHorizDistance": random.randint(0, 5000),
        "estDepartureAirportVertDistance": random.randint(0, 500),
        "estArrivalAirportHorizDistance": random.randint(0, 50000),
        "estArrivalAirportVertDistance": random.randint(0, 5000),
        "departureAirportCandidatesCount": random.randint(1, 5),
        "arrivalAirportCandidatesCount": random.randint(1, 5)
    }


def generate_flight_track() -> Dict:
    """
    Generate flight track record with path data.
    
    Returns:
        Dictionary with flight track matching flight_track schema
    """
    current_time = int(time.time())
    start_time = current_time - random.randint(1800, 7200)  # 30min - 2hrs ago
    
    # Generate path points
    num_points = random.randint(5, 20)
    base_lat = random.uniform(30.0, 50.0)
    base_lon = random.uniform(-100.0, -70.0)
    
    path = []
    for i in range(num_points):
        point_time = start_time + (i * (current_time - start_time) // num_points)
        lat = base_lat + (i * 0.5)  # Move north
        lon = base_lon + (i * 0.3)  # Move east
        alt = random.uniform(5000, 12000)
        track = random.uniform(0, 360)
        on_ground = i == 0 or i == num_points - 1  # On ground at start/end
        
        path.append([point_time, lat, lon, alt, track, on_ground])
    
    return {
        "icao24": random.choice(SAMPLE_ICAO24),
        "startTime": start_time,
        "endTime": current_time,
        "callsign": random.choice(SAMPLE_CALLSIGNS),
        "path": path
    }


class TestProducer:
    """Test producer for sending sample flight data to Kafka."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the test producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers address
        """
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'test-flight-producer',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 1000,
        }
        self.producer = Producer(self.config)
        self.messages_sent = 0
        self.messages_failed = 0
        
        logger.info(f"Test producer initialized with servers: {bootstrap_servers}")
    
    def delivery_report(self, err: Optional[KafkaError], msg) -> None:
        """Callback for message delivery reports."""
        if err is not None:
            self.messages_failed += 1
            logger.error(f"❌ Message delivery failed: {err}")
        else:
            self.messages_sent += 1
            logger.debug(
                f"✓ Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )
    
    def send_message(self, topic: str, key: str, value: Dict) -> None:
        """
        Send a single message to Kafka.
        
        Args:
            topic: Target Kafka topic
            key: Message key
            value: Message value (will be JSON serialized)
        """
        try:
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self.messages_failed += 1
    
    def flush(self) -> None:
        """Flush all pending messages."""
        self.producer.flush(timeout=30)
    
    def send_flight_states(self, count: int, topic: str = "flights_raw") -> None:
        """
        Send multiple flight state messages.
        
        Args:
            count: Number of messages to send
            topic: Target topic (default: flights_raw)
        """
        logger.info(f"Sending {count} flight state messages to '{topic}'...")
        
        for i in range(count):
            flight = generate_flight_state()
            self.send_message(topic, flight["icao24"], flight)
            
            if (i + 1) % 10 == 0:
                logger.info(f"  Sent {i + 1}/{count} messages...")
                self.producer.poll(0.1)
        
        self.flush()
        logger.info(f"✅ Completed sending {count} flight states. "
                   f"Sent: {self.messages_sent}, Failed: {self.messages_failed}")
    
    def send_flight_info(self, count: int, topic: str = "flight_data") -> None:
        """
        Send multiple flight info messages.
        
        Args:
            count: Number of messages to send
            topic: Target topic (default: flight_data)
        """
        logger.info(f"Sending {count} flight info messages to '{topic}'...")
        
        for i in range(count):
            info = generate_flight_info()
            key = f"{info['icao24']}-{info['firstSeen']}"
            self.send_message(topic, key, info)
            
            if (i + 1) % 10 == 0:
                logger.info(f"  Sent {i + 1}/{count} messages...")
                self.producer.poll(0.1)
        
        self.flush()
        logger.info(f"✅ Completed sending {count} flight info. "
                   f"Sent: {self.messages_sent}, Failed: {self.messages_failed}")
    
    def send_flight_tracks(self, count: int, topic: str = "flight_track") -> None:
        """
        Send multiple flight track messages.
        
        Args:
            count: Number of messages to send
            topic: Target topic (default: flight_track)
        """
        logger.info(f"Sending {count} flight track messages to '{topic}'...")
        
        for i in range(count):
            track = generate_flight_track()
            self.send_message(topic, track["icao24"], track)
            
            if (i + 1) % 10 == 0:
                logger.info(f"  Sent {i + 1}/{count} messages...")
                self.producer.poll(0.1)
        
        self.flush()
        logger.info(f"✅ Completed sending {count} flight tracks. "
                   f"Sent: {self.messages_sent}, Failed: {self.messages_failed}")


def main():
    parser = argparse.ArgumentParser(
        description="Test Kafka Producer for Flight Data Pipeline"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=10,
        help="Number of messages to send per topic (default: 10)"
    )
    parser.add_argument(
        "--topic",
        choices=["all", "flights_raw", "flight_data", "flight_track"],
        default="all",
        help="Which topic(s) to send to (default: all)"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously, sending messages every 5 seconds"
    )
    
    args = parser.parse_args()
    
    producer = TestProducer(bootstrap_servers=args.bootstrap_servers)
    
    try:
        if args.continuous:
            logger.info("Running in continuous mode. Press Ctrl+C to stop.")
            while True:
                if args.topic in ["all", "flights_raw"]:
                    producer.send_flight_states(args.messages)
                if args.topic in ["all", "flight_data"]:
                    producer.send_flight_info(args.messages)
                if args.topic in ["all", "flight_track"]:
                    producer.send_flight_tracks(args.messages)
                
                logger.info(f"Sleeping 5 seconds before next batch...")
                time.sleep(5)
        else:
            if args.topic in ["all", "flights_raw"]:
                producer.send_flight_states(args.messages)
            if args.topic in ["all", "flight_data"]:
                producer.send_flight_info(args.messages)
            if args.topic in ["all", "flight_track"]:
                producer.send_flight_tracks(args.messages)
        
        logger.info(f"\n📊 Final Statistics:")
        logger.info(f"   Total sent: {producer.messages_sent}")
        logger.info(f"   Total failed: {producer.messages_failed}")
        
    except KeyboardInterrupt:
        logger.info("\nShutting down producer...")
        producer.flush()
        logger.info(f"Final stats - Sent: {producer.messages_sent}, Failed: {producer.messages_failed}")


if __name__ == "__main__":
    main()
