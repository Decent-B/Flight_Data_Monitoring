"""
Kafka Producer Module
Fetches flight data from OpenSky Network API and publishes to Kafka
"""
import json
import logging
import time
import sys
from typing import Dict, Optional, Any
from datetime import datetime
from confluent_kafka import Producer, KafkaError, KafkaException
import requests

sys.path.append('..')
from config.kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlightDataProducer:
    """Producer that fetches flight data and publishes to Kafka"""
    
    def __init__(self, config: Dict = None, topic: str = None):
        """
        Initialize Kafka Producer
        
        Args:
            config: Optional custom producer configuration
            topic: Target topic name
        """
        self.config = config or KafkaConfig.PRODUCER_CONFIG
        self.topic = topic or KafkaConfig.FLIGHTS_RAW_TOPIC
        self.producer = Producer(self.config)
        
        # Statistics
        self.messages_sent = 0
        self.messages_failed = 0
        self.last_fetch_time = None
        
        logger.info(f"Flight Data Producer initialized")
        logger.info(f"Bootstrap servers: {self.config['bootstrap.servers']}")
        logger.info(f"Target topic: {self.topic}")
    
    def delivery_report(self, err: Optional[KafkaError], msg) -> None:
        """
        Callback function for message delivery reports
        
        Args:
            err: Error object if delivery failed
            msg: Message object
        """
        if err is not None:
            self.messages_failed += 1
            logger.error(f"✗ Message delivery failed: {err}")
        else:
            self.messages_sent += 1
            if self.messages_sent % 100 == 0:  # Log every 100 messages
                logger.info(
                    f"✓ Message delivered to {msg.topic()} "
                    f"[partition {msg.partition()}] at offset {msg.offset()}"
                )
    
    def fetch_flight_data(self) -> Optional[Dict]:
        """
        Fetch current flight data from OpenSky Network API
        
        Returns:
            Dictionary containing flight data or None if failed
        """
        try:
            response = requests.get(
                KafkaConfig.OPENSKY_API_URL,
                timeout=5
            )
            response.raise_for_status()
            
            data = response.json()
            self.last_fetch_time = datetime.utcnow()
            
            logger.info(
                f"✓ Fetched flight data: {len(data.get('states', []))} flights at "
                f"{self.last_fetch_time.strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Failed to fetch flight data: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"✗ Failed to parse flight data JSON: {e}")
            return None
    
    def transform_flight_data(self, raw_data: Dict) -> list:
        """
        Transform raw API response into structured flight records
        
        Args:
            raw_data: Raw data from OpenSky API
            
        Returns:
            List of transformed flight records
        """
        flights = []
        states = raw_data.get('states', [])
        fetch_timestamp = raw_data.get('time', int(time.time()))
        
        for state in states:
            try:
                # OpenSky Network state vector format:
                # [0] icao24, [1] callsign, [2] origin_country, [3] time_position,
                # [4] last_contact, [5] longitude, [6] latitude, [7] baro_altitude,
                # [8] on_ground, [9] velocity, [10] true_track, [11] vertical_rate,
                # [12] sensors, [13] geo_altitude, [14] squawk, [15] spi, [16] position_source
                
                flight = {
                    'icao24': state[0],
                    'callsign': state[1].strip() if state[1] else None,
                    'origin_country': state[2],
                    'time_position': state[3],
                    'last_contact': state[4],
                    'longitude': state[5],
                    'latitude': state[6],
                    'baro_altitude': state[7],
                    'on_ground': state[8],
                    'velocity': state[9],
                    'true_track': state[10],
                    'vertical_rate': state[11],
                    'sensors': state[12],
                    'geo_altitude': state[13],
                    'squawk': state[14],
                    'spi': state[15],
                    'position_source': state[16],
                    'fetch_timestamp': fetch_timestamp,
                    'ingestion_timestamp': int(time.time()),
                }
                
                # Filter out flights with invalid coordinates
                if flight['longitude'] is not None and flight['latitude'] is not None:
                    flights.append(flight)
                    
            except (IndexError, TypeError) as e:
                logger.warning(f"⚠ Failed to parse flight state: {e}")
                continue
        
        logger.info(f"✓ Transformed {len(flights)} valid flight records")
        return flights
    
    def produce_message(self, message: Dict, key: Optional[str] = None) -> None:
        """
        Produce a single message to Kafka
        
        Args:
            message: Dictionary message to send
            key: Optional message key for partitioning
        """
        try:
            # Serialize message to JSON
            message_bytes = json.dumps(message).encode('utf-8')
            
            # Use icao24 as key for consistent partitioning
            key_bytes = (key or message.get('icao24', '')).encode('utf-8')
            
            # Produce message asynchronously
            self.producer.produce(
                topic=self.topic,
                key=key_bytes,
                value=message_bytes,
                callback=self.delivery_report
            )
            
            # Trigger delivery report callbacks
            self.producer.poll(0)
            
        except BufferError:
            logger.warning("⚠ Local producer queue is full, waiting...")
            self.producer.flush()
            self.produce_message(message, key)
        except Exception as e:
            logger.error(f"✗ Failed to produce message: {e}")
            self.messages_failed += 1
    
    def produce_batch(self, messages: list) -> int:
        """
        Produce a batch of messages to Kafka
        
        Args:
            messages: List of message dictionaries
            
        Returns:
            Number of messages successfully queued
        """
        queued = 0
        for message in messages:
            self.produce_message(message)
            queued += 1
        
        # Wait for all messages to be delivered, return number of undelivered messages
        remaining = self.producer.flush(timeout=30)
        
        if remaining > 0:
            logger.warning(f"⚠ {remaining} messages were not delivered")
        
        return queued
    
    def run(self, duration: Optional[int] = None) -> None:
        """
        Run the producer continuously
        
        Args:
            duration: Optional duration in seconds (runs indefinitely if None)
        """
        logger.info(f"Starting flight data producer (fetch interval: {KafkaConfig.FETCH_INTERVAL}s)")
        start_time = time.time()
        
        try:
            while True:
                # Check duration limit
                if duration and (time.time() - start_time) >= duration:
                    logger.info(f"✓ Reached duration limit of {duration} seconds")
                    break
                
                # Fetch flight data
                raw_data = self.fetch_flight_data()
                
                if raw_data:
                    # Transform and produce messages
                    flights = self.transform_flight_data(raw_data)
                    self.produce_batch(flights)
                    
                    # Log statistics
                    logger.info(
                        f"📊 Statistics - Sent: {self.messages_sent}, "
                        f"Failed: {self.messages_failed}"
                    )
                
                # Wait before next fetch
                time.sleep(KafkaConfig.FETCH_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("\n⚠ Producer interrupted by user")
        except Exception as e:
            logger.error(f"✗ Producer error: {e}")
            raise
        finally:
            self.close()
    
    def close(self) -> None:
        """Close the producer and flush remaining messages"""
        logger.info("Closing producer...")
        remaining = self.producer.flush(timeout=30)
        
        if remaining > 0:
            logger.warning(f"⚠ {remaining} messages were not delivered before shutdown")
        
        logger.info(
            f"✓ Producer closed - Total sent: {self.messages_sent}, "
            f"Failed: {self.messages_failed}"
        )


def main():
    """Main function to run the producer"""
    producer = FlightDataProducer()
    
    try:
        # Run for 60 seconds (set to None for indefinite)
        producer.run(duration=None)
    except KeyboardInterrupt:
        logger.info("\nShutting down producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
