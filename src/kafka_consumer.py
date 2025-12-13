"""
Kafka Consumer Module
Consumes flight data from Kafka topics and processes them
"""
import json
import logging
import signal
import sys
from typing import Dict, List, Optional, Callable
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

sys.path.append('..')
from config.kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlightDataConsumer:
    """Consumer that reads and processes flight data from Kafka"""
    
    def __init__(
        self,
        config: Dict = None,
        topics: List[str] = None,
        group_id: str = None
    ):
        """
        Initialize Kafka Consumer
        
        Args:
            config: Optional custom consumer configuration
            topics: List of topics to subscribe to
            group_id: Consumer group ID
        """
        self.config = config or KafkaConfig.CONSUMER_CONFIG.copy()
        
        # Override group_id if provided
        if group_id:
            self.config['group.id'] = group_id
        
        self.topics = topics or [KafkaConfig.FLIGHTS_RAW_TOPIC]
        self.consumer = Consumer(self.config)
        self.running = False
        
        # Statistics
        self.messages_consumed = 0
        self.messages_processed = 0
        self.messages_failed = 0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Flight Data Consumer initialized")
        logger.info(f"Bootstrap servers: {self.config['bootstrap.servers']}")
        logger.info(f"Consumer group: {self.config['group.id']}")
        logger.info(f"Subscribed topics: {self.topics}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"\n⚠ Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def subscribe(self) -> None:
        """Subscribe to Kafka topics"""
        try:
            self.consumer.subscribe(self.topics)
            logger.info(f"✓ Subscribed to topics: {self.topics}")
        except KafkaException as e:
            logger.error(f"✗ Failed to subscribe to topics: {e}")
            raise
    
    def process_message(self, message: Dict, topic: str) -> Dict:
        """
        Process a single flight message
        Override this method for custom processing logic
        
        Args:
            message: Deserialized message dictionary
            
        Returns:
            Processed message dictionary
        """
        # Example processing: calculate derived fields
        try:
            message['_source_topic'] = topic

            if topic == KafkaConfig.FLIGHTS_RAW_TOPIC:
            # Calculate speed in km/h if velocity is available
                if message['velocity']:
                    message['speed_kmh'] = message['velocity'] * 3.6
                
                # Add processing timestamp
                
                # Classify altitude
                altitude = message.get('baro_altitude')
                if altitude is not None:
                    if altitude < 3000:
                        message['altitude_category'] = 'low'
                    elif altitude < 10000:
                        message['altitude_category'] = 'medium'
                    else:
                        message['altitude_category'] = 'high'
            
            message['processing_timestamp'] = int(datetime.utcnow().timestamp())
            
            return message
            
        except Exception as e:
            logger.error(f"✗ Error processing message: {e}")
            raise
    
    def consume_messages(
        self,
        batch_size: int = 100,
        timeout: float = 1.0,
        callback: Optional[Callable] = None
    ) -> List[Dict]:
        """
        Consume a batch of messages
        
        Args:
            batch_size: Maximum number of messages to consume
            timeout: Timeout in seconds for each poll
            callback: Optional callback function for each message
            
        Returns:
            List of processed messages
        """
        messages = []
        
        try:
            for _ in range(batch_size):
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            f"Reached end of partition {msg.partition()} "
                            f"at offset {msg.offset()}"
                        )
                    else:
                        logger.error(f"✗ Consumer error: {msg.error()}")
                        self.messages_failed += 1
                    continue
                
                try:
                    # Deserialize message
                    key = msg.key().decode('utf-8') if msg.key() else None
                    value = json.loads(msg.value().decode('utf-8'))
                    
                    self.messages_consumed += 1
                    
                    # Log every 100 messages
                    if self.messages_consumed % 100 == 0:
                        logger.info(
                            f"✓ Consumed message from {msg.topic()} "
                            f"[partition {msg.partition()}] at offset {msg.offset()}"
                        )
                    
                    # Process message
                    processed_msg = self.process_message(value, msg.topic())
                    processed_msg['_metadata'] = {
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'key': key,
                        'timestamp': msg.timestamp()[1]
                    }
                    
                    messages.append(processed_msg)
                    self.messages_processed += 1
                    
                    # Execute callback if provided
                    if callback:
                        callback(processed_msg)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"✗ Failed to decode message: {e}")
                    self.messages_failed += 1
                except Exception as e:
                    logger.error(f"✗ Failed to process message: {e}")
                    self.messages_failed += 1
            
            return messages
            
        except Exception as e:
            logger.error(f"✗ Error consuming messages: {e}")
            return messages
    
    def commit_offsets(self, async_commit: bool = False) -> None:
        """
        Commit current offsets
        
        Args:
            async_commit: Whether to commit asynchronously
        """
        try:
            if async_commit:
                self.consumer.commit(asynchronous=True)
            else:
                self.consumer.commit(asynchronous=False)
            
            logger.debug("✓ Offsets committed")
            
        except KafkaException as e:
            logger.error(f"✗ Failed to commit offsets: {e}")
    
    def run(
        self,
        batch_size: int = 100,
        commit_interval: int = 10,
        callback: Optional[Callable] = None
    ) -> None:
        """
        Run the consumer continuously
        
        Args:
            batch_size: Number of messages to consume per batch
            commit_interval: Number of batches before committing offsets
            callback: Optional callback function for each message
        """
        self.subscribe()
        self.running = True
        batches_processed = 0
        
        logger.info("Starting consumer loop...")
        
        try:
            while self.running:
                # Consume batch of messages
                messages = self.consume_messages(
                    batch_size=batch_size,
                    timeout=1.0,
                    callback=callback
                )
                
                if messages:
                    batches_processed += 1
                    
                    # Log statistics
                    logger.info(
                        f"📊 Batch {batches_processed} - "
                        f"Consumed: {self.messages_consumed}, "
                        f"Processed: {self.messages_processed}, "
                        f"Failed: {self.messages_failed}"
                    )
                    
                    # Commit offsets periodically
                    if batches_processed % commit_interval == 0:
                        self.commit_offsets(async_commit=False)
                        logger.info(f"✓ Committed offsets after {batches_processed} batches")
        
        except KeyboardInterrupt:
            logger.info("\n⚠ Consumer interrupted by user")
        except Exception as e:
            logger.error(f"✗ Consumer error: {e}")
            raise
        finally:
            self.close()
    
    def seek_to_beginning(self) -> None:
        """Seek all partitions to the beginning"""
        try:
            partitions = self.consumer.assignment()
            for partition in partitions:
                partition.offset = 0
            self.consumer.assign(partitions)
            logger.info("✓ Seeked to beginning of all partitions")
        except Exception as e:
            logger.error(f"✗ Failed to seek to beginning: {e}")
    
    def seek_to_end(self) -> None:
        """Seek all partitions to the end"""
        try:
            partitions = self.consumer.assignment()
            for partition in partitions:
                low, high = self.consumer.get_watermark_offsets(partition)
                partition.offset = high
            self.consumer.assign(partitions)
            logger.info("✓ Seeked to end of all partitions")
        except Exception as e:
            logger.error(f"✗ Failed to seek to end: {e}")
    
    def get_consumer_position(self) -> List[Dict]:
        """
        Get current consumer position for all assigned partitions
        
        Returns:
            List of partition positions
        """
        try:
            positions = []
            partitions = self.consumer.assignment()
            
            for partition in partitions:
                position = self.consumer.position([partition])[0]
                low, high = self.consumer.get_watermark_offsets(partition)
                
                positions.append({
                    'topic': partition.topic,
                    'partition': partition.partition,
                    'current_offset': position.offset,
                    'low_watermark': low,
                    'high_watermark': high,
                    'lag': high - position.offset
                })
            
            return positions
            
        except Exception as e:
            logger.error(f"✗ Failed to get consumer position: {e}")
            return []
    
    def close(self) -> None:
        """Close the consumer and commit final offsets"""
        logger.info("Closing consumer...")
        
        try:
            # Commit final offsets
            self.commit_offsets(async_commit=False)
            
            # Close consumer
            self.consumer.close()
            
            logger.info(
                f"✓ Consumer closed - Total consumed: {self.messages_consumed}, "
                f"Processed: {self.messages_processed}, Failed: {self.messages_failed}"
            )
            
        except Exception as e:
            logger.error(f"✗ Error closing consumer: {e}")


def message_callback(message: Dict) -> None:
    """Example callback function for processing messages"""
    logger.info(
        f"Callback - Key {message['_metadata'].get('key', 'N/A')}, topic: {message['_metadata']['topic']}, "
    )


def main():
    """Main function to run the consumer"""
    # Create consumer with custom group ID
    topics = [
        # KafkaConfig.FLIGHTS_RAW_TOPIC,
        KafkaConfig.FLIGHTS_DATA_TOPIC,
        KafkaConfig.FLIGHTS_TRACK_TOPIC
    ]

    consumer = FlightDataConsumer(
        group_id='flight-processors-1',
        topics=topics
    )
    
    try:
        # Run consumer with callback
        consumer.run(
            batch_size=50,
            commit_interval=5,
            callback=message_callback
        )
    except KeyboardInterrupt:
        logger.info("\nShutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
