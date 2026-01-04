"""
Kafka Admin Module
Manages Kafka topics creation, deletion, and configuration
"""
import logging
from typing import List, Dict
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from confluent_kafka import KafkaException
import sys

sys.path.append('..')
from config.kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaAdminManager:
    """Manages Kafka administrative operations"""
    
    def __init__(self, config: Dict = None):
        """
        Initialize Kafka Admin Client
        
        Args:
            config: Optional custom configuration dictionary
        """
        self.config = config or KafkaConfig.ADMIN_CONFIG
        self.admin_client = AdminClient(self.config)
        logger.info(f"Kafka Admin Client initialized with brokers: {self.config['bootstrap.servers']}")
    
    def create_topics(self, topics: List[str] = None, topic_config: Dict = None) -> bool:
        """
        Create Kafka topics with replication and partitioning
        
        Args:
            topics: List of topic names to create
            topic_config: Optional topic configuration
            
        Returns:
            bool: True if all topics created successfully
        """
        if topics is None:
            topics = [
                KafkaConfig.FLIGHTS_RAW_TOPIC,
                KafkaConfig.FLIGHTS_PROCESSED_TOPIC,
                KafkaConfig.FLIGHTS_ALERTS_TOPIC,
            ]
        
        if topic_config is None:
            topic_config = KafkaConfig.TOPIC_CONFIG
        
        new_topics = [
            NewTopic(
                topic=topic,
                num_partitions=topic_config['num_partitions'],
                replication_factor=topic_config['replication_factor'],
                config=topic_config.get('config', {})
            )
            for topic in topics
        ]
        
        try:
            # Create topics
            futures = self.admin_client.create_topics(new_topics, request_timeout=30.0)
            
            # Wait for each topic creation to complete
            for topic, future in futures.items():
                try:
                    future.result()  # Block until topic is created
                    logger.info(f"✓ Topic '{topic}' created successfully")
                except KafkaException as e:
                    if e.args[0].code() == 36:  # Topic already exists
                        logger.warning(f"⚠ Topic '{topic}' already exists")
                    else:
                        logger.error(f"✗ Failed to create topic '{topic}': {e}")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error creating topics: {e}")
            return False
    
    def delete_topics(self, topics: List[str]) -> bool:
        """
        Delete Kafka topics
        
        Args:
            topics: List of topic names to delete
            
        Returns:
            bool: True if all topics deleted successfully
        """
        try:
            futures = self.admin_client.delete_topics(topics, request_timeout=30.0)
            
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"✓ Topic '{topic}' deleted successfully")
                except KafkaException as e:
                    logger.error(f"✗ Failed to delete topic '{topic}': {e}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error deleting topics: {e}")
            return False
    
    def list_topics(self, timeout: float = 10.0) -> Dict:
        """
        List all topics in the Kafka cluster
        
        Args:
            timeout: Request timeout in seconds
            
        Returns:
            Dictionary of topics with metadata
        """
        try:
            metadata = self.admin_client.list_topics(timeout=timeout)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Kafka Cluster Topics (Total: {len(metadata.topics)})")
            logger.info(f"{'='*60}")
            
            for topic_name, topic_metadata in metadata.topics.items():
                logger.info(f"\nTopic: {topic_name}")
                logger.info(f"  Partitions: {len(topic_metadata.partitions)}")
                
                for partition_id, partition_metadata in topic_metadata.partitions.items():
                    logger.info(f"    Partition {partition_id}:")
                    logger.info(f"      Leader: {partition_metadata.leader}")
                    logger.info(f"      Replicas: {partition_metadata.replicas}")
                    logger.info(f"      ISR: {partition_metadata.isrs}")
            
            return metadata.topics
            
        except Exception as e:
            logger.error(f"✗ Error listing topics: {e}")
            return {}
    
    def describe_topic(self, topic_name: str) -> Dict:
        """
        Get detailed information about a specific topic
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Dictionary with topic details
        """
        try:
            metadata = self.admin_client.list_topics(topic=topic_name, timeout=10.0)
            
            if topic_name in metadata.topics:
                topic_info = metadata.topics[topic_name]
                
                logger.info(f"\nTopic Details: {topic_name}")
                logger.info(f"{'='*60}")
                logger.info(f"Number of Partitions: {len(topic_info.partitions)}")
                
                for partition_id, partition in topic_info.partitions.items():
                    logger.info(f"\n  Partition {partition_id}:")
                    logger.info(f"    Leader Broker: {partition.leader}")
                    logger.info(f"    Replica Brokers: {partition.replicas}")
                    logger.info(f"    In-Sync Replicas: {partition.isrs}")
                
                return {
                    'name': topic_name,
                    'partitions': len(topic_info.partitions),
                    'partition_details': {
                        pid: {
                            'leader': p.leader,
                            'replicas': p.replicas,
                            'isr': p.isrs
                        }
                        for pid, p in topic_info.partitions.items()
                    }
                }
            else:
                logger.warning(f"⚠ Topic '{topic_name}' not found")
                return {}
                
        except Exception as e:
            logger.error(f"✗ Error describing topic: {e}")
            return {}
    
    def get_cluster_metadata(self) -> Dict:
        """
        Get Kafka cluster metadata including brokers
        
        Returns:
            Dictionary with cluster information
        """
        try:
            metadata = self.admin_client.list_topics(timeout=10.0)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Kafka Cluster Metadata")
            logger.info(f"{'='*60}")
            logger.info(f"Cluster ID: {metadata.cluster_id}")
            logger.info(f"Controller Broker ID: {metadata.controller_id}")
            logger.info(f"\nBrokers in Cluster:")
            
            brokers_info = {}
            for broker_id, broker_metadata in metadata.brokers.items():
                logger.info(f"  Broker {broker_id}: {broker_metadata.host}:{broker_metadata.port}")
                brokers_info[broker_id] = {
                    'host': broker_metadata.host,
                    'port': broker_metadata.port
                }
            
            return {
                'cluster_id': metadata.cluster_id,
                'controller_id': metadata.controller_id,
                'brokers': brokers_info,
                'topics_count': len(metadata.topics)
            }
            
        except Exception as e:
            logger.error(f"✗ Error getting cluster metadata: {e}")
            return {}


def main():
    """Main function to demonstrate admin operations"""
    admin = KafkaAdminManager()
    
    # Get cluster metadata
    print("\n" + "="*60)
    print("STEP 1: Getting Cluster Metadata")
    print("="*60)
    admin.get_cluster_metadata()
    
    # Create topics
    print("\n" + "="*60)
    print("STEP 2: Creating Topics")
    print("="*60)
    admin.create_topics()
    
    # List all topics
    print("\n" + "="*60)
    print("STEP 3: Listing All Topics")
    print("="*60)
    admin.list_topics()
    
    # Describe specific topic
    print("\n" + "="*60)
    print("STEP 4: Describing Specific Topic")
    print("="*60)
    admin.describe_topic(KafkaConfig.FLIGHTS_RAW_TOPIC)


if __name__ == "__main__":
    main()
