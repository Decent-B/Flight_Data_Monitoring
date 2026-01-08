-- Flight Analytics Keyspace Initialization Script
-- This script creates the keyspace for the Flight Data Monitoring project
-- with production-ready replication settings for a 3-node cluster

-- Create flight_analytics keyspace with NetworkTopologyStrategy
-- Using NetworkTopologyStrategy for production-style configuration
-- dc1:3 means 3 replicas in datacenter 'dc1' (matches our cluster setup)
CREATE KEYSPACE IF NOT EXISTS flight_analytics
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'dc1': 3
}
AND durable_writes = true;

-- Verify keyspace creation
DESCRIBE KEYSPACE flight_analytics;

-- Display cluster information
SELECT cluster_name, data_center, rack 
FROM system.local;
