-- Weather Data Tables Schema (4 Tables - Purpose-Specific)
-- Supports Weather Observation Map (METAR + SIGMET) and Forecast Timeline (TAF + METAR)
-- Execute this after the flight_analytics keyspace has been created

USE flight_analytics;

-- ============================================================
-- Table 8: metar_latest_by_station
-- ============================================================
-- Supports Task 1: Weather Observation Map (METAR markers)
-- 
-- Purpose: Store ONLY the latest METAR observation per station for map visualization
-- Update Pattern: UPSERT (overwrite with newer observation)
--
-- Spark Responsibilities:
-- ✓ Deduplicate: Use window function to keep only latest obs_time per station
-- ✓ Filter: Remove observations with null lat/lon
-- ✓ Convert: ISO 8601 timestamp → Unix timestamp
-- ✓ Stringify: clouds array → JSON text
--
-- Query Pattern:
-- Q1: Get latest METAR for all stations in viewport
--     SELECT * FROM metar_latest_by_station WHERE station_id IN (list of visible stations)
--
CREATE TABLE IF NOT EXISTS metar_latest_by_station (
    -- Partition Key (one row per station)
    station_id text,              -- ICAO station code (e.g., "KJFK")
    
    -- Observation Metadata
    obs_time bigint,              -- Latest observation time (Unix timestamp in seconds)
    
    -- Location
    lat double,
    lon double,
    site text,                    -- Human-readable site name (e.g., "New York, NY, US")
    
    -- Core Weather Fields (decoded for visualization)
    temp int,                     -- Temperature in °C
    dewp int,                     -- Dewpoint in °C
    
    -- Wind
    wdir int,                     -- Wind direction in degrees
    wspd int,                     -- Wind speed in knots
    wgst int,                     -- Wind gust in knots
    
    -- Visibility & Ceiling
    visib text,                   -- Visibility (e.g., "10" for 10 SM, "6+" for 6+ SM)
    ceil int,                     -- Ceiling height in hundreds of feet AGL
    cover text,                   -- Sky cover code for ceiling (FEW/SCT/BKN/OVC)
    
    -- Flight Category (critical for map color-coding)
    fltcat text,                  -- Flight category: VFR/MVFR/IFR/LIFR
    
    -- Weather Phenomena
    wx text,                      -- Present weather (e.g., "-RA", "TS", "BR")
    
    -- Pressure
    altim int,                    -- Altimeter setting in hPa
    slp int,                      -- Sea level pressure in hPa
    
    -- Cloud Layers (processed by Spark: array → JSON text)
    clouds text,                  -- JSON array: [{"base": 4600, "cover": "BKN"}, ...]
    
    -- Raw METAR (for tooltip/details)
    raw_ob text,                  -- Complete raw METAR string
    
    PRIMARY KEY (station_id)
) WITH comment = 'Latest METAR per station for map view (UPSERT pattern, no TTL)';


-- ============================================================
-- Table 9: metar_history_by_station
-- ============================================================
-- Supports Task 2: Forecast Timeline View (METAR history for TAF comparison)
--
-- Purpose: Store time-series METAR history for comparing observations vs forecast
-- Update Pattern: APPEND (streaming writes from Spark)
--
-- Spark Responsibilities:
-- ✓ Watermark: Handle 10-minute late arrivals
-- ✓ Filter: Remove duplicates (same station_id + obs_time)
-- ✓ Convert: ISO 8601 timestamp → Unix timestamp
-- ✓ Minimal fields: Only store what's needed for timeline visualization
--
-- Query Pattern:
-- Q1: Get METAR observations in time window for timeline
--     SELECT * FROM metar_history_by_station 
--     WHERE station_id = 'KJFK' AND obs_time >= ? AND obs_time <= ?
--
CREATE TABLE IF NOT EXISTS metar_history_by_station (
    -- Partition Key
    station_id text,              -- ICAO station code (e.g., "KJFK")
    
    -- Clustering Column
    obs_time bigint,              -- Observation time (Unix timestamp in seconds)
    
    -- Timeline-Critical Fields Only (minimal for performance)
    fltcat text,                  -- Flight category: VFR/MVFR/IFR/LIFR (for color)
    temp int,                     -- Temperature in °C
    wspd int,                     -- Wind speed in knots
    visib text,                   -- Visibility
    ceil int,                     -- Ceiling in hundreds of feet
    wx text,                      -- Present weather
    raw_ob text,                  -- Raw METAR for tooltip
    
    PRIMARY KEY (station_id, obs_time)
) WITH CLUSTERING ORDER BY (obs_time DESC)
AND default_time_to_live = 259200  -- 3 days retention (72 hours)
AND comment = 'METAR time-series history for timeline comparison (append-only, TTL cleanup)';


-- ============================================================
-- Table 10: sigmet_by_validtime
-- ============================================================
-- Supports Task 1: Weather Observation Map (SIGMET polygons)
--
-- Purpose: Store active SIGMETs with time-based partitioning for efficient slider queries
-- Update Pattern: APPEND (streaming writes from Spark)
--
-- Spark Responsibilities:
-- ✓ Compute hour_bucket: valid_from / 3600 * 3600 (floor to hour)
-- ✓ Convert timestamps: ISO 8601 → Unix timestamp
-- ✓ Stringify polygon: GeoJSON coordinates array → JSON text
-- ✓ Filter: Remove SIGMETs with missing polygon data
-- ✓ Deduplicate: Ensure unique (icao_id, series_id, valid_from) per hour_bucket
--
-- Query Patterns:
-- Q1: Get active SIGMETs at current time (time slider)
--     SELECT * FROM sigmet_by_validtime 
--     WHERE hour_bucket IN (current_hour-3h, current_hour-2h, ..., current_hour)
--     AND valid_from <= now AND valid_to >= now
--
-- Q2: Altitude filtering (application-side after query)
--     Filter by: base <= target_altitude AND top >= target_altitude
--
CREATE TABLE IF NOT EXISTS sigmet_by_validtime (
    -- Partition Key: hour bucket for time-based partitioning
    hour_bucket bigint,           -- valid_from floored to hour (computed by Spark)
    
    -- Clustering Columns: unique identifier for each SIGMET
    icao_id text,                 -- ICAO code of issuing met office
    series_id text,               -- SIGMET series (e.g., "4", "ALPHA1")
    valid_from bigint,            -- SIGMET valid start time (Unix timestamp)
    
    -- Validity Window
    valid_to bigint,              -- SIGMET valid end time (Unix timestamp)
    
    -- FIR Information
    fir_id text,                  -- Flight Information Region ID (e.g., "KZNY")
    fir_name text,                -- FIR full name (e.g., "NEW YORK FIR")
    
    -- Hazard Information (for color-coding and filtering)
    hazard text,                  -- Hazard type: ICE/TURB/TS/VA/MTW/DS/SS/TC
    qualifier text,               -- Severity/frequency: SEV/MOD/FRQ/ISOL/OCNL/EMBD/SQL
    
    -- Vertical Extent (for altitude filtering)
    base int,                     -- Base altitude in feet MSL
    top int,                      -- Top altitude in feet MSL
    
    -- Movement (for arrow visualization)
    dir text,                     -- Direction of movement (cardinal or "-" for stationary)
    spd text,                     -- Speed of movement in knots ("0" for stationary)
    
    -- Change Indicator (for badge)
    chng text,                    -- Change: NC (no change), INTSF (intensifying), WKN (weakening)
    
    -- Polygon (processed by Spark: GeoJSON coordinates → JSON text)
    polygon text,                 -- JSON: "[[[lon1,lat1],[lon2,lat2],...]]"
    
    -- Raw SIGMET (for tooltip/details)
    raw_sigmet text,              -- Complete raw SIGMET text
    
    PRIMARY KEY ((hour_bucket), icao_id, series_id, valid_from)
) WITH CLUSTERING ORDER BY (icao_id ASC, series_id ASC, valid_from DESC)
AND default_time_to_live = 604800  -- 7 days retention (SIGMETs are important safety info)
AND comment = 'SIGMETs partitioned by hour bucket (Spark-computed), supports time-slider queries';


-- ============================================================
-- Table 11: taf_by_station
-- ============================================================
-- Supports Task 2: Forecast Timeline View (TAF forecast periods)
--
-- Purpose: Store TAF forecast periods grouped by bulletin issuance
-- Update Pattern: APPEND (one TAF bulletin → multiple rows, one per period)
--
-- Spark Responsibilities:
-- ✓ Parse TAF bulletins: Split into individual forecast periods (PREVAIL/FM/TEMPO/BECMG)
-- ✓ Assign time_group: Sequentially number periods within bulletin (0, 1, 2, ...)
-- ✓ Convert timestamps: ISO 8601 → Unix timestamp
-- ✓ Stringify clouds: array → JSON text
-- ✓ Deduplicate: Remove duplicate periods (same station_id + issue_time + time_group)
--
-- Query Patterns:
-- Q1: Get latest TAF bulletin with all forecast periods
--     SELECT * FROM taf_by_station 
--     WHERE station_id = 'KJFK' AND issue_time = (latest issue_time)
--     ORDER BY time_group ASC, valid_from ASC
--
-- Q2: Filter periods by time window (timeline zoom)
--     SELECT * FROM taf_by_station
--     WHERE station_id = 'KJFK' AND issue_time = ?
--     AND valid_from >= ? AND valid_to <= ?
--
CREATE TABLE IF NOT EXISTS taf_by_station (
    -- Partition Key (groups all periods from one TAF bulletin)
    station_id text,              -- ICAO station code (e.g., "KJFK")
    issue_time bigint,            -- TAF issuance time (Unix timestamp)
    
    -- Clustering Columns (ordering within bulletin)
    time_group int,               -- Forecast period index (0=PREVAIL, 1+=FM/TEMPO/BECMG)
    valid_from bigint,            -- Forecast period start time (Unix timestamp)
    
    -- Validity Window
    valid_to bigint,              -- Forecast period end time (Unix timestamp)
    
    -- Location
    lat double,
    lon double,
    site text,                    -- Airport name
    
    -- Forecast Type (for timeline step visualization)
    fcst_type text,               -- PREVAIL/FM/TEMPO/BECMG/PROB
    
    -- Forecast Weather Fields
    wdir int,                     -- Forecast wind direction in degrees
    wspd int,                     -- Forecast wind speed in knots
    wgst int,                     -- Forecast wind gust in knots
    
    visib text,                   -- Forecast visibility
    ceil int,                     -- Forecast ceiling in hundreds of feet
    cover text,                   -- Sky cover for ceiling layer
    
    -- Flight Category (for timeline color-coding)
    fltcat text,                  -- Forecast flight category: VFR/MVFR/IFR/LIFR
    
    -- Cloud Layers (processed by Spark: array → JSON text)
    clouds text,                  -- JSON array: [{"base": 4600, "cover": "BKN"}, ...]
    
    -- Raw TAF (complete bulletin for this period)
    raw_taf text,                 -- Complete raw TAF text
    
    PRIMARY KEY ((station_id, issue_time), time_group, valid_from)
) WITH CLUSTERING ORDER BY (time_group ASC, valid_from ASC)
AND default_time_to_live = 259200  -- 3 days retention (TAFs are valid up to 30 hours max)
AND comment = 'TAF forecast periods grouped by bulletin (Spark assigns time_group ordering)';


-- ============================================================
-- Verification Queries
-- ============================================================

-- Verify tables were created
DESCRIBE TABLES;

-- Sample queries to verify schema supports required patterns:

-- Task 1 - Weather Observation Map Queries:
-- Get latest METAR for map markers:
-- SELECT * FROM metar_latest_by_station WHERE station_id IN ('KJFK', 'KLGA', 'KEWR');

-- Get active SIGMETs at current time (using hour buckets):
-- SELECT * FROM sigmet_by_validtime 
-- WHERE hour_bucket IN (current_hour, current_hour-3600, current_hour-7200)
-- AND valid_from <= current_time AND valid_to >= current_time;

-- Task 2 - Forecast Timeline Queries:
-- Get all TAF periods for latest bulletin:
-- SELECT * FROM taf_by_station WHERE station_id = 'KJFK' AND issue_time = (latest);

-- Get METAR history for comparison (6-hour window):
-- SELECT * FROM metar_history_by_station 
-- WHERE station_id = 'KJFK' AND obs_time >= ? AND obs_time <= ?;
