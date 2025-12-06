-- Flight Analytics Tables Schema
-- Creates all 7 tables for the Flight Data Monitoring system
-- Execute this after the flight_analytics keyspace has been created

USE flight_analytics;

-- Table 1: aircrafts_by_icao24
-- Supports Q2: Per-Aircraft Current State
-- Stores the latest known state for each aircraft
CREATE TABLE IF NOT EXISTS aircrafts_by_icao24 (
    icao24 text,
    callsign text,
    last_seen_ts bigint,
    lat double,
    lon double,
    geo_altitude double,
    velocity double,
    true_track double,
    vertical_rate double,
    on_ground boolean,
    geo_cell text,
    country_code text,
    origin_country text,
    PRIMARY KEY (icao24)
);

-- Table 2: aircraftstates_by_icao24
-- Supports Q3: Per-Aircraft Time Series (Flight Tracks)
-- Stores flight tracks with waypoints partitioned by aircraft and flight start time
CREATE TABLE IF NOT EXISTS aircraftstates_by_icao24 (
    icao24 text,
    starttime bigint,          -- FlightTrack.startTime (identifies the flight)
    time bigint,               -- Waypoint.time

    lat double,                -- Waypoint.latitude
    lon double,                -- Waypoint.longitude
    baro_altitude double,      -- Waypoint.baro_altitude
    true_track double,         -- Waypoint.true_track
    on_ground boolean,         -- Waypoint.on_ground

    callsign text,             -- FlightTrack.callsign
    endtime bigint,            -- FlightTrack.endTime

    PRIMARY KEY ((icao24, starttime), time)
) WITH CLUSTERING ORDER BY (time ASC);

-- Table 3: aircrafts_by_cell_minute
-- Supports Q1: Live Map by Region & Minute
-- Stores aircraft positions by geographic cell and time window
DROP TABLE IF EXISTS aircrafts_by_cell_minute;
CREATE TABLE IF NOT EXISTS aircrafts_by_cell_minute (
    minute_bucket bigint,
    icao24 text,
    last_seen_ts bigint,
    lat double,
    lon double,
    geo_altitude double,
    velocity double,
    true_track double,
    PRIMARY KEY ((minute_bucket), icao24)
);

-- Table 4 removed: trafficdensity_by_cell_minute (no longer needed)
-- If previously created, ensure it is dropped
DROP TABLE IF EXISTS trafficdensity_by_cell_minute;

-- Table 5: activeaircraft_by_country_hour
-- Supports Q5: Hourly Active Aircraft per Country/Global
-- Stores hourly active aircraft counts by country
CREATE TABLE IF NOT EXISTS activeaircraft_by_country_hour (
    country_code text,
    hour_bucket bigint,
    active_aircraft_cnt bigint,
    PRIMARY KEY (country_code, hour_bucket)
) WITH CLUSTERING ORDER BY (hour_bucket ASC);

-- Table 6: departures_by_country_hour
-- Supports Q6: Hourly Departures per Country/Global
-- Stores hourly departure counts by country
CREATE TABLE IF NOT EXISTS departures_by_country_hour (
    country_code text,
    hour_bucket bigint,
    departures_cnt bigint,
    PRIMARY KEY (country_code, hour_bucket)
) WITH CLUSTERING ORDER BY (hour_bucket ASC);

-- Table 7: arrivals_by_country_hour
-- Supports Q7: Hourly Arrivals per Country/Global
-- Stores hourly arrival counts by country
CREATE TABLE IF NOT EXISTS arrivals_by_country_hour (
    country_code text,
    hour_bucket bigint,
    arrivals_cnt bigint,
    PRIMARY KEY (country_code, hour_bucket)
) WITH CLUSTERING ORDER BY (hour_bucket ASC);
