# Data

This document describes the JSON schema and properties for data published to each Kafka topic in the Flight Data Monitoring pipeline and static data in `data` directory.

## OpenSky Network API Kafka Topics

### Topic: `flights_raw`

**Source**: [OpenSky Network StateVector](https://openskynetwork.github.io/opensky-api/python.html#openskyapi.StateVector)

**Description**: Real-time aircraft state vectors containing position, velocity, and metadata.

**Message Key**: `icao24` (string)

**Message Value** (JSON):

```json
{
  "icao24": "string",
  "callsign": "string or null",
  "origin_country": "string",
  "time_position": "integer or null",
  "last_contact": "integer",
  "longitude": "float or null",
  "latitude": "float or null",
  "baro_altitude": "float or null",
  "on_ground": "boolean",
  "velocity": "float or null",
  "true_track": "float or null",
  "vertical_rate": "float or null",
  "sensors": "array of integers or null",
  "geo_altitude": "float or null",
  "squawk": "string or null",
  "spi": "boolean",
  "position_source": "integer",
  "ingestion_timestamp": "string"
}

```

**Property Descriptions**:

- `icao24`: Unique ICAO 24-bit address of the transponder in hex string representation
- `callsign`: Callsign of the vehicle (8 chars). Can be null if no callsign has been received
- `origin_country`: Country name inferred from the ICAO 24-bit address
- `time_position`: Unix timestamp (seconds) for the last position update. Can be null if no position report was received
- `last_contact`: Unix timestamp (seconds) for the last update in general
- `longitude`: WGS-84 longitude in decimal degrees. Can be null
- `latitude`: WGS-84 latitude in decimal degrees. Can be null
- `baro_altitude`: Barometric altitude in meters. Can be null
- `on_ground`: Boolean indicating if the position was retrieved from a surface position report
- `velocity`: Velocity over ground in m/s. Can be null
- `true_track`: True track in decimal degrees clockwise from north (north=0°). Can be null
- `vertical_rate`: Vertical rate in m/s. A positive value indicates that the airplane is climbing, a negative value indicates that it descends. Can be null
- `sensors`: IDs of the receivers which contributed to this state vector. Is null if no filtering for sensor was used in the request
- `geo_altitude`: Geometric altitude in meters. Can be null
- `squawk`: The transponder code aka Squawk. Can be null
- `spi`: Whether flight status indicates special purpose indicator
- `position_source`: Origin of this state's position: 0 = ADS-B, 1 = ASTERIX, 2 = MLAT, 3 = FLARM
- `ingestion_timestamp`

---

### Topic: `flight_data`

**Source**: [OpenSky Network FlightData](https://openskynetwork.github.io/opensky-api/python.html#openskyapi.FlightData)

**Description**: Flight connection data including departure/arrival airports and times.

**Message Key**: `icao24-firstSeen` (string)

**Message Value** (JSON):

```json
{
  "icao24": "string",
  "firstSeen": "integer",
  "estDepartureAirport": "string or null",
  "lastSeen": "integer",
  "estArrivalAirport": "string or null",
  "callsign": "string or null",
  "estDepartureAirportHorizDistance": "integer or null",
  "estDepartureAirportVertDistance": "integer or null",
  "estArrivalAirportHorizDistance": "integer or null",
  "estArrivalAirportVertDistance": "integer or null",
  "departureAirportCandidatesCount": "integer or null",
  "arrivalAirportCandidatesCount": "integer or null"
}

```

**Property Descriptions**:

- `icao24`: Unique ICAO 24-bit address of the transponder in hex string representation
- `firstSeen`: Estimated time of departure for the flight as Unix time (seconds since epoch)
- `estDepartureAirport`: ICAO code of the estimated departure airport. Can be null if the airport could not be identified
- `lastSeen`: Estimated time of arrival for the flight as Unix time (seconds since epoch)
- `estArrivalAirport`: ICAO code of the estimated arrival airport. Can be null if the airport could not be identified
- `callsign`: Callsign of the vehicle. Can be null if no callsign has been received
- `estDepartureAirportHorizDistance`: Horizontal distance of the last received airborne position to the estimated departure airport in meters
- `estDepartureAirportVertDistance`: Vertical distance of the last received airborne position to the estimated departure airport in meters
- `estArrivalAirportHorizDistance`: Horizontal distance of the last received airborne position to the estimated arrival airport in meters
- `estArrivalAirportVertDistance`: Vertical distance of the last received airborne position to the estimated arrival airport in meters
- `departureAirportCandidatesCount`: Number of other possible departure airports. These are airports in short distance to estDepartureAirport
- `arrivalAirportCandidatesCount`: Number of other possible arrival airports. These are airports in short distance to estArrivalAirport

---

### Topic: `flight_track`

**Source**: [OpenSky Network FlightTrack](https://openskynetwork.github.io/opensky-api/python.html#openskyapi.FlightTrack)

**Description**: Complete trajectory/track of a flight with waypoints.

**Message Key**: `icao24-startTime` (string)

**Message Value** (JSON):

```json
{
  "icao24": "string",
  "startTime": "integer",
  "endTime": "integer",
  "callsign": "string or null",
  "path": [
    {
      "time": "integer",
      "latitude": "float or null",
      "longitude": "float or null",
      "baro_altitude": "float or null",
      "true_track": "float or null",
      "on_ground": "boolean"
    }
  ]
}

```

**Property Descriptions**:

- `icao24`: Unique ICAO 24-bit address of the transponder in hex string representation
- `startTime`: Time of the first waypoint in seconds since epoch (Unix time)
- `endTime`: Time of the last waypoint in seconds since epoch (Unix time)
- `callsign`: Callsign of the vehicle. Can be null if no callsign has been received
- `path`: Array of waypoint objects, each containing:
    - `time`: Unix timestamp (seconds) for this waypoint
    - `latitude`: WGS-84 latitude in decimal degrees. Can be null
    - `longitude`: WGS-84 longitude in decimal degrees. Can be null
    - `baro_altitude`: Barometric altitude in meters. Can be null
    - `true_track`: True track in decimal degrees clockwise from north (north=0°). Can be null
    - `on_ground`: Boolean indicating if the aircraft was on ground at this waypoint

---

## Aviation Weather API Kafka Topics

### Topic: `current_airport_weather`

**Source**: [Aviation Weather API - METAR](https://aviationweather.gov/data/api/)

**Description**: Meteorological Aerodrome Reports - surface weather observations.

**Message Key**: `id` (string)

**Message Value** (JSON - Feature object from GeoJSON FeatureCollection):

```json
{
  "type": "Feature",
  "properties": {
    "id": "string",
    "site": "string",
    "obsTime": "string (ISO 8601)",
    "temp": "integer or null",
    "dewp": "integer or null",
    "wdir": "integer or null",
    "wspd": "integer or null",
    "wgst": "integer or null",
    "ceil": "integer or null",
    "cover": "string or null",
    "fltcat": "string",
    "visib": "string or null",
    "wx": "string or null",
    "altim": "integer or null",
    "slp": "integer or null",
    "rawOb": "string",
    "clouds": [
      {
        "base": "integer",
        "cover": "string"
      }
    ]
  },
  "geometry": {
    "type": "Point",
    "coordinates": ["float", "float"]
  }
}

```

**Property Descriptions**:

- `type`: GeoJSON feature type (always "Feature")
- `properties.id`: ICAO station identifier (4-letter airport code)
- `properties.site`: Human-readable site name with location (city, state/province, country)
- `properties.obsTime`: Observation time in ISO 8601 format (e.g., "2025-12-13T02:30:00.000Z")
- `properties.temp`: Temperature in degrees Celsius
- `properties.dewp`: Dewpoint temperature in degrees Celsius
- `properties.wdir`: Wind direction in degrees from true north
- `properties.wspd`: Wind speed in knots
- `properties.wgst`: Wind gust speed in knots
- `properties.ceil`: Ceiling height in hundreds of feet AGL (e.g., 46 = 4,600 feet)
- `properties.cover`: Sky cover code for ceiling layer (e.g., "FEW", "SCT", "BKN", "OVC")
- `properties.fltcat`: Flight category ("VFR", "MVFR", "IFR", "LIFR")
- `properties.visib`: Visibility (e.g., "6+" for 6+ statute miles, or numeric values)
- `properties.wx`: Present weather phenomena (e.g., "-RA", "BR", "TS")
- `properties.altim`: Altimeter setting in hectopascals/millibars (QNH)
- `properties.slp`: Sea level pressure in hectopascals
- `properties.rawOb`: Raw METAR observation string as transmitted
- `properties.clouds`: Array of cloud layer objects, each containing:
    - `base`: Cloud base height in hundreds of feet AGL
    - `cover`: Sky cover code ("FEW", "SCT", "BKN", "OVC", "CLR", "SKC")
- `geometry.type`: GeoJSON geometry type (always "Point")
- `geometry.coordinates`: Array of [longitude, latitude] in decimal degrees

---

### Topic: `future_airport_weather`

**Source**: [Aviation Weather API - TAF](https://aviationweather.gov/data/api/)

**Description**: Terminal Aerodrome Forecasts - aviation weather forecasts.

**Message Key**: `id` (string)

**Message Value** (JSON - Feature object from GeoJSON FeatureCollection):

```json
{
  "type": "Feature",
  "properties": {
    "id": "string",
    "site": "string",
    "issueTime": "string (ISO 8601)",
    "validTimeFrom": "string (ISO 8601)",
    "validTimeTo": "string (ISO 8601)",
    "timeGroup": "integer",
    "fcstType": "string",
    "wdir": "integer or null",
    "wspd": "integer or null",
    "wgst": "integer or null",
    "visib": "string or null",
    "ceil": "integer or null",
    "clouds": [
      {
        "base": "integer",
        "cover": "string"
      }
    ],
    "fltcat": "string",
    "rawTAF": "string",
    "cover": "string or null"
  },
  "geometry": {
    "type": "Point",
    "coordinates": ["float", "float"]
  }
}

```

**Property Descriptions**:

- `type`: GeoJSON feature type (always "Feature")
- `properties.id`: ICAO station identifier (4-letter airport code)
- `properties.site`: Human-readable site name (airport name)
- `properties.issueTime`: TAF issuance time in ISO 8601 format (e.g., "2025-12-13T03:15:00.000Z")
- `properties.validTimeFrom`: Forecast valid start time in ISO 8601 format
- `properties.validTimeTo`: Forecast valid end time in ISO 8601 format
- `properties.timeGroup`: Forecast time group index (0 for base forecast, increments for FM/TEMPO/BECMG groups)
- `properties.fcstType`: Forecast type indicator (e.g., "PREVAIL", "FM", "TEMPO", "BECMG", "PROB")
- `properties.wdir`: Wind direction in degrees from true north
- `properties.wspd`: Wind speed in knots
- `properties.wgst`: Wind gust speed in knots
- `properties.visib`: Forecast visibility (e.g., "6+" for 6+ statute miles, or numeric values)
- `properties.ceil`: Forecast ceiling height in hundreds of feet AGL
- `properties.clouds`: Array of cloud layer objects, each containing:
    - `base`: Cloud base height in hundreds of feet AGL
    - `cover`: Sky cover code ("FEW", "SCT", "BKN", "OVC", "CLR", "SKC")
- `properties.fltcat`: Flight category forecast ("VFR", "MVFR", "IFR", "LIFR")
- `properties.rawTAF`: Complete raw TAF text as transmitted
- `properties.cover`: Sky cover code for the primary cloud layer
- `geometry.type`: GeoJSON geometry type (always "Point")
- `geometry.coordinates`: Array of [longitude, latitude] in decimal degrees

**Note**: The API returns multiple Feature objects for a single TAF, one for each forecast period (base forecast, FM groups, TEMPO groups, etc.). Each period is identified by the `timeGroup` field.

---

### Topic: `weather_warnings`

**Source**: [Aviation Weather API - International SIGMET](https://aviationweather.gov/data/api/)

**Description**: International SIGMETs (Significant Meteorological Information) - warnings of hazardous weather.

**Message Key**: `icaoId` (string)

**Message Value** (JSON - Feature object from GeoJSON FeatureCollection):

```json
{
  "type": "Feature",
  "properties": {
    "icaoId": "string",
    "firId": "string",
    "firName": "string",
    "seriesId": "string",
    "hazard": "string",
    "qualifier": "string or null",
    "validTimeFrom": "string (ISO 8601)",
    "validTimeTo": "string (ISO 8601)",
    "base": "integer or null",
    "top": "integer or null",
    "dir": "string or null",
    "spd": "string or null",
    "chng": "string or null",
    "rawSigmet": "string"
  },
  "geometry": {
    "type": "Polygon",
    "coordinates": "array"
  }
}

```

**Property Descriptions**:

- `type`: GeoJSON feature type (always "Feature")
- `properties.icaoId`: ICAO code of the issuing meteorological office
- `properties.firId`: Flight Information Region identifier (4-letter code)
- `properties.firName`: Full name of the Flight Information Region
- `properties.seriesId`: SIGMET series number (e.g., "4", "ALPHA1", "BRAVO2")
- `properties.hazard`: Type of hazard (e.g., "ICE", "TURB", "TS", "VA", "MTW", "DS", "SS", "TC")
- `properties.qualifier`: Hazard intensity/frequency qualifier (e.g., "SEV", "MOD", "FRQ", "ISOL", "OCNL", "EMBD", "SQL")
- `properties.validTimeFrom`: SIGMET valid start time in ISO 8601 format (e.g., "2025-12-12T23:55:00.000Z")
- `properties.validTimeTo`: SIGMET valid end time in ISO 8601 format
- `properties.base`: Base altitude of phenomenon in feet MSL
- `properties.top`: Top altitude of phenomenon in feet MSL
- `properties.dir`: Direction of movement (cardinal direction or "-" for stationary)
- `properties.spd`: Speed of movement in knots (or "0" for stationary)
- `properties.chng`: Change indicator (e.g., "NC" = no change, "INTSF" = intensifying, "WKN" = weakening)
- `properties.rawSigmet`: Complete raw SIGMET text as transmitted (may contain multiple lines)
- `geometry.type`: GeoJSON geometry type (typically "Polygon", can be "MultiPolygon")
- `geometry.coordinates`: GeoJSON polygon coordinates array defining the affected area as [[[longitude, latitude], ...]]

---

## Static Reference Data (Aviationstack API)

The following static reference datasets are stored in the `data/` folder and sourced from the Aviationstack API. These files provide dimension tables for enriching streaming flight data with airline, airport, and city information.

### File: `airlines_processed.json`

**Source**: Aviationstack API - Airlines endpoint

**Description**: Comprehensive database of global airlines with fleet and operational information.

**Format**: JSON array of airline objects

**Schema**:
```json
{
  "id": "string",
  "fleet_average_age": "string",
  "airline_id": "string",
  "callsign": "string",
  "hub_code": "string",
  "iata_code": "string",
  "icao_code": "string",
  "country_iso2": "string",
  "date_founded": "string",
  "iata_prefix_accounting": "string",
  "airline_name": "string",
  "country_name": "string",
  "fleet_size": "string",
  "status": "string",
  "type": "string"
}
```

**Property Descriptions**:
- `id`: Unique identifier for this record
- `fleet_average_age`: Average age of the airline's fleet in years
- `airline_id`: Unique airline identifier
- `callsign`: Radio callsign used by the airline
- `hub_code`: IATA code of the airline's primary hub airport
- `iata_code`: 2-letter IATA airline code (e.g., "AA", "DL", "UA")
- `icao_code`: 3-letter ICAO airline code (e.g., "AAL", "DAL", "UAL")
- `country_iso2`: ISO 3166-1 alpha-2 country code
- `date_founded`: Year the airline was founded
- `iata_prefix_accounting`: IATA accounting prefix number
- `airline_name`: Full name of the airline
- `country_name`: Country where the airline is registered
- `fleet_size`: Total number of aircraft in the fleet
- `status`: Operational status (e.g., "active", "inactive")
- `type`: Type of airline operation (e.g., "scheduled", "charter", "cargo", "division")

---

### File: `airports_processed.json`

**Source**: Aviationstack API - Airports endpoint

**Description**: Global airport database with geographic and operational details.

**Format**: JSON array of airport objects

**Schema**:
```json
{
  "id": "string",
  "gmt": "string",
  "airport_id": "string",
  "iata_code": "string",
  "city_iata_code": "string",
  "icao_code": "string",
  "country_iso2": "string",
  "geoname_id": "string or null",
  "latitude": "string",
  "longitude": "string",
  "airport_name": "string",
  "country_name": "string",
  "phone_number": "string or null",
  "timezone": "string"
}
```

**Property Descriptions**:
- `id`: Unique identifier for this record
- `gmt`: GMT offset in hours (e.g., "-10", "2", "10")
- `airport_id`: Unique airport identifier
- `iata_code`: 3-letter IATA airport code (e.g., "JFK", "LAX", "LHR")
- `city_iata_code`: IATA code of the city where the airport is located
- `icao_code`: 4-letter ICAO airport code (e.g., "KJFK", "KLAX", "EGLL")
- `country_iso2`: ISO 3166-1 alpha-2 country code
- `geoname_id`: GeoNames database identifier for geographic data
- `latitude`: Airport latitude in decimal degrees
- `longitude`: Airport longitude in decimal degrees
- `airport_name`: Full name of the airport
- `country_name`: Country where the airport is located
- `phone_number`: Contact phone number (can be null)
- `timezone`: IANA timezone identifier (e.g., "America/New_York", "Pacific/Tahiti")

---

### File: `cities_processed.json`

**Source**: Aviationstack API - Cities endpoint

**Description**: Database of cities with aviation-related services and their geographic information.

**Format**: JSON array of city objects

**Schema**:
```json
{
  "id": "string",
  "gmt": "string",
  "city_id": "string",
  "iata_code": "string",
  "country_iso2": "string",
  "geoname_id": "string or null",
  "latitude": "string",
  "longitude": "string",
  "city_name": "string",
  "timezone": "string"
}
```

**Property Descriptions**:
- `id`: Unique identifier for this record
- `gmt`: GMT offset in hours (e.g., "-10", "2", "10")
- `city_id`: Unique city identifier
- `iata_code`: 3-letter IATA city code
- `country_iso2`: ISO 3166-1 alpha-2 country code
- `geoname_id`: GeoNames database identifier (can be null)
- `latitude`: City center latitude in decimal degrees
- `longitude`: City center longitude in decimal degrees
- `city_name`: Full name of the city
- `timezone`: IANA timezone identifier (e.g., "America/New_York", "Africa/Cairo")

---

## Notes

- All timestamps are either Unix epoch time (seconds since January 1, 1970 00:00:00 UTC) or ISO 8601 format strings as indicated
- `null` values indicate missing or unavailable data
- Kafka message keys are used for partitioning and should contain the primary identifier for each record type
- All messages are serialized as JSON strings in UTF-8 encoding
- Aviation Weather API returns GeoJSON FeatureCollections; individual Feature objects from the `features` array are published to Kafka topics
- Static reference data files are used for enrichment joins with streaming data (e.g., joining flight callsigns with airline information, airport codes with airport details)