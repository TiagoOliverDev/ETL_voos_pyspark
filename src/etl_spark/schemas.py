from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


RAW_FLIGHT_SCHEMA = StructType(
    [
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position_unix", LongType(), True),
        StructField("last_contact_unix", LongType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("sensors", StringType(), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", LongType(), True),
        StructField("api_snapshot_time_unix", LongType(), True),
        StructField("record_timestamp_unix", LongType(), False),
        StructField("source_file_path", StringType(), False),
    ]
)


SILVER_FLIGHT_COLUMNS = [
    "aircraft_code",
    "flight_callsign",
    "country_of_origin",
    "latitude",
    "longitude",
    "velocity",
    "heading",
    "vertical_rate",
    "barometric_altitude_m",
    "geometric_altitude_m",
    "is_on_ground",
    "position_timestamp",
    "last_contact_timestamp",
    "snapshot_timestamp",
    "ingested_at",
    "position_source",
    "squawk",
    "spi",
    "sensors",
    "source_file_path",
    "snapshot_date",
    "snapshot_hour",
]


GOLD_FLIGHT_POSITION_COLUMNS = [
    "aircraft_code",
    "flight_callsign",
    "country_of_origin",
    "latitude",
    "longitude",
    "velocity",
    "heading",
    "vertical_rate",
    "barometric_altitude_m",
    "geometric_altitude_m",
    "is_on_ground",
    "position_timestamp",
    "last_contact_timestamp",
    "snapshot_timestamp",
    "ingested_at",
    "snapshot_date",
    "snapshot_hour",
]


GOLD_COUNTRY_METRIC_COLUMNS = [
    "snapshot_timestamp",
    "snapshot_date",
    "snapshot_hour",
    "country_of_origin",
    "total_flights",
    "airborne_flights",
    "grounded_flights",
    "avg_velocity",
    "avg_barometric_altitude_m",
    "max_geometric_altitude_m",
]
