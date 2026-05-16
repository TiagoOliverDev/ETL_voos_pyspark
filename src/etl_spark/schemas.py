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
        StructField("record_timestamp_unix", LongType(), False),
    ]
)


FINAL_FLIGHT_COLUMNS = [
    "aircraft_code",
    "flight_callsign",
    "country_of_origin",
    "latitude",
    "longitude",
    "velocity",
    "heading",
    "barometric_altitude_m",
    "geometric_altitude_m",
    "is_on_ground",
    "position_timestamp",
    "last_contact_timestamp",
    "ingested_at",
]
