import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    expr,
    from_unixtime,
    max as spark_max,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
)

from config.settings import (
    GOLD_COUNTRY_METRICS_DATASET,
    GOLD_DIR,
    GOLD_FLIGHT_POSITIONS_DATASET,
    SILVER_DATASET_NAME,
    SILVER_DIR,
)
from src.etl_spark.schemas import (
    GOLD_COUNTRY_METRIC_COLUMNS,
    GOLD_FLIGHT_POSITION_COLUMNS,
    RAW_FLIGHT_SCHEMA,
    SILVER_FLIGHT_COLUMNS,
)
from src.utils.logger import logger


def _to_float(value: Any) -> float | None:
    """
    Converte um valor numerico bruto para `float`.

    Args:
        value: Valor recebido no payload bruto da API.

    Returns:
        Valor convertido para `float` ou `None` quando vazio.
    """
    if value is None:
        return None
    return float(value)


def _state_to_row(
    state: list[Any],
    snapshot_time_unix: int | None,
    record_timestamp_unix: int,
    source_file_path: str,
) -> dict[str, Any]:
    """
    Traduz uma linha posicional da OpenSky para um dicionario nomeado.

    Args:
        state: Lista com os campos de um voo no formato original da OpenSky.
        snapshot_time_unix: Timestamp do snapshot informado pela API.
        record_timestamp_unix: Timestamp da ingestao local do pipeline.
        source_file_path: Caminho do arquivo Bronze que originou a linha.

    Returns:
        Dicionario compativel com o schema bruto da camada Silver.
    """
    return {
        "icao24": state[0] if len(state) > 0 else None,
        "callsign": state[1] if len(state) > 1 else None,
        "origin_country": state[2] if len(state) > 2 else None,
        "time_position_unix": state[3] if len(state) > 3 else None,
        "last_contact_unix": state[4] if len(state) > 4 else None,
        "longitude": _to_float(state[5]) if len(state) > 5 else None,
        "latitude": _to_float(state[6]) if len(state) > 6 else None,
        "baro_altitude": _to_float(state[7]) if len(state) > 7 else None,
        "on_ground": state[8] if len(state) > 8 else None,
        "velocity": _to_float(state[9]) if len(state) > 9 else None,
        "heading": _to_float(state[10]) if len(state) > 10 else None,
        "vertical_rate": _to_float(state[11]) if len(state) > 11 else None,
        "sensors": json.dumps(state[12]) if len(state) > 12 and state[12] is not None else None,
        "geo_altitude": _to_float(state[13]) if len(state) > 13 else None,
        "squawk": state[14] if len(state) > 14 else None,
        "spi": state[15] if len(state) > 15 else None,
        "position_source": state[16] if len(state) > 16 else None,
        "api_snapshot_time_unix": snapshot_time_unix,
        "record_timestamp_unix": record_timestamp_unix,
        "source_file_path": source_file_path,
    }


def load_raw_json(raw_file_path: Path) -> dict[str, Any]:
    """
    Carrega um arquivo JSON bruto da Bronze para memoria.

    Args:
        raw_file_path: Caminho do snapshot bruto salvo em disco.

    Returns:
        Conteudo do JSON convertido para dicionario Python.
    """
    with open(raw_file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def transform_bronze_to_silver(
    spark: SparkSession,
    data: dict[str, Any],
    source_file_path: Path,
    execution_dt: datetime,
) -> DataFrame:
    """
    Normaliza o payload bruto da Bronze em um dataset Silver curado.

    Nesta etapa entram padronizacao de tipos, limpeza de texto, conversao de
    timestamps, validacoes minimas, deduplicacao e enriquecimento tecnico para
    particionamento.

    Args:
        spark: Sessao Spark usada para criar e transformar DataFrames.
        data: Payload bruto do snapshot OpenSky.
        source_file_path: Arquivo Bronze que originou o processamento.
        execution_dt: Momento em que o pipeline esta sendo executado.

    Returns:
        DataFrame Silver pronto para gravacao em Parquet.
    """
    record_timestamp_unix = int(execution_dt.timestamp())
    snapshot_time_unix = data.get("time")

    rows = [
        _state_to_row(
            state=state,
            snapshot_time_unix=snapshot_time_unix,
            record_timestamp_unix=record_timestamp_unix,
            source_file_path=str(source_file_path),
        )
        for state in data.get("states", [])
        if isinstance(state, list)
    ]

    raw_df = spark.createDataFrame(rows, schema=RAW_FLIGHT_SCHEMA)

    silver_df = (
        raw_df
        .withColumn("aircraft_code", trim(col("icao24")))
        .withColumn("flight_callsign", trim(col("callsign")))
        .withColumn("country_of_origin", trim(col("origin_country")))
        .withColumn("position_timestamp", to_timestamp(from_unixtime(col("time_position_unix"))))
        .withColumn("last_contact_timestamp", to_timestamp(from_unixtime(col("last_contact_unix"))))
        .withColumn("snapshot_timestamp", to_timestamp(from_unixtime(col("api_snapshot_time_unix"))))
        .withColumn("ingested_at", to_timestamp(from_unixtime(col("record_timestamp_unix"))))
        .withColumn("barometric_altitude_m", col("baro_altitude"))
        .withColumn("geometric_altitude_m", col("geo_altitude"))
        .withColumn("is_on_ground", col("on_ground"))
        .withColumn("snapshot_date", to_date(col("ingested_at")))
        .withColumn("snapshot_hour", expr("hour(ingested_at)"))
        .where(col("aircraft_code").isNotNull())
        .where(col("aircraft_code") != "")
        .where(col("position_timestamp").isNotNull())
        .where(col("latitude").between(-90.0, 90.0) | col("latitude").isNull())
        .where(col("longitude").between(-180.0, 180.0) | col("longitude").isNull())
        .dropDuplicates(["aircraft_code", "position_timestamp"])
        .select(*SILVER_FLIGHT_COLUMNS)
    )

    logger.info("Camada Silver gerada com sucesso.")
    return silver_df


def save_silver_layer(df: DataFrame) -> Path:
    """
    Persiste o DataFrame Silver no data lake em formato Parquet particionado.

    Args:
        df: DataFrame Silver curado.

    Returns:
        Caminho base do dataset Silver salvo.
    """
    output_path = SILVER_DIR / SILVER_DATASET_NAME
    output_path.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("append")
        .partitionBy("snapshot_date", "snapshot_hour")
        .parquet(str(output_path))
    )

    logger.info(f"Camada Silver salva em: {output_path}")
    return output_path


def build_gold_flight_positions(silver_df: DataFrame) -> DataFrame:
    """
    Cria a Gold detalhada com posicoes de voo prontas para consumo analitico.

    Args:
        silver_df: DataFrame da camada Silver.

    Returns:
        DataFrame Gold detalhado de posicoes de voo.
    """
    gold_df = silver_df.select(*GOLD_FLIGHT_POSITION_COLUMNS)
    logger.info("Gold detalhada de posicoes gerada com sucesso.")
    return gold_df


def build_gold_country_metrics(silver_df: DataFrame) -> DataFrame:
    """
    Gera uma Gold agregada com metricas operacionais por pais e snapshot.

    Args:
        silver_df: DataFrame da camada Silver.

    Returns:
        DataFrame Gold agregado por pais de origem.
    """
    gold_df = (
        silver_df
        .withColumn("snapshot_timestamp", coalesce(col("snapshot_timestamp"), col("ingested_at")))
        .fillna({"country_of_origin": "UNKNOWN"})
        .groupBy("snapshot_timestamp", "snapshot_date", "snapshot_hour", "country_of_origin")
        .agg(
            count("*").alias("total_flights"),
            spark_sum(when_not_grounded()).alias("airborne_flights"),
            spark_sum(when_grounded()).alias("grounded_flights"),
            avg("velocity").alias("avg_velocity"),
            avg("barometric_altitude_m").alias("avg_barometric_altitude_m"),
            spark_max("geometric_altitude_m").alias("max_geometric_altitude_m"),
        )
        .select(*GOLD_COUNTRY_METRIC_COLUMNS)
    )
    logger.info("Gold agregada por pais gerada com sucesso.")
    return gold_df


def when_grounded():
    """
    Monta a expressao Spark usada para contar voos em solo.

    Returns:
        Coluna Spark com a marcacao binaria de voos em solo.
    """
    return expr("CASE WHEN is_on_ground = true THEN 1 ELSE 0 END")


def when_not_grounded():
    """
    Monta a expressao Spark usada para contar voos em voo.

    Returns:
        Coluna Spark com a marcacao binaria de voos em voo.
    """
    return expr("CASE WHEN is_on_ground = false THEN 1 ELSE 0 END")


def save_gold_layer(df: DataFrame, dataset_name: str) -> Path:
    """
    Persiste um dataset Gold em Parquet particionado por data e hora.

    Args:
        df: DataFrame Gold a ser salvo.
        dataset_name: Nome do dataset Gold de destino.

    Returns:
        Caminho base onde o dataset foi gravado.
    """
    output_path = GOLD_DIR / dataset_name
    output_path.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("append")
        .partitionBy("snapshot_date", "snapshot_hour")
        .parquet(str(output_path))
    )

    logger.info(f"Camada Gold salva em: {output_path}")
    return output_path


def transform_file_to_silver(
    spark: SparkSession,
    raw_file_path: Path,
    execution_dt: datetime,
) -> DataFrame:
    """
    Executa o fluxo Bronze -> Silver a partir de um arquivo bruto.

    Args:
        spark: Sessao Spark usada no processamento.
        raw_file_path: Arquivo JSON da Bronze.
        execution_dt: Momento de execucao do pipeline.

    Returns:
        DataFrame Silver curado.
    """
    data = load_raw_json(raw_file_path)
    silver_df = transform_bronze_to_silver(
        spark=spark,
        data=data,
        source_file_path=raw_file_path,
        execution_dt=execution_dt,
    )
    save_silver_layer(silver_df)
    return silver_df


def build_and_save_gold_layers(silver_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Gera e persiste os datasets Gold detalhado e agregado.

    Args:
        silver_df: DataFrame da camada Silver.

    Returns:
        Tupla com o DataFrame Gold detalhado e o DataFrame Gold agregado.
    """
    gold_flight_positions_df = build_gold_flight_positions(silver_df)
    gold_country_metrics_df = build_gold_country_metrics(silver_df)

    save_gold_layer(gold_flight_positions_df, GOLD_FLIGHT_POSITIONS_DATASET)
    save_gold_layer(gold_country_metrics_df, GOLD_COUNTRY_METRICS_DATASET)

    return gold_flight_positions_df, gold_country_metrics_df
