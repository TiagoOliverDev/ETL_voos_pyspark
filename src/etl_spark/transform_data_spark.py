import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, trim

from config.settings import NATAL_TZ
from src.etl_spark.schemas import FINAL_FLIGHT_COLUMNS, RAW_FLIGHT_SCHEMA
from src.utils.logger import logger


def _to_float(value: Any) -> float | None:
    """
    Converte valores numericos para `float` antes da criacao do DataFrame Spark.

    A API OpenSky pode retornar alguns campos ora como `int`, ora como `float`.
    Como o schema Spark usa `DoubleType`, esta normalizacao evita erros de tipo
    durante `spark.createDataFrame`.

    Args:
        value: Valor bruto vindo da API.

    Returns:
        O valor convertido para `float` ou `None` quando nao houver valor.
    """
    if value is None:
        return None
    return float(value)


def _state_to_row(state: list[Any], record_timestamp_unix: int) -> dict[str, Any]:
    """
    Converte uma linha bruta do array `states` da OpenSky em um dicionario nomeado.

    A API OpenSky retorna cada voo como uma lista posicional, onde cada indice
    representa um campo. Esta funcao traduz esses indices para nomes de colunas,
    deixando os dados prontos para criar um Spark DataFrame com schema explicito.

    Args:
        state: Lista com os campos de um voo no formato original da API OpenSky.
        record_timestamp_unix: Timestamp Unix da execucao do pipeline, usado para
            marcar quando o registro foi coletado/processado.

    Returns:
        Dicionario com nomes de colunas compativeis com `RAW_FLIGHT_SCHEMA`.
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
        "record_timestamp_unix": record_timestamp_unix,
    }


def load_raw_json(raw_file_path: Path) -> dict[str, Any]:
    """
    Carrega o arquivo JSON bruto salvo na etapa de extracao.

    Esta funcao isola a leitura do arquivo para que a transformacao Spark possa
    receber um dicionario Python ja desserializado. Isso tambem facilita testes,
    pois voce pode passar arquivos JSON pequenos e controlados.

    Args:
        raw_file_path: Caminho do arquivo JSON bruto gerado pela extracao.

    Returns:
        Conteudo do JSON convertido para dicionario Python.
    """
    with open(raw_file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def transform_data_spark(
    spark: SparkSession,
    data: dict[str, Any],
    processed_data_dir: Path | None = None,
    timestamp: str | None = None,
) -> DataFrame:
    """
    Transforma dados brutos da OpenSky no formato final da tabela `flight_data`.

    Esta e a principal etapa de transformacao do ETL Spark. Ela recebe o JSON
    bruto da OpenSky, converte o array posicional `states` em linhas nomeadas,
    cria um Spark DataFrame com schema explicito, limpa campos textuais, converte
    timestamps Unix para timestamp Spark e seleciona somente as colunas finais.

    Args:
        spark: SparkSession usada para criar e manipular DataFrames.
        data: Dicionario com o retorno bruto da API OpenSky.
        processed_data_dir: Diretorio opcional onde uma copia processada em CSV
            sera salva para inspecao local.
        timestamp: Identificador temporal usado no nome da pasta CSV processada.

    Returns:
        Spark DataFrame transformado, com as colunas esperadas pela tabela
        `flight_data`.
    """
    record_timestamp = datetime.now(NATAL_TZ).replace(microsecond=0)
    record_timestamp_unix = int(record_timestamp.timestamp())
    rows = [
        _state_to_row(state, record_timestamp_unix)
        for state in data.get("states", [])
        if isinstance(state, list)
    ]

    raw_df = spark.createDataFrame(rows, schema=RAW_FLIGHT_SCHEMA)

    transformed_df = (
        raw_df
        .withColumn("flight_callsign", trim(col("callsign")))
        .withColumn("position_timestamp", to_timestamp(from_unixtime(col("time_position_unix"))))
        .withColumn("last_contact_timestamp", to_timestamp(from_unixtime(col("last_contact_unix"))))
        .withColumn("ingested_at", to_timestamp(from_unixtime(col("record_timestamp_unix"))))
        .withColumn("aircraft_code", col("icao24"))
        .withColumn("country_of_origin", col("origin_country"))
        .withColumn("barometric_altitude_m", col("baro_altitude"))
        .withColumn("geometric_altitude_m", col("geo_altitude"))
        .withColumn("is_on_ground", col("on_ground"))
        .where(col("icao24").isNotNull())
        .where(col("position_timestamp").isNotNull())
        .select(*FINAL_FLIGHT_COLUMNS)
    )

    if processed_data_dir and timestamp:
        output_path = processed_data_dir / f"processed_flight_data_spark_{timestamp}"
        (
            transformed_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(str(output_path))
        )
        logger.info(f"Dados Spark transformados salvos em: {output_path}")

    return transformed_df


def transform_file_spark(
    spark: SparkSession,
    raw_file_path: Path,
    processed_data_dir: Path | None = None,
    timestamp: str | None = None,
) -> DataFrame:
    """
    Executa a transformacao Spark a partir de um arquivo JSON bruto.

    Esta funcao combina duas etapas: leitura do JSON salvo em disco e chamada da
    transformacao principal. Ela e util para o pipeline completo, porque conecta
    naturalmente a saida da extracao com a entrada da transformacao.

    Args:
        spark: SparkSession usada durante a transformacao.
        raw_file_path: Caminho do arquivo JSON bruto.
        processed_data_dir: Diretorio opcional para salvar uma copia CSV tratada.
        timestamp: Identificador temporal usado no nome da saida processada.

    Returns:
        Spark DataFrame transformado.
    """
    data = load_raw_json(raw_file_path)
    return transform_data_spark(
        spark=spark,
        data=data,
        processed_data_dir=processed_data_dir,
        timestamp=timestamp,
    )
