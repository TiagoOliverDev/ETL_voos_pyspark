from pyspark.sql import DataFrame

from src.db.db_connections import (
    build_staging_table_name,
    create_staging_table_like_target,
    drop_table_if_exists,
    ensure_gold_tables_exist,
    get_gold_table_primary_keys,
    get_postgres_jdbc_options,
    merge_staging_into_target,
)
from src.etl_spark.schemas import (
    GOLD_COUNTRY_METRIC_COLUMNS,
    GOLD_FLIGHT_POSITION_COLUMNS,
)
from src.utils.logger import logger


def write_dataframe_to_postgres(df: DataFrame, dbtable: str, mode: str = "append") -> None:
    """
    Escreve um DataFrame Spark em uma tabela PostgreSQL via JDBC.

    Args:
        df: DataFrame Spark a ser persistido.
        dbtable: Nome da tabela de destino no PostgreSQL.
        mode: Modo de escrita JDBC, como `append` ou `overwrite`.

    Returns:
        None.

    Raises:
        TypeError: Quando o objeto recebido nao e um DataFrame Spark.
        Exception: Repassa erros de conexao ou escrita JDBC.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("O objeto recebido nao e um Spark DataFrame valido.")

    options = get_postgres_jdbc_options(dbtable=dbtable)

    try:
        (
            df.write
            .format("jdbc")
            .mode(mode)
            .options(**options)
            .save()
        )
    except Exception as exc:
        logger.error(f"Erro ao inserir dados Spark na tabela {dbtable}: {exc}")
        raise

    logger.info(f"Dados Spark escritos na tabela {dbtable} com sucesso.")


def get_gold_table_load_config() -> dict[str, dict[str, list[str]]]:
    """
    Retorna a configuracao de colunas e chaves usada na carga Gold.

    Returns:
        Dicionario com colunas e chaves por tabela Gold.
    """
    primary_keys = get_gold_table_primary_keys()
    return {
        "gold_flight_positions": {
            "columns": GOLD_FLIGHT_POSITION_COLUMNS,
            "conflict_columns": primary_keys["gold_flight_positions"],
        },
        "gold_country_metrics": {
            "columns": GOLD_COUNTRY_METRIC_COLUMNS,
            "conflict_columns": primary_keys["gold_country_metrics"],
        },
    }


def upsert_dataframe_to_postgres(
    df: DataFrame,
    target_table: str,
    columns: list[str],
    conflict_columns: list[str],
) -> dict[str, int]:
    """
    Publica um DataFrame no PostgreSQL com staging e merge por `ON CONFLICT`.

    O fluxo segue estes passos:
    1. cria uma tabela de staging com a estrutura da Gold alvo;
    2. grava o DataFrame Spark nessa staging via JDBC;
    3. faz o merge na Gold com insert ou update, conforme a chave;
    4. remove a staging ao final.

    Args:
        df: DataFrame Spark a ser publicado.
        target_table: Nome da tabela Gold de destino.
        columns: Colunas da tabela de destino.
        conflict_columns: Chaves usadas no `ON CONFLICT`.

    Returns:
        Dicionario com a quantidade de linhas inseridas e atualizadas.
    """
    staging_table = build_staging_table_name(target_table)
    dataframe_to_write = df.select(*columns)
    merge_result = {"inserted_count": 0, "updated_count": 0}

    try:
        create_staging_table_like_target(
            target_table=target_table,
            staging_table=staging_table,
        )
        write_dataframe_to_postgres(
            df=dataframe_to_write,
            dbtable=staging_table,
            mode="append",
        )
        merge_result = merge_staging_into_target(
            target_table=target_table,
            staging_table=staging_table,
            columns=columns,
            conflict_columns=conflict_columns,
        )
    finally:
        drop_table_if_exists(staging_table)

    logger.info(
        "Upsert finalizado para %s: %s inseridos, %s atualizados.",
        target_table,
        merge_result["inserted_count"],
        merge_result["updated_count"],
    )
    return merge_result


def load_gold_data_spark(
    gold_flight_positions_df: DataFrame,
    gold_country_metrics_df: DataFrame,
) -> None:
    """
    Publica as tabelas Gold do data lake no PostgreSQL com comportamento de upsert.

    Args:
        gold_flight_positions_df: Gold detalhada com posicoes de voo.
        gold_country_metrics_df: Gold agregada com metricas por pais.

    Returns:
        None.
    """
    ensure_gold_tables_exist()
    load_config = get_gold_table_load_config()

    upsert_dataframe_to_postgres(
        df=gold_flight_positions_df,
        target_table="gold_flight_positions",
        columns=load_config["gold_flight_positions"]["columns"],
        conflict_columns=load_config["gold_flight_positions"]["conflict_columns"],
    )
    upsert_dataframe_to_postgres(
        df=gold_country_metrics_df,
        target_table="gold_country_metrics",
        columns=load_config["gold_country_metrics"]["columns"],
        conflict_columns=load_config["gold_country_metrics"]["conflict_columns"],
    )
