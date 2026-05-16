from pyspark.sql import DataFrame

from src.db.db_connections import ensure_gold_tables_exist, get_postgres_jdbc_options
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

    logger.info(f"Dados Spark inseridos na tabela {dbtable} com sucesso.")


def load_gold_data_spark(
    gold_flight_positions_df: DataFrame,
    gold_country_metrics_df: DataFrame,
    mode: str = "append",
) -> None:
    """
    Publica as tabelas Gold do data lake no PostgreSQL.

    Args:
        gold_flight_positions_df: Gold detalhada com posicoes de voo.
        gold_country_metrics_df: Gold agregada com metricas por pais.
        mode: Modo de escrita JDBC usado nas duas tabelas.

    Returns:
        None.
    """
    ensure_gold_tables_exist()
    write_dataframe_to_postgres(
        df=gold_flight_positions_df,
        dbtable="gold_flight_positions",
        mode=mode,
    )
    write_dataframe_to_postgres(
        df=gold_country_metrics_df,
        dbtable="gold_country_metrics",
        mode=mode,
    )
