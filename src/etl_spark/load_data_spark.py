from pyspark.sql import DataFrame

from src.db.db_connections import ensure_flight_data_table_exists, get_postgres_jdbc_options
from src.utils.logger import logger


def load_data_spark(df: DataFrame, mode: str = "append") -> None:
    """
    Carrega um Spark DataFrame na tabela `flight_data` do PostgreSQL via JDBC.

    Esta funcao representa a etapa de carga do ETL Spark. Nesta primeira versao,
    o modo padrao e `append`, ou seja, os dados sao adicionados na tabela. Em uma
    evolucao futura, esta carga pode virar um upsert usando tabela temporaria,
    `ON CONFLICT` no PostgreSQL ou `foreachBatch` no streaming.

    Args:
        df: Spark DataFrame ja transformado e com o schema final esperado.
        mode: Modo de escrita do Spark JDBC, como `append`, `overwrite`,
            `ignore` ou `error`.

    Raises:
        TypeError: Quando o objeto recebido nao e um Spark DataFrame.
        Exception: Repassa erros de conexao ou escrita JDBC.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("O objeto recebido nao e um Spark DataFrame valido.")

    ensure_flight_data_table_exists()
    options = get_postgres_jdbc_options()

    try:
        (
            df.write
            .format("jdbc")
            .mode(mode)
            .options(**options)
            .save()
        )
    except Exception as exc:
        logger.error(f"Erro ao inserir dados Spark no PostgreSQL: {exc}")
        raise

    logger.info("Dados Spark inseridos na tabela flight_data com sucesso.")
