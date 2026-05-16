from datetime import datetime

from config.settings import ensure_data_lake_directories, NATAL_TZ
from src.etl_spark.extract_data_spark import extract_data_spark
from src.etl_spark.load_data_spark import load_gold_data_spark
from src.etl_spark.spark_session import create_spark_session
from src.etl_spark.transform_data_spark import (
    build_and_save_gold_layers,
    transform_file_to_silver,
)
from src.utils.logger import logger


def main() -> None:
    """
    Executa o pipeline medalhao completo para dados de voos da OpenSky.

    O fluxo atual segue as etapas:
    1. extrair o snapshot bruto e gravar na Bronze;
    2. transformar o bruto em dados curados na Silver;
    3. gerar datasets Gold detalhados e agregados;
    4. publicar as tabelas Gold no PostgreSQL.

    Returns:
        None.

    Raises:
        Exception: Repassa qualquer erro ocorrido durante a execucao.
    """
    execution_dt = datetime.now(NATAL_TZ).replace(microsecond=0)
    ensure_data_lake_directories()
    spark = create_spark_session()

    try:
        logger.info("Iniciando pipeline medalhao Spark - OpenSky Flight Data")

        logger.info("[1/4] Extraindo snapshot bruto para a Bronze...")
        bronze_file_path = extract_data_spark(execution_dt=execution_dt)

        logger.info("[2/4] Transformando Bronze em Silver...")
        silver_df = transform_file_to_silver(
            spark=spark,
            raw_file_path=bronze_file_path,
            execution_dt=execution_dt,
        )

        logger.info("[3/4] Gerando e persistindo a camada Gold...")
        gold_flight_positions_df, gold_country_metrics_df = build_and_save_gold_layers(
            silver_df
        )

        logger.info("[4/4] Publicando tabelas Gold no PostgreSQL...")
        load_gold_data_spark(
            gold_flight_positions_df=gold_flight_positions_df,
            gold_country_metrics_df=gold_country_metrics_df,
        )

        logger.info("Pipeline medalhao Spark finalizado com sucesso.")
    except Exception as exc:
        logger.error(f"Ocorreu um erro no pipeline medalhao Spark: {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
