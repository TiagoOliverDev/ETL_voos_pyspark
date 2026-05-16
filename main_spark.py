from datetime import datetime
from pathlib import Path

from config.settings import FILES_FOLDER_RAW, NATAL_TZ, PROCESSED_DATA_DIR
from src.etl_spark.extract_data_spark import extract_data_spark
from src.etl_spark.load_data_spark import load_data_spark
from src.etl_spark.spark_session import create_spark_session
from src.etl_spark.transform_data_spark import transform_file_spark
from src.utils.logger import logger


def main() -> None:
    """
    Executa o pipeline ETL Spark completo para dados de voos da OpenSky.

    O fluxo orquestrado aqui segue tres etapas:
    1. extrair o JSON bruto da API OpenSky;
    2. persistir a camada raw no MongoDB;
    3. transformar os dados com PySpark;
    4. carregar o DataFrame final no PostgreSQL via JDBC.

    A funcao tambem cria a SparkSession no inicio e garante o encerramento dela
    no bloco `finally`, liberando os recursos locais ou do cluster Spark.

    Returns:
        None.

    Raises:
        Exception: Repassa qualquer erro ocorrido nas etapas de extracao,
        transformacao ou carga.
    """
    execution_dt = datetime.now(NATAL_TZ)
    timestamp = execution_dt.strftime("%Y-%m-%d_%H-%M-%S")
    output_path = Path(FILES_FOLDER_RAW) / f"flight_data_spark_{timestamp}.json"

    spark = create_spark_session()

    try:
        logger.info("Iniciando pipeline ETL Spark - OpenSky Flight Data")

        logger.info("[1/3] Extraindo dados da API OpenSky...")
        raw_file_path = extract_data_spark(output_path=output_path)

        logger.info("[2/3] Transformando dados com PySpark...")
        processed_df = transform_file_spark(
            spark=spark,
            raw_file_path=raw_file_path,
            processed_data_dir=Path(PROCESSED_DATA_DIR),
            timestamp=timestamp,
        )

        logger.info("[3/3] Carregando dados no PostgreSQL via JDBC...")
        load_data_spark(processed_df)

        logger.info("Pipeline ETL Spark finalizado com sucesso.")
    except Exception as exc:
        logger.error(f"Ocorreu um erro no pipeline ETL Spark: {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
