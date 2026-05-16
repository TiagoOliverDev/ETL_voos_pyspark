import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "OpenSkyFlightETL") -> SparkSession:
    """
    Cria a SparkSession usada pelos jobs PySpark do projeto.

    A SparkSession e o ponto de entrada para trabalhar com DataFrames no Spark.
    Se a variavel `SPARK_MASTER_URL` estiver configurada, a sessao usa esse
    master, por exemplo `spark://spark-master:7077` em um cluster Docker. Se a
    variavel nao existir, a execucao usa `local[*]`, aproveitando os nucleos da
    maquina local.

    Args:
        app_name: Nome da aplicacao exibido nos logs e na Spark UI.

    Returns:
        Uma SparkSession pronta para leitura, transformacao e escrita de dados.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
    )

    spark_master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
    builder = builder.master(spark_master_url)

    return builder.getOrCreate()
