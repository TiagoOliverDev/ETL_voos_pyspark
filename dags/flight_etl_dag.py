from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timedelta
from uuid import uuid4

import docker
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

LOG = logging.getLogger("airflow.task")


def _check_tcp(host: str, port: int, timeout: int = 5) -> None:
    """
    Verifica se um servico TCP esta acessivel no host e porta informados.

    Args:
        host: Nome do host a ser verificado.
        port: Porta TCP do servico.
        timeout: Tempo maximo de espera em segundos.

    Returns:
        None.

    Raises:
        AirflowException: Quando o servico nao responde no tempo esperado.
    """
    socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_client.settimeout(timeout)
    try:
        socket_client.connect((host, port))
    except Exception as exc:
        raise AirflowException(f"Service {host}:{port} unreachable: {exc}")
    finally:
        socket_client.close()


def check_services(**context) -> None:
    """
    Valida se PostgreSQL, Spark Master e Docker estao disponiveis antes do ETL.

    Args:
        **context: Contexto de execucao do Airflow.

    Returns:
        None.
    """
    services = [
        ("postgres-etl", int(context.get("params", {}).get("pg_port", 5432))),
        ("spark-master", int(context.get("params", {}).get("spark_port", 7077))),
    ]

    for host, port in services:
        LOG.info("Checking %s:%s", host, port)
        _check_tcp(host, port)

    try:
        docker.from_env().ping()
    except Exception as exc:
        raise AirflowException(f"Docker daemon indisponivel para o Airflow: {exc}")


def run_medallion_pipeline_in_spark_container(**context) -> None:
    """
    Executa o pipeline medalhao em um container derivado da imagem Spark do projeto.

    O Airflow atua apenas como orquestrador. A execucao real do job ocorre em
    um container temporario criado a partir da imagem `flight-etl-spark:latest`,
    que submete o script principal ao cluster Spark.

    Args:
        **context: Contexto de execucao do Airflow.

    Returns:
        None.

    Raises:
        AirflowException: Quando a execucao do container falha.
    """
    container_name = f"airflow_medallion_job_{uuid4().hex[:10]}"
    environment = {
        **context.get("params", {}).get("env", {}),
    }

    client = docker.from_env()
    container = None

    try:
        LOG.info(
            "Criando container temporario %s para executar o spark-submit.",
            container_name,
        )
        container = client.containers.run(
            image="flight-etl-spark:latest",
            command=["/opt/spark/bin/spark-submit", "/app/main_spark.py"],
            name=container_name,
            detach=True,
            network="etl-spark-network",
            working_dir="/app",
            environment=environment,
        )

        for raw_line in container.logs(stream=True, follow=True):
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            if line:
                LOG.info(line)

        result = container.wait()
        status_code = int(result.get("StatusCode", 1))
        if status_code != 0:
            raise AirflowException(
                f"Container {container_name} finalizou com codigo {status_code}."
            )

        LOG.info("Container temporario %s finalizado com sucesso.", container_name)
    except docker.errors.DockerException as exc:
        raise AirflowException(f"Falha ao executar o container Spark: {exc}")
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except docker.errors.DockerException:
                LOG.warning(
                    "Nao foi possivel remover o container temporario %s.",
                    container_name,
                )


def log_pipeline_start() -> None:
    """
    Registra no log o inicio da execucao da DAG.

    Returns:
        None.
    """
    LOG.info("Iniciando a execucao da DAG flight_data_medallion_etl.")


def log_pipeline_finish() -> None:
    """
    Registra no log o fim da execucao da DAG.

    Returns:
        None.
    """
    LOG.info("Finalizando a execucao da DAG flight_data_medallion_etl.")


default_args = {
    "owner": "voos-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="flight_data_medallion_etl",
    start_date=datetime(2026, 5, 16),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["etl", "pyspark", "flights", "medallion"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=log_pipeline_start,
    )

    check_deps = PythonOperator(
        task_id="check_service_dependencies",
        python_callable=check_services,
        params={"pg_port": 5432, "spark_port": 7077},
    )

    run_medallion_pipeline = PythonOperator(
        task_id="run_medallion_pipeline",
        python_callable=run_medallion_pipeline_in_spark_container,
        params={
            "env": {
                "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
                "ETL_DB_HOST": os.getenv("ETL_DB_HOST", "postgres-etl"),
                "ETL_DB_PORT": os.getenv("ETL_DB_PORT", "5432"),
                "ETL_DB_NAME": os.getenv("ETL_DB_NAME", "flight_data_db"),
                "ETL_DB_USER": os.getenv("ETL_DB_USER", "postgres"),
                "ETL_DB_PASSWORD": os.getenv("ETL_DB_PASSWORD", "root"),
                "PYTHONPATH": "/app",
                "PYSPARK_PYTHON": "/usr/bin/python3",
                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
            }
        },
    )

    finish = PythonOperator(
        task_id="finish",
        python_callable=log_pipeline_finish,
    )

    start >> check_deps >> run_medallion_pipeline >> finish
