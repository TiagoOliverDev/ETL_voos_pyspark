from __future__ import annotations

import logging
import os
import socket
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
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
    Valida se PostgreSQL e Spark Master estao disponiveis antes do ETL.

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


def _run_command(cmd: list[str], cwd: str | None = None, env: dict | None = None) -> None:
    """
    Executa um comando shell e envia a saida para o log do Airflow.

    Args:
        cmd: Comando a ser executado.
        cwd: Diretorio de trabalho opcional.
        env: Variaveis de ambiente opcionais.

    Returns:
        None.

    Raises:
        AirflowException: Quando o comando termina com erro.
    """
    LOG.info("Running command: %s", " ".join(cmd))
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=cwd,
        env={**os.environ, **(env or {})},
        text=True,
    )

    for line in process.stdout or []:
        LOG.info(line.rstrip())

    return_code = process.wait()
    if return_code != 0:
        raise AirflowException(
            f"Command failed with exit code {return_code}: {' '.join(cmd)}"
        )


def run_full_medallion_pipeline(**context) -> None:
    """
    Dispara a execucao completa do pipeline medalhao via script principal.

    Args:
        **context: Contexto de execucao do Airflow.

    Returns:
        None.
    """
    app_path = "/opt/airflow/app"
    main_script = f"{app_path}/main_spark.py"

    command = ["python", main_script]
    env = {
        **context.get("params", {}).get("env", {}),
    }
    _run_command(command, cwd=app_path, env=env)


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

    start = DummyOperator(task_id="start")

    check_deps = PythonOperator(
        task_id="check_service_dependencies",
        python_callable=check_services,
        params={"pg_port": 5432, "spark_port": 7077},
    )

    run_medallion_pipeline = PythonOperator(
        task_id="run_medallion_pipeline",
        python_callable=run_full_medallion_pipeline,
        params={
            "env": {
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "ETL_DB_HOST": "postgres-etl",
                "ETL_DB_PORT": "5432",
            }
        },
    )

    finish = DummyOperator(task_id="finish")

    start >> check_deps >> run_medallion_pipeline >> finish
