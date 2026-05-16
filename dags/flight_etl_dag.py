from __future__ import annotations

import logging
import socket
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

LOG = logging.getLogger("airflow.task")


def _check_tcp(host: str, port: int, timeout: int = 5) -> None:
    """Verifica se um serviço TCP está acessível no host e porta informados."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
    except Exception as e:
        raise AirflowException(f"Service {host}:{port} unreachable: {e}")
    finally:
        s.close()


def check_services(**context):
    """Valida que os serviços dependentes estão disponíveis antes de executar o ETL."""
    services = [
        ("mongodb", int(context.get("params", {}).get("mongo_port", 27017))),
        ("postgres-etl", int(context.get("params", {}).get("pg_port", 5432))),
        ("spark-master", int(context.get("params", {}).get("spark_port", 7077))),
    ]
    for host, port in services:
        LOG.info("Checking %s:%s", host, port)
        _check_tcp(host, port)


def _run_command(cmd: list[str], cwd: str | None = None, env: dict | None = None) -> None:
    """Executa um comando shell e propaga a saída para o log do Airflow."""
    LOG.info("Running command: %s", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd, env=env, text=True)
    for line in proc.stdout or []:
        LOG.info(line.rstrip())
    rc = proc.wait()
    if rc != 0:
        raise AirflowException(f"Command failed with exit code {rc}: {' '.join(cmd)}")


def run_full_etl(**context):
    """Dispara a execução completa do pipeline ETL via o script principal do projeto."""
    app_path = "/opt/airflow/app"
    main_script = f"{app_path}/main_spark.py"

    cmd = ["python", main_script]
    env = {
        **context.get("params", {}).get("env", {}),
    }
    _run_command(cmd, cwd=app_path, env=env)


default_args = {
    "owner": "voos-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="flight_data_etl",
    start_date=datetime(2026, 5, 16),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["etl", "pyspark", "flights"],
) as dag:

    start = DummyOperator(task_id="start")

    check_deps = PythonOperator(
        task_id="check_service_dependencies",
        python_callable=check_services,
        params={"mongo_port": 27017, "pg_port": 5432, "spark_port": 7077},
    )

    extract_and_persist = PythonOperator(
        task_id="run_full_etl",
        python_callable=run_full_etl,
        params={
            "env": {
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "ETL_DB_HOST": "postgres-etl",
                "ETL_DB_PORT": "5432",
                "MONGO_HOST": "mongodb",
                "MONGO_PORT": "27017",
            }
        },
    )

    finish = DummyOperator(task_id="finish")

    start >> check_deps >> extract_and_persist >> finish
