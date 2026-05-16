from datetime import datetime
from pathlib import Path

import pytz


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_LAKE_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_LAKE_DIR / "bronze"
SILVER_DIR = DATA_LAKE_DIR / "silver"
GOLD_DIR = DATA_LAKE_DIR / "gold"

BRONZE_DATASET_NAME = "opensky_api_states"
SILVER_DATASET_NAME = "flight_positions_curated"
GOLD_FLIGHT_POSITIONS_DATASET = "flight_positions"
GOLD_COUNTRY_METRICS_DATASET = "country_metrics"

NATAL_TZ = pytz.timezone("America/Recife")


def ensure_data_lake_directories() -> None:
    """
    Garante a existencia das pastas principais da arquitetura medalhao.

    Returns:
        None.
    """
    for directory in (DATA_LAKE_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def build_bronze_file_path(execution_dt: datetime) -> Path:
    """
    Monta o caminho particionado do arquivo bruto da camada Bronze.

    O objetivo e organizar os snapshots crus por data e hora de ingestao,
    facilitando auditoria, reprocessamento e navegacao no data lake local.

    Args:
        execution_dt: Data e hora da execucao que gerou o snapshot.

    Returns:
        Caminho completo do arquivo JSON a ser salvo na Bronze.
    """
    partition_dir = (
        BRONZE_DIR
        / BRONZE_DATASET_NAME
        / f"year={execution_dt:%Y}"
        / f"month={execution_dt:%m}"
        / f"day={execution_dt:%d}"
        / f"hour={execution_dt:%H}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    return partition_dir / f"flight_snapshot_{execution_dt:%Y-%m-%d_%H-%M-%S}.json"
