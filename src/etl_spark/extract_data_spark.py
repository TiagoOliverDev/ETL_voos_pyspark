import json
from datetime import datetime
from pathlib import Path

import requests

from config.settings import build_bronze_file_path, ensure_data_lake_directories
from src.utils.logger import logger


def fetch_opensky_snapshot() -> dict:
    """
    Consulta a API OpenSky e retorna o payload bruto do snapshot atual.

    Returns:
        Dicionario com a resposta JSON da API OpenSky.

    Raises:
        Exception: Repassa erros de rede, timeout ou resposta invalida.
    """
    url = "https://opensky-network.org/api/states/all"
    headers = {
        "User-Agent": "api-voo-pipeline-spark/2.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    try:
        response = requests.get(url=url, timeout=10, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        logger.error(f"Erro ao fazer requisicao para OpenSky API: {exc}")
        raise


def save_bronze_snapshot(data: dict, output_path: Path) -> Path:
    """
    Persiste um snapshot bruto na camada Bronze do data lake local.

    Args:
        data: Payload bruto retornado pela API OpenSky.
        output_path: Caminho completo do arquivo JSON de destino.

    Returns:
        Caminho final do arquivo gravado na Bronze.

    Raises:
        Exception: Repassa erros de escrita em disco.
    """
    ensure_data_lake_directories()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
    except Exception as exc:
        logger.error(f"Erro ao salvar arquivo bruto na Bronze: {exc}")
        raise

    logger.info(f"Snapshot Bronze salvo em: {output_path}")
    return output_path


def extract_data_spark(execution_dt: datetime) -> Path:
    """
    Executa a extracao da OpenSky e salva o resultado na camada Bronze.

    Args:
        execution_dt: Data e hora de referencia da execucao atual.

    Returns:
        Caminho do arquivo JSON salvo na Bronze.
    """
    output_path = build_bronze_file_path(execution_dt)
    data = fetch_opensky_snapshot()
    return save_bronze_snapshot(data=data, output_path=output_path)
