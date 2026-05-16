import json
import os
import requests

from pathlib import Path
from config.settings import FILES_FOLDER_RAW
from src.db.mongo_connections import save_raw_snapshot_to_mongo
from src.utils.logger import logger


def extract_data_spark(output_path: Path) -> Path:
    """
    Extrai dados brutos de voos da API OpenSky e salva o retorno em JSON e MongoDB.

    Esta funcao representa a etapa de extracao do ETL Spark. O Spark nao
    participa da chamada HTTP, porque a coleta via API e uma operacao pequena
    e centralizada. O arquivo JSON gerado aqui vira a entrada da etapa de
    transformacao com PySpark, enquanto o MongoDB recebe a camada raw com os
    estados brutos e os metadados do snapshot.

    Args:
        output_path: Caminho completo onde o arquivo JSON bruto sera salvo.

    Returns:
        O mesmo caminho recebido em `output_path`, para facilitar o encadeamento
        com a etapa de transformacao.

    Raises:
        Exception: Repassa erros de requisicao HTTP ou de escrita do arquivo.
    """
    url = "https://opensky-network.org/api/states/all"
    headers = {
        "User-Agent": "api-voo-pipeline-spark/1.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    try:
        response = requests.get(url=url, timeout=10, headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        logger.error(f"Erro ao fazer requisicao para OpenSky API: {exc}")
        raise

    os.makedirs(FILES_FOLDER_RAW, exist_ok=True)

    try:
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
    except Exception as exc:
        logger.error(f"Erro ao salvar arquivo bruto Spark: {exc}")
        raise

    try:
        save_raw_snapshot_to_mongo(data=data, source_file_path=str(output_path))
    except Exception as exc:
        logger.error(f"Erro ao salvar snapshot bruto no MongoDB: {exc}")
        raise

    logger.info(f"Dados brutos salvos para o ETL Spark em: {output_path}")
    return output_path
