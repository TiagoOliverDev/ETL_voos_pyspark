import os
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient

from src.utils.logger import logger

_ = load_dotenv()


def get_mongo_env_value(name: str, default: str | None = None) -> str | None:
    """
    Retorna uma variavel de ambiente usada pela conexao com MongoDB.

    Args:
        name: Nome da variavel de ambiente.
        default: Valor padrao usado quando a variavel nao existir.

    Returns:
        Valor da variavel ou o padrao informado.
    """
    return os.getenv(name, default)


def get_mongo_connection_config() -> dict[str, Any]:
    """
    Retorna a configuracao de conexao usada pelo MongoDB do projeto.

    Returns:
        Dicionario com host, port, database, username e password.
    """
    return {
        "host": get_mongo_env_value("MONGO_HOST", "mongodb"),
        "port": int(get_mongo_env_value("MONGO_PORT", "27017")),
        "database": get_mongo_env_value("MONGO_DB", "flight_data_raw"),
        "username": get_mongo_env_value("MONGO_USER"),
        "password": get_mongo_env_value("MONGO_PASSWORD"),
    }


def get_mongo_client() -> MongoClient:
    """
    Cria um cliente MongoDB com ou sem autenticacao.

    Returns:
        Cliente MongoDB pronto para acessar o banco configurado.
    """
    config = get_mongo_connection_config()

    if config["username"] and config["password"]:
        return MongoClient(
            host=config["host"],
            port=config["port"],
            username=config["username"],
            password=config["password"],
            authSource="admin",
        )

    return MongoClient(host=config["host"], port=config["port"])


def get_mongo_database():
    """
    Retorna o banco MongoDB usado como camada raw do projeto.

    Returns:
        Instancia do banco configurado no MongoDB.
    """
    client = get_mongo_client()
    return client[get_mongo_connection_config()["database"]]


def ensure_mongo_indexes() -> None:
    """
    Garante indices uteis nas colecoes raw do MongoDB.

    Returns:
        None.
    """
    database = get_mongo_database()
    database.raw_flight_snapshots.create_index("extracted_at")
    database.raw_flight_snapshots.create_index("api_timestamp")
    database.raw_flight_states.create_index("snapshot_id")
    database.raw_flight_states.create_index("icao24")
    database.raw_flight_states.create_index("time_position")


def save_raw_snapshot_to_mongo(data: dict[str, Any], source_file_path: str) -> str:
    """
    Salva o snapshot bruto da OpenSky em MongoDB.

    O armazenamento raw e dividido em duas colecoes:
    - `raw_flight_snapshots`: metadados do snapshot;
    - `raw_flight_states`: um documento por estado bruto retornado pela API.

    Isso evita criar um documento gigante com milhares de estados e deixa a
    camada bronze mais adequada para alto volume e dados semiestruturados.

    Args:
        data: Payload bruto retornado pela API OpenSky.
        source_file_path: Caminho local do arquivo JSON salvo em disco.

    Returns:
        ID do documento de snapshot criado na colecao `raw_flight_snapshots`.
    """
    ensure_mongo_indexes()
    database = get_mongo_database()

    extracted_at = datetime.now(timezone.utc)
    snapshot_document = {
        "source": "opensky_api_states_all",
        "api_timestamp": data.get("time"),
        "state_count": len(data.get("states", [])),
        "source_file_path": source_file_path,
        "extracted_at": extracted_at,
    }

    snapshot_result = database.raw_flight_snapshots.insert_one(snapshot_document)
    snapshot_id = snapshot_result.inserted_id

    state_documents = []
    for state in data.get("states", []):
        if not isinstance(state, list):
            continue

        state_documents.append(
            {
                "snapshot_id": snapshot_id,
                "api_timestamp": data.get("time"),
                "extracted_at": extracted_at,
                "icao24": state[0] if len(state) > 0 else None,
                "callsign_raw": state[1] if len(state) > 1 else None,
                "origin_country": state[2] if len(state) > 2 else None,
                "time_position": state[3] if len(state) > 3 else None,
                "last_contact": state[4] if len(state) > 4 else None,
                "longitude": state[5] if len(state) > 5 else None,
                "latitude": state[6] if len(state) > 6 else None,
                "baro_altitude": state[7] if len(state) > 7 else None,
                "on_ground": state[8] if len(state) > 8 else None,
                "velocity": state[9] if len(state) > 9 else None,
                "heading": state[10] if len(state) > 10 else None,
                "vertical_rate": state[11] if len(state) > 11 else None,
                "sensors": state[12] if len(state) > 12 else None,
                "geo_altitude": state[13] if len(state) > 13 else None,
                "squawk": state[14] if len(state) > 14 else None,
                "spi": state[15] if len(state) > 15 else None,
                "position_source": state[16] if len(state) > 16 else None,
                "raw_state": state,
            }
        )

    if state_documents:
        database.raw_flight_states.insert_many(state_documents, ordered=False)

    logger.info(
        f"Snapshot bruto salvo no MongoDB com {len(state_documents)} estados."
    )
    return str(snapshot_id)
