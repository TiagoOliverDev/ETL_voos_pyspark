import os

import psycopg2
from dotenv import load_dotenv

from src.utils.logger import logger

_ = load_dotenv()


def get_env_value(*names: str) -> str | None:
    """
    Busca o primeiro valor existente entre varias variaveis de ambiente.

    Args:
        *names: Nomes das variaveis de ambiente a verificar.

    Returns:
        O valor da primeira variavel encontrada ou `None`.
    """
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def get_postgres_connection_config() -> dict[str, str]:
    """
    Retorna a configuracao de conexao com PostgreSQL usada pelo projeto.

    Returns:
        Dicionario com host, port, database, user e password.

    Raises:
        ValueError: Quando alguma variavel obrigatoria nao estiver configurada.
    """
    config = {
        "host": get_env_value("ETL_DB_HOST", "HOST"),
        "port": get_env_value("ETL_DB_PORT", "PORT"),
        "database": get_env_value("ETL_DB_NAME", "DATABASE"),
        "user": get_env_value("ETL_DB_USER", "USERNAME"),
        "password": get_env_value("ETL_DB_PASSWORD", "PASSWORD"),
    }

    missing = [name for name, value in config.items() if not value]
    if missing:
        raise ValueError(
            f"Variaveis de conexao ausentes para PostgreSQL: {', '.join(missing)}"
        )

    return config


def get_postgres_jdbc_url() -> str:
    """
    Monta a URL JDBC do PostgreSQL.

    Returns:
        URL JDBC no formato aceito pelo Spark.
    """
    config = get_postgres_connection_config()
    return f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"


def get_postgres_jdbc_options(dbtable: str) -> dict[str, str]:
    """
    Monta as opcoes JDBC para escrita Spark no PostgreSQL.

    Args:
        dbtable: Nome da tabela de destino.

    Returns:
        Dicionario com URL, tabela, usuario, senha e driver JDBC.
    """
    config = get_postgres_connection_config()
    return {
        "url": get_postgres_jdbc_url(),
        "dbtable": dbtable,
        "user": config["user"],
        "password": config["password"],
        "driver": "org.postgresql.Driver",
    }


def get_postgres_psycopg2_connection():
    """
    Cria uma conexao psycopg2 para operacoes administrativas leves.

    Returns:
        Conexao aberta com o PostgreSQL configurado.
    """
    config = get_postgres_connection_config()
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["user"],
        password=config["password"],
    )


def get_gold_table_ddl() -> list[str]:
    """
    Retorna os comandos DDL das tabelas Gold publicadas no PostgreSQL.

    Returns:
        Lista de comandos `CREATE TABLE IF NOT EXISTS`.
    """
    return [
        """
        CREATE TABLE IF NOT EXISTS gold_flight_positions (
            aircraft_code VARCHAR(10) NOT NULL,
            flight_callsign VARCHAR(20),
            country_of_origin VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            velocity DOUBLE PRECISION,
            heading DOUBLE PRECISION,
            vertical_rate DOUBLE PRECISION,
            barometric_altitude_m DOUBLE PRECISION,
            geometric_altitude_m DOUBLE PRECISION,
            is_on_ground BOOLEAN,
            position_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            last_contact_timestamp TIMESTAMP WITHOUT TIME ZONE,
            snapshot_timestamp TIMESTAMP WITHOUT TIME ZONE,
            ingested_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            snapshot_date DATE NOT NULL,
            snapshot_hour INTEGER NOT NULL,
            PRIMARY KEY (aircraft_code, position_timestamp)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS gold_country_metrics (
            snapshot_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            snapshot_date DATE NOT NULL,
            snapshot_hour INTEGER NOT NULL,
            country_of_origin VARCHAR(100) NOT NULL,
            total_flights BIGINT NOT NULL,
            airborne_flights BIGINT NOT NULL,
            grounded_flights BIGINT NOT NULL,
            avg_velocity DOUBLE PRECISION,
            avg_barometric_altitude_m DOUBLE PRECISION,
            max_geometric_altitude_m DOUBLE PRECISION,
            PRIMARY KEY (snapshot_timestamp, country_of_origin)
        );
        """,
    ]


def ensure_gold_tables_exist() -> None:
    """
    Garante a existencia das tabelas Gold no PostgreSQL.

    Returns:
        None.

    Raises:
        Exception: Repassa erros de conexao ou execucao SQL.
    """
    statements = get_gold_table_ddl()

    try:
        with get_postgres_psycopg2_connection() as connection:
            with connection.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
        logger.info("Tabelas Gold verificadas/criadas com sucesso.")
    except Exception as exc:
        logger.error(f"Erro ao garantir a existencia das tabelas Gold: {exc}")
        raise
