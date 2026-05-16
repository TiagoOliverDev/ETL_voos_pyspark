import os
from uuid import uuid4

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

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


def get_gold_table_primary_keys() -> dict[str, list[str]]:
    """
    Retorna as colunas de chave primaria de cada tabela Gold.

    Returns:
        Dicionario com o nome da tabela e sua lista de chaves.
    """
    return {
        "gold_flight_positions": ["aircraft_code", "position_timestamp"],
        "gold_country_metrics": ["snapshot_timestamp", "country_of_origin"],
    }


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


def build_staging_table_name(target_table: str) -> str:
    """
    Gera um nome unico para a tabela de staging de uma carga Gold.

    Args:
        target_table: Nome da tabela Gold de destino.

    Returns:
        Nome fisico da tabela temporaria de staging.
    """
    return f"stg_{target_table}_{uuid4().hex[:12]}"


def create_staging_table_like_target(target_table: str, staging_table: str) -> None:
    """
    Cria uma tabela de staging com a mesma estrutura da tabela de destino.

    Args:
        target_table: Tabela Gold de destino.
        staging_table: Nome da tabela de staging a ser criada.

    Returns:
        None.
    """
    statement = sql.SQL(
        "CREATE TABLE {staging} (LIKE {target} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY)"
    ).format(
        staging=sql.Identifier(staging_table),
        target=sql.Identifier(target_table),
    )

    with get_postgres_psycopg2_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)


def drop_table_if_exists(table_name: str) -> None:
    """
    Remove uma tabela auxiliar do PostgreSQL caso ela exista.

    Args:
        table_name: Nome da tabela a ser removida.

    Returns:
        None.
    """
    statement = sql.SQL("DROP TABLE IF EXISTS {table_name}").format(
        table_name=sql.Identifier(table_name)
    )

    with get_postgres_psycopg2_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)


def merge_staging_into_target(
    target_table: str,
    staging_table: str,
    columns: list[str],
    conflict_columns: list[str],
) -> dict[str, int]:
    """
    Faz o merge da staging na tabela Gold usando `ON CONFLICT`.

    Quando a chave ainda nao existe, o registro e inserido. Quando a chave ja
    existe, os campos nao-chave sao atualizados com os valores mais recentes da
    staging.

    Args:
        target_table: Nome da tabela Gold de destino.
        staging_table: Nome da tabela de staging com os dados da carga atual.
        columns: Colunas que participam do merge.
        conflict_columns: Colunas usadas para detectar duplicidade.

    Returns:
        Dicionario com a quantidade de registros inseridos e atualizados.
    """
    update_columns = [column for column in columns if column not in conflict_columns]

    insert_columns_sql = sql.SQL(", ").join(
        sql.Identifier(column) for column in columns
    )
    select_columns_sql = sql.SQL(", ").join(
        sql.Identifier(column) for column in columns
    )
    conflict_columns_sql = sql.SQL(", ").join(
        sql.Identifier(column) for column in conflict_columns
    )

    if update_columns:
        update_assignments_sql = sql.SQL(", ").join(
            sql.SQL("{column} = EXCLUDED.{column}").format(
                column=sql.Identifier(column)
            )
            for column in update_columns
        )
        statement = sql.SQL(
            """
            WITH merged_rows AS (
                INSERT INTO {target} ({insert_columns})
                SELECT {select_columns}
                FROM {staging}
                ON CONFLICT ({conflict_columns})
                DO UPDATE SET {update_assignments}
                RETURNING xmax = 0 AS inserted
            )
            SELECT
                COALESCE(SUM(CASE WHEN inserted THEN 1 ELSE 0 END), 0) AS inserted_count,
                COALESCE(SUM(CASE WHEN inserted THEN 0 ELSE 1 END), 0) AS updated_count
            FROM merged_rows
            """
        ).format(
            target=sql.Identifier(target_table),
            insert_columns=insert_columns_sql,
            select_columns=select_columns_sql,
            staging=sql.Identifier(staging_table),
            conflict_columns=conflict_columns_sql,
            update_assignments=update_assignments_sql,
        )
    else:
        statement = sql.SQL(
            """
            WITH merged_rows AS (
                INSERT INTO {target} ({insert_columns})
                SELECT {select_columns}
                FROM {staging}
                ON CONFLICT ({conflict_columns})
                DO NOTHING
                RETURNING TRUE AS inserted
            )
            SELECT
                COALESCE(COUNT(*), 0) AS inserted_count,
                0 AS updated_count
            FROM merged_rows
            """
        ).format(
            target=sql.Identifier(target_table),
            insert_columns=insert_columns_sql,
            select_columns=select_columns_sql,
            staging=sql.Identifier(staging_table),
            conflict_columns=conflict_columns_sql,
        )

    with get_postgres_psycopg2_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            result = cursor.fetchone()

    return {
        "inserted_count": int(result[0]) if result else 0,
        "updated_count": int(result[1]) if result else 0,
    }
