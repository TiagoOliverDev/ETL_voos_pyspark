import os

from dotenv import load_dotenv
import psycopg2

from src.utils.logger import logger

_ = load_dotenv()


def get_env_value(*names: str) -> str | None:
    """
    Busca o primeiro valor existente entre varias variaveis de ambiente.

    O projeto pode rodar localmente, via Docker ou como job Spark. Cada contexto
    pode usar nomes diferentes para as mesmas credenciais. Esta funcao permite
    consultar esses nomes em ordem de prioridade.

    Args:
        *names: Nomes das variaveis de ambiente a verificar.

    Returns:
        O valor da primeira variavel encontrada ou `None` se nenhuma existir.
    """
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def get_postgres_connection_config() -> dict[str, str]:
    """
    Retorna as credenciais do PostgreSQL usadas pelos jobs Spark.

    Variaveis aceitas:
        - ETL_DB_HOST ou HOST
        - ETL_DB_PORT ou PORT
        - ETL_DB_NAME ou DATABASE
        - ETL_DB_USER ou USERNAME
        - ETL_DB_PASSWORD ou PASSWORD

    Returns:
        Dicionario com `host`, `port`, `database`, `user` e `password`.

    Raises:
        ValueError: Quando alguma variavel obrigatoria nao foi configurada.
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
    Monta a URL JDBC do PostgreSQL para uso com Spark.

    Returns:
        URL no formato `jdbc:postgresql://host:porta/banco`.
    """
    config = get_postgres_connection_config()
    return f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"


def get_postgres_jdbc_options(dbtable: str = "flight_data") -> dict[str, str]:
    """
    Monta as opcoes JDBC necessarias para o Spark gravar no PostgreSQL.

    Esta e a funcao principal de conexao usada pelo ETL Spark. Ela centraliza
    URL, tabela, usuario, senha e driver para que os modulos de carga nao
    precisem conhecer detalhes das variaveis de ambiente.

    Args:
        dbtable: Nome da tabela de destino usada pelo writer JDBC do Spark.

    Returns:
        Dicionario pronto para ser passado em `df.write.options(**options)`.
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
    Cria uma conexao direta com PostgreSQL usando psycopg2.

    Esta conexao e usada para operacoes administrativas leves do ETL, como
    garantir a existencia da tabela de destino antes da carga via Spark JDBC.

    Returns:
        Conexao psycopg2 aberta para o banco configurado.
    """
    config = get_postgres_connection_config()
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["user"],
        password=config["password"],
    )


def ensure_flight_data_table_exists() -> None:
    """
    Garante que a tabela `flight_data` exista no PostgreSQL.

    A funcao executa um `CREATE TABLE IF NOT EXISTS` com o schema esperado pelo
    ETL Spark. Isso permite que a primeira carga do projeto funcione mesmo em um
    banco vazio, sem depender de criacao manual previa.

    Returns:
        None.

    Raises:
        Exception: Repassa erros de conexao ou execucao SQL.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS flight_data (
        aircraft_code VARCHAR(10) NOT NULL,
        flight_callsign VARCHAR(10),
        country_of_origin VARCHAR(100),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        velocity DOUBLE PRECISION,
        heading DOUBLE PRECISION,
        barometric_altitude_m DOUBLE PRECISION,
        geometric_altitude_m DOUBLE PRECISION,
        is_on_ground BOOLEAN,
        position_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        last_contact_timestamp TIMESTAMP WITHOUT TIME ZONE,
        ingested_at TIMESTAMP WITHOUT TIME ZONE,
        PRIMARY KEY (aircraft_code, position_timestamp)
    );
    """

    try:
        with get_postgres_psycopg2_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
        logger.info("Tabela flight_data verificada/criada com sucesso.")
    except Exception as exc:
        logger.error(f"Erro ao garantir a existencia da tabela flight_data: {exc}")
        raise
