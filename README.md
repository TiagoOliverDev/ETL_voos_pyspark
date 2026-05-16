# Flight Data ETL com PySpark

Projeto de ETL para coleta, transformacao e carga de dados de voos da API OpenSky Network usando **Python**, **PySpark**, **MongoDB**, **PostgreSQL** e um **cluster Spark em Docker**.

O pipeline atual faz quatro etapas:

1. extrai dados brutos da API OpenSky e salva em JSON;
2. persiste a camada raw no MongoDB;
3. transforma os dados com PySpark;
4. carrega o resultado na tabela `flight_data` do PostgreSQL via JDBC.

## Visao Geral

O projeto foi organizado para praticar PySpark em um fluxo real de engenharia de dados.

- A extracao consulta a API `states/all` da OpenSky.
- A transformacao converte o array `states` em colunas nomeadas.
- Os timestamps Unix sao convertidos para `timestamp`.
- Os dados brutos sao persistidos no MongoDB.
- Os dados finais tratados sao gravados no PostgreSQL.
- A tabela `flight_data` é criada automaticamente se ainda nao existir.

## Arquitetura Atual

```text
OpenSky API
    |
    v
MongoDB raw + JSON bruto em data/raw
    |
    v
PySpark ETL
    |
    v
PostgreSQL
```

Com Docker Compose, o ambiente atual sobe:

- `postgres-etl`
- `mongodb`
- `spark-master`
- `spark-worker-1`
- `spark-worker-2`
- `etl-spark`

## Estrutura do Projeto

```text
.
|-- config/
|   `-- settings.py
|-- src/
|   |-- db/
|   |   `-- db_connections.py
|   |-- etl_spark/
|   |   |-- extract_data_spark.py
|   |   |-- transform_data_spark.py
|   |   |-- load_data_spark.py
|   |   |-- schemas.py
|   |   `-- spark_session.py
|   `-- utils/
|       `-- logger.py
|-- .env.example
|-- docker-compose.yml
|-- Dockerfile
|-- Dockerfile.spark
|-- main_spark.py
|-- requirements.txt
`-- README.md
```

## Principais Arquivos

| Caminho | Descricao |
|---|---|
| `main_spark.py` | Executa o pipeline completo |
| `src/etl_spark/extract_data_spark.py` | Extrai dados brutos da API OpenSky |
| `src/etl_spark/transform_data_spark.py` | Transforma os dados com PySpark |
| `src/etl_spark/load_data_spark.py` | Grava os dados no PostgreSQL via JDBC |
| `src/etl_spark/spark_session.py` | Cria a `SparkSession` |
| `src/etl_spark/schemas.py` | Define o schema usado na transformacao |
| `src/db/db_connections.py` | Centraliza configuracoes de conexao com PostgreSQL |
| `src/db/mongo_connections.py` | Centraliza persistencia raw no MongoDB |
| `docker-compose.yml` | Sobe PostgreSQL e cluster Spark |
| `Dockerfile.spark` | Imagem base do cluster Spark com dependencias do projeto |
| `requirements.spark.txt` | Dependencias minimas usadas pela imagem do cluster Spark |

## Requisitos

- Docker
- Docker Compose
- Python 3.11+ para execucao local

## Variaveis de Ambiente

Use o arquivo `.env` com base em `.env.example`.

Variaveis principais:

```env
ETL_DB_HOST=postgres-etl
ETL_DB_PORT=5432
ETL_DB_HOST_PORT=5439
ETL_DB_NAME=flight_data_db
ETL_DB_USER=postgres
ETL_DB_PASSWORD=root
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_HOST_PORT=27018
MONGO_DB=flight_data_raw
MONGO_USER=mongo_admin
MONGO_PASSWORD=mongo_pass
SPARK_MASTER_URL=spark://spark-master:7077
```

## Como Executar com Docker

Suba o ambiente:

```bash
docker compose up --build -d
```

Execute o ETL no cluster Spark:

```bash
docker compose exec etl-spark /opt/spark/bin/spark-submit /app/main_spark.py
```

Interfaces disponiveis:

- Spark Master UI: `http://localhost:8081`
- Spark Worker 1 UI: `http://localhost:8082`
- Spark Worker 2 UI: `http://localhost:8083`
- PostgreSQL no host: porta definida em `ETL_DB_HOST_PORT` (padrao `5439`)
- MongoDB no host: porta definida em `MONGO_HOST_PORT` (padrao `27018`)

## Como Acessar os Bancos

### PostgreSQL

O PostgreSQL armazena a camada tratada do pipeline, na tabela `flight_data`.

Dados de conexao no host:

```text
Host: 127.0.0.1
Port: 5439
Database: flight_data_db
Username: postgres
Password: root
```

Voce pode acessar com DBeaver, DataGrip, pgAdmin ou outro cliente SQL.

Exemplo de consulta:

```sql
SELECT *
FROM flight_data
ORDER BY ingested_at DESC
LIMIT 20;
```

Se preferir acessar pelo terminal do Docker:

```bash
docker compose exec postgres-etl psql -U postgres -d flight_data_db
```

### MongoDB

O MongoDB armazena a camada raw do pipeline.

Dados de conexao no host:

```text
Host: 127.0.0.1
Port: 27018
Username: mongo_admin
Password: mongo_pass
Authentication Database: admin
Database: flight_data_raw
```

Voce pode acessar com MongoDB Compass, extensao MongoDB do VS Code ou outro cliente compatível.

Connection string para o MongoDB Compass:

```text
mongodb://mongo_admin:mongo_pass@127.0.0.1:27018/?authSource=admin
```

Colecoes principais:

- `raw_flight_snapshots`
- `raw_flight_states`

Exemplos de consulta no MongoDB:

```javascript
db.raw_flight_snapshots.find().limit(5)
```

```javascript
db.raw_flight_states.find().limit(5)
```

Se preferir acessar pelo terminal do Docker:

```bash
docker compose exec mongodb mongosh -u mongo_admin -p mongo_pass --authenticationDatabase admin
```

## Como Executar Localmente

Instale as dependencias:

```bash
pip install -r requirements.txt
```

Execute:

```bash
python main_spark.py
```

## Tabela de Destino

O ETL grava os dados na tabela `flight_data`.

Ela é criada automaticamente com o schema abaixo:

```sql
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
```

## Observacoes

- Os dados brutos ficam em `data/raw` e no MongoDB.
- Os dados processados podem ser exportados para `data/processed`.
- A carga atual usa modo `append`.
- O projeto esta focado em arquitetura de dados com camada raw em MongoDB e camada tratada em PostgreSQL.
