# Flight Data ETL com PySpark e Arquitetura Medalhao

Projeto de engenharia de dados para coleta, tratamento e publicacao de dados de voos da API OpenSky Network usando **Python**, **PySpark**, **PostgreSQL**, **Airflow** e um **cluster Spark em Docker**.

O pipeline foi evoluido para uma arquitetura medalhao local:

1. **Bronze**: salva o snapshot bruto da OpenSky em JSON particionado por data e hora;
2. **Silver**: normaliza, tipa, limpa e deduplica os registros em Parquet;
3. **Gold**: gera datasets analiticos detalhados e agregados em Parquet;
4. **Serving**: publica as tabelas Gold no PostgreSQL via JDBC.

## Visao Geral

O projeto foi organizado para praticar um fluxo mais profissional de dados com separacao clara entre camadas.

- A extracao consulta a API `states/all` da OpenSky.
- A Bronze preserva o payload bruto para auditoria e reprocessamento.
- A Silver padroniza schema, converte timestamps, limpa campos textuais e remove duplicidades.
- A Gold entrega tabelas prontas para consumo analitico.
- O PostgreSQL atua como camada de serving para consultas e dashboards.
- O Airflow orquestra a execucao batch do pipeline.

## Arquitetura Atual

```text
OpenSky API
    |
    v
Bronze JSON em data/bronze
    |
    v
Silver Parquet em data/silver
    |
    v
Gold Parquet em data/gold
    |
    v
PostgreSQL (serving layer)
```

Com Docker Compose, o ambiente atual sobe:

- `postgres-etl`
- `spark-master`
- `spark-worker-1`
- `spark-worker-2`
- `etl-spark`
- `airflow`

## Estrutura do Projeto

```text
.
|-- config/
|   `-- settings.py
|-- dags/
|   `-- flight_etl_dag.py
|-- src/
|   |-- db/
|   |   `-- db_connections.py
|   |-- etl_spark/
|   |   |-- extract_data_spark.py
|   |   |-- load_data_spark.py
|   |   |-- schemas.py
|   |   |-- spark_session.py
|   |   `-- transform_data_spark.py
|   `-- utils/
|       `-- logger.py
|-- .env.example
|-- docker-compose.yml
|-- Dockerfile.airflow
|-- Dockerfile.spark
|-- main_spark.py
|-- requirements.spark.txt
`-- README.md
```

## Principais Arquivos

| Caminho | Descricao |
|---|---|
| `main_spark.py` | Orquestra o pipeline Bronze -> Silver -> Gold -> PostgreSQL |
| `src/etl_spark/extract_data_spark.py` | Faz a extracao da API e grava a Bronze em JSON |
| `src/etl_spark/transform_data_spark.py` | Gera a Silver e as tabelas Gold em Parquet |
| `src/etl_spark/load_data_spark.py` | Publica as tabelas Gold no PostgreSQL via JDBC |
| `src/etl_spark/schemas.py` | Centraliza schemas e listas de colunas das camadas |
| `src/db/db_connections.py` | Centraliza conexao JDBC/psycopg2 e DDL das tabelas Gold |
| `dags/flight_etl_dag.py` | DAG batch do Airflow para orquestrar o pipeline |
| `config/settings.py` | Define diretarios do data lake local e paths particionados |

## Requisitos

- Docker
- Docker Compose
- Python 3.11+ para execucao local

## Variaveis de Ambiente

Use o arquivo `.env` com base em [.env.example](</c:/Users/tiago/OneDrive/Área de Trabalho/Pratica eng/.env.example:1>).

Variaveis principais:

```env
ETL_DB_HOST=postgres-etl
ETL_DB_PORT=5432
ETL_DB_HOST_PORT=5439
ETL_DB_NAME=flight_data_db
ETL_DB_USER=postgres
ETL_DB_PASSWORD=root
SPARK_MASTER_URL=spark://spark-master:7077
AIRFLOW_USER=ROOT
AIRFLOW_PASSWORD=ROOT
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
- Airflow UI: `http://localhost:8085`
- PostgreSQL no host: porta definida em `ETL_DB_HOST_PORT` (padrao `5439`)

## Como Acessar o PostgreSQL

O PostgreSQL armazena a camada Gold publicada para consumo.

Dados de conexao no host:

```text
Host: 127.0.0.1
Port: 5439
Database: flight_data_db
Username: postgres
Password: root
```

Voce pode acessar com DBeaver, DataGrip, pgAdmin ou outro cliente SQL.

Tabelas publicadas:

- `gold_flight_positions`
- `gold_country_metrics`

Exemplos de consulta:

```sql
SELECT *
FROM gold_flight_positions
ORDER BY ingested_at DESC
LIMIT 20;
```

```sql
SELECT *
FROM gold_country_metrics
ORDER BY snapshot_timestamp DESC, total_flights DESC
LIMIT 20;
```

Se preferir acessar pelo terminal do Docker:

```bash
docker compose exec postgres-etl psql -U postgres -d flight_data_db
```

## Camadas do Data Lake

### Bronze

Armazena o payload bruto da API em JSON particionado:

```text
data/bronze/opensky_api_states/year=YYYY/month=MM/day=DD/hour=HH/
```

### Silver

Armazena registros curados de posicoes de voo em Parquet particionado por `snapshot_date` e `snapshot_hour`.

Dataset:

```text
data/silver/flight_positions_curated/
```

### Gold

Armazena datasets analiticos em Parquet:

- `data/gold/flight_positions/`
- `data/gold/country_metrics/`

## Tabelas Gold no PostgreSQL

### `gold_flight_positions`

Tabela detalhada com historico curado de posicoes de voo, pronta para consumo analitico.

### `gold_country_metrics`

Tabela agregada por pais e snapshot, com metricas como:

- total de voos
- voos em solo
- voos em voo
- velocidade media
- altitude media e maxima

## Como Executar Localmente

Instale as dependencias:

```bash
pip install -r requirements.txt
```

Execute:

```bash
python main_spark.py
```

## Orquestracao com Airflow

A DAG [flight_etl_dag.py](</c:/Users/tiago/OneDrive/Área de Trabalho/Pratica eng/dags/flight_etl_dag.py:1>) faz:

1. validacao de dependencias de infraestrutura;
2. disparo do pipeline medalhao completo;
3. execucao batch horaria.

Nome da DAG:

```text
flight_data_medallion_etl
```

## Observacoes

- A Bronze preserva o dado original para reprocessamento.
- A Silver concentra regras de limpeza, tipagem e deduplicacao.
- A Gold entrega datasets prontos para analise e serving.
- O MongoDB foi removido do projeto porque o data lake local em arquivos atende melhor ao modelo medalhao com Spark.
