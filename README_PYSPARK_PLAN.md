# Plano de Implementacao PySpark no Projeto OpenSky

Este documento descreve um plano para evoluir o projeto atual de ETL de dados de voos da OpenSky Network, adicionando **PySpark** e usando **Docker Compose** para simular um ambiente distribuido com Spark Master, Spark Workers, Kafka, Airflow e PostgreSQL.

O objetivo principal e usar o projeto como laboratorio pratico para estudar engenharia de dados com Spark, mantendo o pipeline atual funcionando enquanto novas partes sao adicionadas de forma incremental.

---

## Objetivo

Implementar processamento batch e streaming com PySpark para:

- transformar dados de voos usando Spark DataFrames;
- simular um cluster Spark local com Docker;
- gravar dados tratados no PostgreSQL via JDBC;
- integrar jobs Spark ao Airflow;
- consumir dados em tempo real do Kafka com Spark Structured Streaming.

---

## Arquitetura Desejada

```text
[ OpenSky Network API ]
           |
           v
[ Batch Extract Python ]        [ Kafka Producer ]
           |                            |
           v                            v
[ Arquivos JSON/Raw ]           [ Kafka Topic: flight-data-raw ]
           |                            |
           v                            v
[ Spark Batch Job / PySpark DataFrame ] [ Spark Structured Streaming ]
           |                            |
           +-------------+--------------+
                         |
                         v
              [ Spark Cluster ]
              [ master + workers ]
                         |
                         v
              [ PostgreSQL / flight_data ]
```

---

## Fase 1: Manter o ETL Atual e Criar uma Versao Spark

O projeto ja possui uma estrutura ETL tradicional:

```text
src/etl/extract_data.py
src/etl/transform_data.py
src/etl/load_data.py
```

A primeira etapa e manter essa versao funcionando e criar uma implementacao paralela usando PySpark:

```text
src/spark/
  jobs/
    batch_flight_etl.py
  schemas/
    flight_schema.py
  utils/
    spark_session.py
```

Nesta fase, o foco sera:

- criar uma `SparkSession`;
- definir um schema explicito para os dados da OpenSky;
- transformar dados usando `select`, `withColumn`, `cast` e funcoes do `pyspark.sql.functions`;
- converter timestamps Unix para formato de data/hora;
- limpar campos como `callsign`;
- remover registros invalidos;
- preparar os dados para carga no PostgreSQL.

---

## Fase 2: Criar Cluster Spark com Docker Compose

Adicionar ao `docker-compose.yml` os servicos:

```text
spark-master
spark-worker-1
spark-worker-2
postgres
airflow
kafka
```

Exemplo de topologia:

```text
spark-master
  |
  +-- spark-worker-1
  |
  +-- spark-worker-2
```

Portas sugeridas:

| Servico | Porta | Uso |
|---|---:|---|
| Airflow | 8080 | Interface web do Airflow |
| Spark Master UI | 8081 | Monitoramento do cluster Spark |
| Spark Worker 1 UI | 8082 | Monitoramento do worker 1 |
| Spark Worker 2 UI | 8083 | Monitoramento do worker 2 |
| Spark Master | 7077 | Submissao de jobs Spark |
| Kafka | 9092 | Broker Kafka |
| PostgreSQL | 5432 | Banco de dados |

Com isso, sera possivel executar jobs apontando para:

```bash
spark://spark-master:7077
```

---

## Fase 3: Implementar Job Spark Batch

Criar o arquivo:

```text
src/spark/jobs/batch_flight_etl.py
```

Fluxo do job:

```text
JSON bruto ou dados extraidos
        |
        v
Spark DataFrame
        |
        v
Transformacao e limpeza
        |
        v
PostgreSQL via JDBC
```

Conceitos praticados:

- `SparkSession`;
- `StructType` e `StructField`;
- leitura de JSON;
- transformacoes com DataFrame API;
- conversao de tipos;
- escrita JDBC;
- particionamento;
- logs e analise pela Spark UI.

Exemplo de execucao local dentro do ambiente Docker:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/app/src/spark/jobs/batch_flight_etl.py
```

---

## Fase 4: Gravar no PostgreSQL com JDBC

O Spark deve gravar os dados tratados na tabela `flight_data`.

Tabela esperada:

```sql
CREATE TABLE IF NOT EXISTS flight_data (
    icao24 VARCHAR(10) NOT NULL,
    callsign VARCHAR(10),
    origin_country VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    heading DOUBLE PRECISION,
    baro_altitude DOUBLE PRECISION,
    geo_altitude DOUBLE PRECISION,
    on_ground BOOLEAN,
    time_position TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_contact TIMESTAMP WITHOUT TIME ZONE,
    record_timestamp TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (icao24, time_position)
);
```

Configuracoes importantes:

- adicionar o driver JDBC do PostgreSQL ao container Spark;
- configurar URL JDBC;
- passar usuario e senha via variaveis de ambiente;
- testar primeiro com modo `append`;
- depois evoluir para estrategia de `upsert`.

---

## Fase 5: Integrar PySpark com Airflow

Depois que o job Spark batch funcionar isoladamente, integrar ao Airflow.

Modelo inicial da DAG:

```text
extract_open_sky_data
        |
        v
run_spark_batch_job
        |
        v
validate_postgres_load
```

O Airflow pode executar o Spark com:

```bash
spark-submit --master spark://spark-master:7077 src/spark/jobs/batch_flight_etl.py
```

Conceitos praticados:

- orquestracao de jobs Spark;
- separacao entre extracao e transformacao;
- logs no Airflow;
- dependencias entre tarefas;
- retries e agendamento.

---

## Fase 6: Evoluir para Spark Structured Streaming

Depois do batch estar consolidado, implementar streaming com Kafka.

Fluxo esperado:

```text
OpenSky API
    |
    v
Kafka Producer
    |
    v
Kafka Topic: flight-data-raw
    |
    v
Spark Structured Streaming
    |
    v
PostgreSQL
```

Arquivo sugerido:

```text
src/spark/jobs/streaming_flight_etl.py
```

Conceitos praticados:

- leitura de Kafka com Spark;
- parsing de JSON com schema;
- `from_json`;
- `explode`, caso os dados venham no formato original da OpenSky;
- `foreachBatch` para gravar no PostgreSQL;
- checkpointing;
- tolerancia a falhas;
- processamento continuo.

---

## Estrutura Final Sugerida

```text
src/
  etl/
    extract_data.py
    transform_data.py
    load_data.py

  spark/
    jobs/
      batch_flight_etl.py
      streaming_flight_etl.py
    schemas/
      flight_schema.py
    utils/
      spark_session.py

  streaming/
    kafka_producer.py

  db/
    db_connections.py

dags/
  flight_data_pipeline.py
  spark_flight_data_pipeline.py
```

---

## Ordem Recomendada de Implementacao

1. Criar estrutura `src/spark/`.
2. Criar `spark_session.py`.
3. Criar schema Spark dos dados da OpenSky.
4. Criar job batch lendo JSON local.
5. Rodar job Spark em modo local.
6. Adicionar Spark Master e Workers ao Docker Compose.
7. Rodar job usando `spark://spark-master:7077`.
8. Gravar dados no PostgreSQL via JDBC.
9. Integrar job Spark ao Airflow.
10. Criar producer Kafka para dados OpenSky.
11. Criar job Spark Structured Streaming.
12. Adicionar checkpointing e estrategia de upsert.

---

## Primeiro Marco de Sucesso

O primeiro objetivo pratico deve ser simples:

```text
Ler um JSON local com PySpark,
transformar os campos principais
e gravar no PostgreSQL usando o cluster Spark em Docker.
```

Quando isso estiver funcionando, o projeto ja tera uma base solida para evoluir para Airflow e streaming.

---

## Comandos Esperados

Subir ambiente:

```bash
docker-compose up --build
```

Executar job Spark batch:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/app/src/spark/jobs/batch_flight_etl.py
```

Executar streaming:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/app/src/spark/jobs/streaming_flight_etl.py
```

---

## Observacoes

- Comecar pelo batch e mais simples do que comecar direto pelo streaming.
- Kafka e Spark Streaming devem entrar depois que o Spark DataFrame API estiver bem entendido.
- O PostgreSQL pode ser usado primeiro com `append`; depois, vale implementar `upsert`.
- A Spark UI sera essencial para entender jobs, stages, tarefas e performance.
- O projeto atual nao precisa ser descartado: a versao PySpark pode conviver com a versao pandas.
