# Roadmap PySpark do Projeto OpenSky

Este arquivo registra a evolucao arquitetural do projeto e os proximos movimentos esperados depois da implantacao da arquitetura medalhao.

## Estado Atual

O pipeline atual ja opera no modelo:

```text
OpenSky API
    |
    v
Bronze JSON
    |
    v
Silver Parquet
    |
    v
Gold Parquet
    |
    v
PostgreSQL
```

Com isso, o projeto ja cobre:

- extracao batch da OpenSky;
- organizacao em camadas Bronze, Silver e Gold;
- persistencia em arquivos no data lake local;
- publicacao das tabelas Gold no PostgreSQL;
- orquestracao inicial com Airflow;
- execucao em cluster Spark local com Docker.

## Objetivos da Proxima Fase

As proximas evolucoes naturais sao:

1. ampliar a camada Gold com mais visoes analiticas;
2. adicionar testes automatizados nas regras de transformacao;
3. criar pipeline de CI/CD;
4. evoluir de batch para streaming com Kafka e Structured Streaming;
5. considerar upsert e historizacao controlada no PostgreSQL.

## Estrutura Alvo de Evolucao

```text
src/
  db/
    db_connections.py

  etl_spark/
    extract_data_spark.py
    transform_data_spark.py
    load_data_spark.py
    schemas.py
    spark_session.py

  streaming/
    kafka_producer.py
    streaming_flight_etl.py

dags/
  flight_etl_dag.py
  flight_streaming_dag.py
```

## Proximo Marco Tecnico Recomendado

O proximo marco mais valioso e:

```text
Adicionar testes para Bronze -> Silver -> Gold
e automatizar validacao do projeto no CI.
```

Depois disso, a entrada de Kafka e streaming fica bem mais tranquila.
