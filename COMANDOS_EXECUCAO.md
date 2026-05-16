# Comandos para Rodar o Projeto

Este arquivo mostra a ordem recomendada para executar o pipeline medalhao corretamente.

## 1. Criar o arquivo `.env`

Use o `.env.example` como base.

Exemplo:

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

## 2. Subir o ambiente com Docker

Na raiz do projeto:

```bash
docker compose down -v
docker compose up --build -d
```

Esse comando sobe:

- PostgreSQL
- Spark Master
- Spark Worker 1
- Spark Worker 2
- container `etl-spark`
- Airflow

Observacoes:

- o build do cluster Spark usa `requirements.spark.txt`, com apenas as dependencias necessarias para o pipeline;
- o PostgreSQL continua usando a porta `5432` dentro do Docker, mas fica publicado na porta `5439` na sua maquina;
- o data lake local fica montado no proprio workspace, nas pastas `data/bronze`, `data/silver` e `data/gold`.

## 3. Verificar se os containers subiram

```bash
docker compose ps
```

## 4. Verificar a interface do cluster Spark

Abra no navegador:

```text
http://localhost:8081
```

Se tudo estiver certo, o Master deve mostrar os workers conectados.

## 5. Rodar o pipeline medalhao no cluster Spark

```bash
docker compose exec etl-spark /opt/spark/bin/spark-submit /app/main_spark.py
```

Esse comando executa:

1. extracao da API OpenSky;
2. gravacao do snapshot bruto na Bronze;
3. transformacao da Bronze para a Silver;
4. geracao das tabelas Gold em Parquet;
5. publicacao das tabelas Gold no PostgreSQL.

## 6. Ver logs do ETL, se precisar

```bash
docker compose logs -f etl-spark
```

## 7. Ver logs do Spark Master, se precisar

```bash
docker compose logs -f spark-master
```

## 8. Abrir o Airflow, se quiser acompanhar a DAG

```text
http://localhost:8085
```

## 9. Parar o ambiente

```bash
docker compose down
```

## 10. Parar e remover volumes do banco, se quiser resetar tudo

```bash
docker compose down -v
```

Use esse comando apenas se quiser apagar os dados persistidos do PostgreSQL.

## Fluxo resumido

```bash
docker compose up --build -d
docker compose ps
docker compose exec etl-spark /opt/spark/bin/spark-submit /app/main_spark.py
docker compose down
```
