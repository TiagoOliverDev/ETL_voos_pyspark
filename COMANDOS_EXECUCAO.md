# Comandos para Rodar o Projeto

Este arquivo mostra a ordem recomendada para executar o projeto corretamente.

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
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_HOST_PORT=27018
MONGO_DB=flight_data_raw
MONGO_USER=mongo_admin
MONGO_PASSWORD=mongo_pass
SPARK_MASTER_URL=spark://spark-master:7077
```

## 2. Subir o ambiente com Docker

Na raiz do projeto:

```bash
docker compose down -v
docker compose up --build -d
```

Esse comando sobe:

- PostgreSQL
- MongoDB
- Spark Master
- Spark Worker 1
- Spark Worker 2
- container `etl-spark`

Observacao:

- o build do cluster Spark usa `requirements.spark.txt`, que contem apenas as dependencias necessarias para executar o ETL dentro do Docker.
- o PostgreSQL continua usando a porta `5432` dentro do Docker, mas fica publicado na porta `5439` na sua maquina.
- o MongoDB continua usando a porta `27017` dentro do Docker, mas fica publicado na porta `27018` na sua maquina.

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

## 5. Rodar o ETL no cluster Spark

```bash
docker compose exec etl-spark /opt/spark/bin/spark-submit /app/main_spark.py
```

Esse comando executa:

1. extracao da API OpenSky;
2. transformacao com PySpark;
3. criacao automatica da tabela `flight_data`, se necessario;
4. carga no PostgreSQL via JDBC.

## 6. Ver logs do ETL, se precisar

```bash
docker compose logs -f etl-spark
```

## 7. Ver logs do Spark Master, se precisar

```bash
docker compose logs -f spark-master
```

## 8. Parar o ambiente

```bash
docker compose down
```

## 9. Parar e remover volumes do banco, se quiser resetar tudo

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
