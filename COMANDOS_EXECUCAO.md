# Comandos para Rodar o Projeto

Este arquivo mostra a ordem completa para executar o pipeline medalhao via Docker.

## 1. Criar o arquivo `.env`

Use o `.env.example` como base.

Se quiser copiar rapidamente:

```bash
cp .env.example .env
```

Exemplo de conteudo:

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
AIRFLOW_FIRSTNAME=ROOT
AIRFLOW_LASTNAME=ROOT
AIRFLOW_EMAIL=root@example.com
```

## 2. Limpar o ambiente anterior, se quiser reiniciar do zero

```bash
docker compose down -v
```

Esse comando remove containers, rede e volume do PostgreSQL.

## 3. Subir o ambiente com Docker

Na raiz do projeto:

```bash
docker compose up --build -d
```

Esse comando sobe:

- PostgreSQL
- Spark Master
- Spark Worker 1
- Spark Worker 2
- container `etl-spark`
- Airflow

## 4. Verificar se os containers subiram

```bash
docker compose ps
```

## 5. Validar a configuracao final do Compose, se quiser conferir

```bash
docker compose config
```

## 6. Acompanhar a inicializacao dos servicos

```bash
docker compose logs -f postgres-etl
```

Em outro terminal, se quiser:

```bash
docker compose logs -f spark-master
```

```bash
docker compose logs -f airflow
```

## 7. Verificar a interface do cluster Spark

Abra no navegador:

```text
http://localhost:8081
```

Se tudo estiver certo, o Master deve mostrar os workers conectados.

## 8. Verificar a interface do Airflow

Abra no navegador:

```text
http://localhost:8085
```

Credenciais padrao:

```text
Usuario: ROOT
Senha: ROOT
```

Observacao:

- o container do Airflow sincroniza automaticamente o usuario admin com os valores do `.env`;
- se o usuario informado em `AIRFLOW_USER` ja existir, a senha sera redefinida com `AIRFLOW_PASSWORD` durante a inicializacao;
- se nao existir, o usuario sera criado automaticamente.

## 9. Executar o pipeline medalhao no cluster Spark

```bash
docker compose exec etl-spark /opt/spark/bin/spark-submit /app/main_spark.py
```

Esse comando executa:

1. extracao da API OpenSky;
2. gravacao do snapshot bruto na Bronze;
3. transformacao da Bronze para a Silver;
4. geracao das tabelas Gold em Parquet;
5. publicacao das tabelas Gold no PostgreSQL com upsert.

## 10. Ver os logs do ETL durante ou depois da execucao

```bash
docker compose logs -f etl-spark
```

## 11. Conferir os arquivos gerados no data lake local

```bash
ls data
```

```bash
ls data/bronze
```

```bash
ls data/silver
```

```bash
ls data/gold
```

## 12. Acessar o PostgreSQL pelo terminal

```bash
docker compose exec postgres-etl psql -U postgres -d flight_data_db
```

## 13. Consultar as tabelas Gold no PostgreSQL

Dentro do `psql`:

```sql
\dt
```

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

## 14. Executar o pipeline novamente para validar o upsert

```bash
docker compose exec etl-spark /opt/spark/bin/spark-submit /app/main_spark.py
```

Depois, acompanhe os logs:

```bash
docker compose logs -f etl-spark
```

Voce deve ver mensagens com quantidade de registros inseridos e atualizados por tabela Gold.

## 15. Rodar pela DAG do Airflow, se quiser testar a orquestracao

Depois de abrir o Airflow em `http://localhost:8085`:

1. procure a DAG `flight_data_medallion_etl`;
2. habilite a DAG;
3. clique em trigger para executar manualmente.

## 16. Parar o ambiente

```bash
docker compose down
```

## 17. Parar e remover volumes do banco, se quiser resetar tudo

```bash
docker compose down -v
```

Use esse comando apenas se quiser apagar os dados persistidos do PostgreSQL.

## Fluxo resumido

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
docker compose exec etl-spark /opt/spark/bin/spark-submit /app/main_spark.py
docker compose exec postgres-etl psql -U postgres -d flight_data_db
docker compose down
```
