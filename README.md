# Realtime-Data-Streaming
This repository is a data engineering project that will be focusing on build a data streaming component by using docker and other tools. The process will be:

1. Extract data from API `randomuser.me`
2. Automate and streaming data to **Kafka** by **Airflow**
3. Streaming **Kafka** to Database **Cassandra** with **Apache Spark**

## Prerequisite
1. Docker 
2. Python
3. Apache Spark version 3.4.1
4. Sacala version 2.12.x
5. Java 11.x.x


## Commands
start docker container
```
docker compose up -d 
```

After we have start our container, we now have  to turn on the airflow DAGs automation at `localhost:8080`
 
After we start
```
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  spark_stream.py

```

You can access into  Cassandra to do querying by do: 
```
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
```