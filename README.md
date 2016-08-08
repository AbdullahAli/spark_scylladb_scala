# SCALA

```
sbt assembly
```

# DOCKER
```
docker pull scylladb/scylla
docker run -p 127.0.0.1:9042:9042 -i -t scylladb/scylla
cqlsh
CREATE KEYSPACE spark_example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE spark_example;
CREATE TABLE json_test (uuid TEXT PRIMARY KEY, json TEXT);
DESCRIBE TABLES;
```

# SPARK
```
Download from: http://spark.apache.org/downloads.html (I used 1.5.1)
unzip it
cd into it
./bin/spark-submit --executor-memory 4G --driver-memory 4G --properties-file path/to/project/spark-scylla.conf --class ScyllaSparkExampleSimple path/to/project/target/scala-2.10/scylla-spark-example-simple-assembly-1.0.jar
```