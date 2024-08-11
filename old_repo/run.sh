###
cd /opt/kafka-2.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties

cd /opt/kafka-2.8.0
bin/kafka-server-start.sh config/server.properties

### create a topic 
cd /opt/kafka-2.8.0
bin/kafka-topics.sh --create --topic lazada-products --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

## check list topic kafka 
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

## check topic 
bin/kafka-topics.sh --describe --topic streaming-data --bootstrap-server localhost:9092

## check message count 
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic streaming-data --time -1
## Run kibana
./bin/kibana


spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.0 test2.py

python spark_process.py

spark.driver.extraClassPath /path/to/scala-library-2.13.0.jar



## create topic 
docker exec -it docker-kafka-kafka1-1 /bin/bash
kafka-topics.sh --create --topic lazada-products --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics.sh --list --zookeeper zookeeper:2181
exit

docker exec -it docker-kafka-kafka2-1 /bin/bash
kafka-topics.sh --create --topic amazon-products3 --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics.sh --list --zookeeper zookeeper:2181
kafka-topics.sh --list --zookeeper zookeeper:2181