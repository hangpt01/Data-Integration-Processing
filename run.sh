### create topic 
docker exec -it bdaf29d55c27 /bin/bash

kafka-topics.sh --create --topic rotten-tomatoes --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181


## Run 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 submit_to_elasticsearch_rotten_tomatoes.py



# IMDB
docker exec -it 327eec1e3d71 /bin/bash
kafka-topics.sh --create --topic imdb --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

docker exec -it 327eec1e3d71 /opt/kafka_2.13-2.8.1/bin/kafka-topics.sh --create --topic imdb --bootstrap-server localhost:9094
docker exec -it 327eec1e3d71 /opt/kafka_2.13-2.8.1/bin/kafka-topics.sh --list --bootstrap-server localhost:9094

docker exec -it 327eec1e3d71 /opt/kafka_2.13-2.8.1/bin/kafka-topics.sh --delete --topic amazon-products4 --bootstrap-server localhost:9094

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 send_data_of_imdb.py

docker exec -it 327eec1e3d71 /opt/kafka_2.13-2.8.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic imdb --from-beginning --max-messages 1


spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 test_elastic.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 submit_to_elasticsearch_imdb.py
