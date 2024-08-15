### create topic 
docker exec -it 1c6cfdaabc53 /bin/bash

kafka-topics.sh --create --topic rotten-tomatoes --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics.sh --create --topic mubi-sqlite --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181



## Run 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 submit_to_elasticsearch_rotten_tomatoes.py

<<<<<<< HEAD
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 submit_to_elasticsearch_mubi_sqlite.py

###
docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka" -q) kafka-topics --create --topic my_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
=======


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
>>>>>>> 3681336d1a036a8ebb4bcc81702b7d57d395b5e9
