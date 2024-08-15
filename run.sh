### create topic 
docker exec -it 1c6cfdaabc53 /bin/bash

kafka-topics.sh --create --topic rotten-tomatoes --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics.sh --create --topic mubi-sqlite --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181



## Run 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 submit_to_elasticsearch_rotten_tomatoes.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 submit_to_elasticsearch_mubi_sqlite.py

###
docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka" -q) kafka-topics --create --topic my_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1