spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 try2.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 try2.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 run2.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 phone_run.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.13:8.2.0 amazon_run.py


spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.13:8.6.2 run2.py

spark-submit --master yarn --deploy-mode cluster save_to_hdfs.py


spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 kafka_to_hdfs.py
