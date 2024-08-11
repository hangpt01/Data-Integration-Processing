# running hadoop in Windows
hdfs namenode -format
# cd C/hadoop/sbin
start-dfs.cmd
start-yarn.cmd


python jsonl_to_csv.py
pip install -r requirements.txt
python split_data.py amazon

hdfs dfs -mkdir /user

hdfs dfs -mkdir /user/pth



spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 sent_data2lazadatopic.py

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 kafka_to_hdfs.py amazon

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 kafka_to_hdfs.py lazada


# hdfs dfs -put C:\Master-HUST\Data-Storage-Processing\BigData\merge_data\train_lazada.csv /user/pth
# hdfs dfs -put C:\Master-HUST\Data-Storage-Processing\BigData\merge_data\test_lazada.csv /user/pth

# hdfs dfs -put C:\Master-HUST\Data-Storage-Processing\BigData\merge_data\train_amazon.csv /user/pth
# hdfs dfs -put C:\Master-HUST\Data-Storage-Processing\BigData\merge_data\test_amazon.csv /user/pth

hdfs dfs -ls /user/pth


spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 ml_model.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 ml_model.py --model_name gbt


# modify feature_importance.py to remove 1 feature, then run these 4 commands again
hdfs dfs -rm -r /user/pth/model_pipeline_hdfs_amazon/model/linear_regression_model
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 feature_importance.py
hdfs dfs -rm -r /user/pth/model_pipeline_hdfs_amazon/model/gbt_model
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 feature_importance.py --model_name gbt


