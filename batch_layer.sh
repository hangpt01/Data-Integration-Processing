# running hadoop in Windows
hdfs namenode -format
# cd C/hadoop/sbin
start-dfs.cmd
start-yarn.cmd


python imdb_to_csv.py
python filter_movies.py

hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/pth


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 sent_data2lazadatopic.py
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 kafka_to_hdfs.py amazon
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 kafka_to_hdfs.py lazada

# hdfs dfs -put C:\Data-Integration-Processing\Project\Data-Integration-Processing\Dataset\IMDB\csv\part-01.csv /user/pth
# hdfs dfs -put C:\Data-Integration-Processing\Project\Data-Integration-Processing\Dataset\IMDB\csv\part-02.csv /user/pth

# hdfs dfs -put C:\Data-Integration-Processing\Project\Data-Integration-Processing\Dataset\IMDB\csv\part-01-temp.csv /user/pth
# hdfs dfs -put C:\Data-Integration-Processing\Project\Data-Integration-Processing\Dataset\IMDB\csv\part-02-temp.csv /user/pth

hdfs dfs -put C:\Data-Integration-Processing\Project\Data-Integration-Processing\Dataset\IMDB\csv\training_set.csv /user/pth
hdfs dfs -put C:\Data-Integration-Processing\Project\Data-Integration-Processing\Dataset\IMDB\csv\testing_set.csv /user/pth


hdfs dfs -ls /user/pth

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 test_recommend.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 recommender.py
hdfs dfs -rm -r /user/pth/model_pipeline_hdfs_movie



# # modify feature_importance.py to remove 1 feature, then run these 4 commands again
# hdfs dfs -rm -r /user/pth/model_pipeline_hdfs_amazon/model/linear_regression_model
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 feature_importance.py
# hdfs dfs -rm -r /user/pth/model_pipeline_hdfs_amazon/model/gbt_model
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 feature_importance.py --model_name gbt
