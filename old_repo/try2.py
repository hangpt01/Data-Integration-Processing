from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, lower
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ReadFromKafka") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .getOrCreate()

# Set log level to WARN to suppress INFO messages
spark.sparkContext.setLogLevel("WARN")

# List of keywords to search for
keywords = ["sony", "iphone", "samsung", "nokia"]

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "lazada-products") \
    .option("startingOffsets", "earliest") \
    .load()

# Select the key and value columns and cast them to strings
kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define the schema for the JSON data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("discount", StringType(), True),
    StructField("star", DoubleType(), True),
    StructField("url", StringType(), True),
    StructField("n_fb", IntegerType(), True)
])

# Parse the JSON data
parsed_df = kafka_df.withColumn("value", from_json(col("value"), schema))

# Select individual fields
processed_df = parsed_df.select(col("key"), col("value.*"))

# Create a DataFrame with keyword and a column for occurrence count
keyword_expr = [expr(f"CASE WHEN lower(name) LIKE '%{keyword.lower()}%' THEN 1 ELSE 0 END AS {keyword}") for keyword in keywords]

# Add the keyword presence columns to the DataFrame
keyword_counts_df = processed_df.select(col("key"), *keyword_expr)

# Aggregate the counts for each keyword
keyword_counts = keyword_counts_df.groupBy("key").agg(*[expr(f"sum({keyword}) as {keyword}") for keyword in keywords])

# Stack the DataFrame to have keyword counts in rows
stack_expr = "stack({0}, {1}) as (keyword, count)".format(
    len(keywords),
    ", ".join(["'{}', {}".format(keyword, keyword) for keyword in keywords])
)

stacked_df = keyword_counts.selectExpr(stack_expr)

# Explode the stacked DataFrame
melted_df = stacked_df.selectExpr("keyword", "count")

# Define function to write data to Elasticsearch
def write_data_to_elasticsearch(df, epoch_id):
    es = Elasticsearch(["http://localhost:9200"],
                       basic_auth=('elastic', 'yihaDK9_Es7ASWQI53bL'))

    index_name = "count_index2"

    # Convert the DataFrame to a Pandas DataFrame
    pandas_df = df.toPandas()

    # Prepare bulk update data for Elasticsearch
    actions = []
    for index, row in pandas_df.iterrows():
        action = {
            "_op_type": "update",
            "_index": index_name,
            "_id": row['keyword'],
            "doc": {
                "keyword": row['keyword'],
                "count": row['count']
            },
            "doc_as_upsert": True
        }
        actions.append(action)

    # Bulk update to Elasticsearch
    bulk(es, actions)

# Write the aggregated counts to Elasticsearch with a processing interval of 10 seconds

es_query = melted_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_data_to_elasticsearch) \
    .option("checkpointLocation", "checkpoint_dir") \
    .trigger(processingTime="10 seconds") \
    .start()

es_query.awaitTermination()