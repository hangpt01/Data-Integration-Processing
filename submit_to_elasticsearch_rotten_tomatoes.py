from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType
import uuid
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Initialize Spark Session with required packages
spark = SparkSession.builder \
    .appName("ReadFromKafka") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .getOrCreate()

# Set log level to WARN to suppress INFO messages
spark.sparkContext.setLogLevel("WARN")

# Elasticsearch configuration
es = Elasticsearch(["http://localhost:9200"],
                   basic_auth=('elastic', 'changeme'))

# Initialize logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

# Function to create index with mapping if it doesn't exist
def create_index_with_mapping(index_name):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "movieId": {"type": "keyword"},
                    "rating": {"type": "float"},
                    "reviewId": {"type": "keyword"},
                    "score": {"type": "double"},
                    "userId": {"type": "keyword"},
                    "movieYear": {"type": "integer"},  # Convert movieYear to integer
                    "movieTitle": {"type": "text"}
                }
            }
        }, ignore=400)
        logger.info(f"Created index with mapping: {index_name}")

# Ensure the index is created
index_name = "movie_reviews"
create_index_with_mapping(index_name)

topic_name = "rotten-tomatoes"

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Select the key and value columns and cast them to strings
kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define the updated schema for the JSON data
schema = StructType([
    StructField("movieId", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("reviewId", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("userId", StringType(), True),
    StructField("movieYear", StringType(), True),  # Initially as StringType to convert later
    StructField("movieTitle", StringType(), True)
])

# Parse the JSON data and include the relevant fields
parsed_df = kafka_df.withColumn("value", from_json(col("value"), schema))

# Convert movieYear to integer and select relevant fields
processed_df = parsed_df.select(
    col("key"),
    col("value.movieId"),
    col("value.rating"),
    col("value.reviewId"),
    col("value.score"),
    col("value.userId"),
    col("value.movieYear").cast(IntegerType()).alias("movieYear"),  # Convert movieYear to integer
    col("value.movieTitle")
)

# Add a unique 'id' column to the DataFrame using a UUID
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
processed_df = processed_df.withColumn("id", uuid_udf())

# Define function to write data to Elasticsearch
def write_data_to_elasticsearch(df, epoch_id):
    es = Elasticsearch(["http://localhost:9200"],
                       basic_auth=('elastic', 'changeme'))

    index_name = "movie_reviews"

    # Convert the DataFrame to a Pandas DataFrame
    pandas_df = df.toPandas()

    # Prepare bulk data for Elasticsearch
    actions = []
    for index, row in pandas_df.iterrows():
        action = {
            "_op_type": "index",
            "_index": index_name,
            "_id": row['id'],
            "_source": {
                "movieId": row['movieId'],
                "rating": row['rating'],
                "reviewId": row['reviewId'],
                "score": row['score'],
                "userId": row['userId'],
                "movieYear": row['movieYear'],      # Integer type
                "movieTitle": row['movieTitle']
            }
        }
        actions.append(action)

    # Bulk index to Elasticsearch with error handling
    try:
        bulk(es, actions)
        logger.info(f"Successfully indexed batch with epoch_id: {epoch_id}")
    except Exception as e:
        logger.error(f"Error indexing batch with epoch_id: {epoch_id} - {e}")

# Write the processed data to Elasticsearch with a processing interval of 10 seconds
es_query = processed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_data_to_elasticsearch) \
    .option("checkpointLocation", "checkpoint_dir") \
    .trigger(processingTime="10 seconds") \
    .start()

# Await termination for the Elasticsearch query
es_query.awaitTermination()