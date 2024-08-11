from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType
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
                   basic_auth=('elastic', 'yihaDK9_Es7ASWQI53bL'))

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
                    "store": {"type": "text"},
                    "rating_number": {"type": "float"},
                    "title": {"type": "text"},
                    "parent_asin": {"type": "keyword"},
                    "price": {"type": "double"},
                    "main_category": {"type": "keyword"},
                    "average_rating": {"type": "double"}
                }
            }
        }, ignore=400)
        logger.info(f"Created index with mapping: {index_name}")

# Ensure the index is created
index_name = "amazon_data4"
create_index_with_mapping(index_name)

topic_name = "amazon-products4"

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

# Define the schema for the JSON data based on keys_to_keep
schema = StructType([
    StructField("store", StringType(), True),
    StructField("rating_number", FloatType(), True),
    StructField("title", StringType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("main_category", StringType(), True),
    StructField("average_rating", DoubleType(), True)
])

# Parse the JSON data
parsed_df = kafka_df.withColumn("value", from_json(col("value"), schema))

processed_df = parsed_df.select(
    col("key"),
    col("value.store"),
    col("value.rating_number"),
    col("value.title"),
    col("value.parent_asin"),
    col("value.price"),
    col("value.main_category"),
    col("value.average_rating"),
)

# Ensure each field in keys_to_keep has a non-null value
# keys_to_keep = {'store', 'rating_number', 'title', 'parent_asin', 'price', 'main_category', 'average_rating'}
# for key in keys_to_keep:
#     parsed_df = parsed_df.filter(col(f"value.{key}").isNotNull())

# Add a unique 'id' column to the DataFrame using a UUID
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
processed_df = processed_df.withColumn("id", uuid_udf())

# Define function to write data to Elasticsearch
def write_data_to_elasticsearch(df, epoch_id):
    es = Elasticsearch(["http://localhost:9200"],
                       basic_auth=('elastic', 'yihaDK9_Es7ASWQI53bL'))

    index_name = "amazon_data4"

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
                "store": row['store'],
                "rating_number": row['rating_number'],
                "title": row['title'],
                "parent_asin": row['parent_asin'],
                "price": row['price'],
                "main_category": row['main_category'],
                "average_rating": row['average_rating']
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