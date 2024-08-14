from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import IntegerType

# StringIndexer to convert movie names to numerical indices
from pyspark.ml.feature import StringIndexer

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("ALSModel").getOrCreate()

# Step 2: Create the DataFrame with the provided data
# data = [
#     ("rw5704482", "After Life", 9, "2020-05-03", 2019),
#     ("rw5704483", "The Valhalla Murders", 6, "2020-05-03", 2019),
#     ("rw5704484", "Special OPS", 7, "2020-05-03", 2020),
#     ("rw5704485", "#BlackAF", 8, "2020-05-03", 2020),
#     ("rw5704487", "The Droving", 2, "2020-05-03", 2020),
#     ("rw5704488", "All About Eve", 10, "2020-05-03", 1950),
#     ("rw5704489", "Runaway Train", 7, "2020-05-03", 1985),
#     ("rw5704490", "Iron Fist", 9, "2020-05-03", 2017),
#     ("rw5704491", "The Half of It (I)", 4, "2020-05-03", 2020),
#     ("rw5704492", "This Is Us", 2, "2020-05-03", 2016),
#     ("rw5704494", "Closure (I)", 9, "2020-05-03", 2018),
#     ("rw5704493", "Unstoppable", 8, "2020-05-03", 2010),
#     ("rw5704496", "Dangerous Lies", None, "2020-05-03", 2020),
#     ("rw5704497", "Beastie Boys Story", 3, "2020-05-03", 2020),
#     ("rw5704500", "Ruben Brandt, Collector", 10, "2020-05-03", 2018),
#     ("rw5704499", "Some Kind of Hate", 7, "2020-05-03", 2015),
#     ("rw5704501", "Cube Zero", 10, "2020-05-03", 2004),
#     ("rw5704502", "Carne", 8, "2020-05-03", 1991),
#     ("rw5704504", "500 Days of Summer", 5, "2020-05-03", 2009),
#     ("rw5704503", "8½", 10, "2020-05-03", 1963)
# ]

data = [
    ("rw5704482", "After Life", 9, "2020-05-03", 2019),
    ("rw5704483", "The Valhalla Murders", 6, "2020-05-03", 2019),
    ("rw5704484", "Special OPS", 7, "2020-05-03", 2020),
    ("rw5704485", "#BlackAF", 8, "2020-05-03", 2020),
    ("rw5704487", "The Droving", 2, "2020-05-03", 2020),
    ("rw5704488", "All About Eve", 10, "2020-05-03", 1950),
    ("rw5704489", "Runaway Train", 7, "2020-05-03", 1985),
    ("rw5704490", "Iron Fist", 9, "2020-05-03", 2017),
    ("rw5704491", "The Half of It (I)", 4, "2020-05-03", 2020),
    ("rw5704492", "This Is Us", 2, "2020-05-03", 2016),
    ("rw5704494", "Closure (I)", 9, "2020-05-03", 2018),
    ("rw5704493", "Unstoppable", 8, "2020-05-03", 2010),
    ("rw5704496", "Dangerous Lies", None, "2020-05-03", 2020),
    ("rw5704497", "Beastie Boys Story", 3, "2020-05-03", 2020),
    ("rw5704500", "Ruben Brandt, Collector", 10, "2020-05-03", 2018),
    ("rw5704499", "Some Kind of Hate", 7, "2020-05-03", 2015),
    ("rw5704501", "Cube Zero", 10, "2020-05-03", 2004),
    ("rw5704502", "Carne", 8, "2020-05-03", 1991),
    ("rw5704504", "500 Days of Summer", 5, "2020-05-03", 2009),
    ("rw5704503", "8½", 10, "2020-05-03", 1963),
    
    # Duplicate entries
    ("rw5704505", "After Life", 8, "2020-05-04", 2019),
    ("rw5704506", "The Valhalla Murders", 7, "2020-05-04", 2019),
    ("rw5704507", "Special OPS", 6, "2020-05-04", 2020),
    ("rw5704508", "#BlackAF", 9, "2020-05-04", 2020),
    ("rw5704509", "The Droving", 3, "2020-05-04", 2020),
    ("rw5704510", "All About Eve", 9, "2020-05-04", 1950),
    ("rw5704511", "Runaway Train", 6, "2020-05-04", 1985),
    ("rw5704512", "Iron Fist", 10, "2020-05-04", 2017),
    ("rw5704513", "The Half of It (I)", 5, "2020-05-04", 2020),
    ("rw5704514", "This Is Us", 3, "2020-05-04", 2016),
    ("rw5704515", "Closure (I)", 8, "2020-05-04", 2018),
    ("rw5704516", "Unstoppable", 7, "2020-05-04", 2010),
    ("rw5704517", "Dangerous Lies", 4, "2020-05-04", 2020),
    ("rw5704518", "Beastie Boys Story", 2, "2020-05-04", 2020),
    ("rw5704519", "Ruben Brandt, Collector", 9, "2020-05-04", 2018),
    ("rw5704520", "Some Kind of Hate", 8, "2020-05-04", 2015),
    ("rw5704521", "Cube Zero", 9, "2020-05-04", 2004),
    ("rw5704522", "Carne", 7, "2020-05-04", 1991),
    ("rw5704523", "500 Days of Summer", 6, "2020-05-04", 2009),
    ("rw5704524", "8½", 9, "2020-05-04", 1963)
]


columns = ["review_id", "movie", "rating", "review_date", "release_time"]
df = spark.createDataFrame(data, columns)

# Step 3: Preprocess the data
# Remove rows where rating is missing
df = df.na.drop(subset=["rating"])

# Convert movie names to numeric IDs
df = df.withColumn("userId", col("review_id").substr(3, 2).cast(IntegerType()))
# df = df.withColumn("itemId", col("movie").cast("string"))
# indexer = StringIndexer(inputCol="itemId", outputCol="itemId_indexed")
indexer = StringIndexer(inputCol="movie", outputCol="itemId")

df = indexer.fit(df).transform(df)

# Rename columns for ALS compatibility
df = df.withColumn("rating", col("rating").cast(IntegerType()))
# df = df.withColumnRenamed("userId", "userId").withColumnRenamed("itemId_indexed", "itemId")

# Step 4: Split the data into training and test sets
total_count = df.count()
train_count = int(total_count * 0.8)
# Select the first 80% for training
training = df.limit(train_count)
# Select the remaining 20% for testing
test = df.subtract(training)

# (training, test) = df.randomSplit([0.8, 0.2])
# print(training.head(5))
# print(test.head(5))     # 2 items
# import pdb; pdb.set_trace()


# Step 5: Build and train the ALS model
als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="itemId", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(training)

# Step 6: Evaluate the model using RMSE
predictions = model.transform(test)
# print(predictions)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
if predictions.count() > 0:
    rmse = evaluator.evaluate(predictions)
    print(f"Root-mean-square error = {rmse}")
else:
    print("No predictions were generated. The predictions DataFrame is empty.")
rmse = evaluator.evaluate(predictions)
print(f"Root-mean-square error = {rmse}")

# Step 7: Generate top 3 movie recommendations for each user
userRecs = model.recommendForAllUsers(3)
userRecs.show(truncate=False)

# Step 8: Stop the Spark session
spark.stop()
