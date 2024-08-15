import csv
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml import Pipeline
import argparse
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import pyspark.sql.functions as F
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from datetime import datetime
from py4j.java_gateway import java_import
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Collaborative Filtering Model Pipeline Example")
    parser.add_argument('--save_dir', type=str, default='hdfs://localhost:9000/user/pth/model_pipeline_hdfs_movie', help='Directory to save model and pipeline')
    parser.add_argument('--mode', type=str, choices=['train', 'test', 'full'], default='full')
    return parser.parse_args()

class ALSModelPipeline():
    def __init__(self, save_dir, seed=52):
        self.save_dir = save_dir
        self.model_dir = f"{save_dir}/model"
        self.pipeline_dir = f"{save_dir}/pipeline_dir"
        self.model_name = "ALS"
        self.seed = seed
        self.pipeline_list = []
        self.feature_columns = []

    def init_data_pipeline(self):
        stages = []
        
        # Convert review_id (user) and movie (item) to numeric indices
        user_indexer = StringIndexer(inputCol="review_id", outputCol="user_index", handleInvalid="keep")
        movie_indexer = StringIndexer(inputCol="movie", outputCol="movie_index", handleInvalid="keep")
        stages += [user_indexer, movie_indexer]

        # Create pipeline
        pipeline = Pipeline(stages=stages)
        return pipeline

    def remove_nan_rows(self, df):
        cleaned_df = df.dropna()
        return cleaned_df

    def fit_transform(self, df):
        print("Removing nan...")
        df = self.remove_nan_rows(df)
        self.pipeline = self.init_data_pipeline()
        self.pipeline_model = self.pipeline.fit(df)
        print("Transform data...")
        transformed_data = self.pipeline_model.transform(df)
        # print(transformed_data)
        print(transformed_data.head())
        # import pdb; pdb.set_trace()
        return transformed_data

    # def init_model(self):
    #     print("------ Init ALS model ------")
    #     self.model = ALS(
    #         userCol="user_index",
    #         itemCol="movie_index",
    #         ratingCol="rating",
    #         predictionCol="rating_prediction",
    #         coldStartStrategy="drop",
    #         nonnegative=True,
    #         seed=self.seed
    #     )

    def init_model(self):
        print("------ Init ALS model ------")
        self.model = ALS(
            userCol="user_index",
            itemCol="movie_index",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True,
            seed=self.seed
        ).setPredictionCol("rating_prediction")


    def load_model(self):
        print("------ Load model ------")
        self.pipeline_model = PipelineModel.load(f"{self.pipeline_dir}/pipeline_model")
        self.model = ALSModel.load(f"{self.model_dir}/als_model")

    def hdfs_exists(self, path):
        sc = SparkSession.builder.getOrCreate().sparkContext
        java_import(sc._jvm, 'org.apache.hadoop.fs.FileSystem')
        java_import(sc._jvm, 'org.apache.hadoop.fs.Path')
        fs = sc._jvm.FileSystem.get(sc._jsc.hadoopConfiguration())
        return fs.exists(sc._jvm.Path(path))

    def train(self, df):
        pipeline_model_path = f"{self.pipeline_dir}/pipeline_model"
        model_path = f"{self.model_dir}/als_model"

        print("model_path", model_path, os.path.exists(model_path), self.hdfs_exists(model_path))                                          
        print("pipeline_model_path", pipeline_model_path, os.path.exists(pipeline_model_path), self.hdfs_exists(pipeline_model_path))       

        # Check if the model and pipeline model already exist in HDFS
        if self.hdfs_exists(pipeline_model_path) and self.hdfs_exists(model_path):
            print("Loading existing model and pipeline...")
            self.load_model()
        else:
            print("Training new model...")
            transformed_data = self.fit_transform(df)
            print("\n\nDONE DATA PROCESSING!!!!!!!\n\n")
            self.init_model()
            self.model = self.model.fit(transformed_data)
            self.pipeline_model.write().overwrite().save(pipeline_model_path)
            self.model.write().overwrite().save(model_path)
        return self.model

    def predict(self, df):
        pipeline_model_path = f"{self.pipeline_dir}/pipeline_model"
        model_path = f"{self.model_dir}/als_model"

        print("model_path", model_path, os.path.exists(model_path), self.hdfs_exists(model_path))                                          
        print("pipeline_model_path", pipeline_model_path, os.path.exists(pipeline_model_path), self.hdfs_exists(pipeline_model_path))       

        # Check if the model and pipeline model exist in HDFS
        if not self.hdfs_exists(pipeline_model_path) or not self.hdfs_exists(model_path):
            raise FileNotFoundError(f"Model path {model_path} or pipeline path {pipeline_model_path} does not exist. Please train the model first.")

        self.load_model()
        # print(df.head(5))
        # import pdb; pdb.set_trace()
        transformed_data = self.pipeline_model.transform(df)
        predictions = self.model.transform(transformed_data)
        return predictions


    def evaluate_model(self, predictions):
        # Check if the predictions DataFrame is empty
        if predictions.count() == 0:
            print("No predictions to evaluate. The DataFrame is empty.")
            return
        
        # Log the schema and some data for debugging
        predictions.printSchema()
        predictions.show(5)
        # import pdb; pdb.set_trace()
        
        evaluator_rmse = RegressionEvaluator(labelCol="rating", predictionCol="rating_prediction", metricName="rmse")
        evaluator_mae = RegressionEvaluator(labelCol="rating", predictionCol="rating_prediction", metricName="mae")
        evaluator_mse = RegressionEvaluator(labelCol="rating", predictionCol="rating_prediction", metricName="mse")

        rmse = evaluator_rmse.evaluate(predictions)
        mae = evaluator_mae.evaluate(predictions)
        mse = evaluator_mse.evaluate(predictions)

        print(f"RMSE: {rmse}")
        print(f"MAE: {mae}")
        print(f"MSE: {mse}")

        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if not os.path.exists("result"):
            os.makedirs("result")
        # Define the CSV file path
        csv_file_path = f"result/{self.model_name}.csv"
        
        # Write the metrics to the CSV file
        with open(csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            # writer.writerow([self.category_fts, self.text_fts, self.num_fts])
            writer.writerow([current_datetime, self.model_name, rmse, mae, mse])
        
        print(f"Metrics saved to {csv_file_path}")


if __name__ == "__main__":
    args = parse_args()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ALSModelPipelineExample") \
        .getOrCreate()

    # Define the schema for the input data
    schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("movie", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("review_date", StringType(), True),
        StructField("release_time", StringType(), True)
    ])    

    # HDFS file paths
    train_file_path = "hdfs://localhost:9000/user/pth/training_set.csv"
    test_file_path = "hdfs://localhost:9000/user/pth/testing_set.csv"

    # feature_dict = {
    #     "category": ["main_category", "store"],
    #     "text": ["name", "parent_asin"],
    #     "num": ["price", "rating_number"]
    # }

    # Read train CSV file from HDFS
    train_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(train_file_path)
    
    # print(train_df)
    # print(train_df.head(5))
    # import pdb; pdb.set_trace()

    # Read test CSV file from HDFS
    test_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(test_file_path)

    # Initialize ALSModelPipeline class
    save_dir = args.save_dir
    mode = args.mode
    pipeline = ALSModelPipeline(save_dir)

    if mode == 'train':
        # Train the model
        model = pipeline.train(train_df)
    elif mode == 'test':
        # Make predictions
        predictions = pipeline.predict(test_df)
        predictions.select("review_id", "movie", "rating", "rating_prediction").show()
        pipeline.evaluate_model(predictions)
    else:
        model = pipeline.train(train_df)
        predictions = pipeline.predict(test_df)
        predictions.select("review_id", "movie", "rating", "rating_prediction").show()
        pipeline.evaluate_model(predictions)

    # Stop Spark session
    spark.stop()
