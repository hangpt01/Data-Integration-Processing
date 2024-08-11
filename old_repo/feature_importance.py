import csv
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.ml import Pipeline
import argparse
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor, LinearRegressionModel, RandomForestRegressionModel, GBTRegressionModel
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import PipelineModel
import pyspark.sql.functions as F
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import random
from datetime import datetime
from py4j.java_gateway import java_import
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Model Pipeline Example")
    parser.add_argument('--model_name', type=str, default='lr', help='Model name: lr, random_forest, gbt')
    parser.add_argument('--save_dir', type=str, default='hdfs://localhost:9000/user/pth/model_pipeline_hdfs_amazon', help='Directory to save model and pipeline')
    parser.add_argument('--mode', type=str, choices=['train', 'test', 'full'], default='full')
    return parser.parse_args()

class ModelPipeLine():
    def __init__(self, save_dir, model_name, feature_dict, seed=52, text_process_method='tf-idf'):
        self.save_dir = save_dir
        self.model_dir = f"{save_dir}/model"
        self.pipeline_dir = f"{save_dir}/pipeline_dir"
        self.model_name = model_name
        self.n_gram = 1
        self.category_fts = feature_dict['category']
        self.text_fts = feature_dict['text']
        self.num_fts = feature_dict['num']
        self.seed = seed
        self.text_process_method = text_process_method
        self.pipeline_list = []
        self.feature_columns = []

    def fillna_numerical(self, df, strategy='constant'):
        if strategy == 'mean':
            for col in self.num_fts:
                mean_value = df.select(F.mean(col)).collect()[0][0]
                df = df.fillna(mean_value, subset=[col])
        elif strategy == 'median':
            for col in self.num_fts:
                median_value = df.approxQuantile(col, [0.5], 0.001)[0]
                df = df.fillna(median_value, subset=[col])
        elif strategy == 'constant':
            df = df.fillna(0, subset=self.num_fts)
        else:
            raise ValueError(f"Unknown strategy: {strategy}. Supported strategies are 'mean', 'median', or 'constant'.")
        return df
    
    def replace_empty_with_unknown(self, df):
        """
        Replace empty strings with 'Unknown' for specified columns in a DataFrame.
        
        :param df: The input DataFrame.
        :param columns: A list of columns to apply the replacement on.
        :return: DataFrame with replacements applied.
        """
        for col_name in self.category_fts:
            # df = df.withColumn(col_name, when(col(col_name) == "", "Unknown").otherwise(col(col_name)))
            df = df.withColumn(
                col_name,
                when(col(col_name).isNull() | (col(col_name) == ""), "Unknown").otherwise(col(col_name))
            )
        return df

    def init_data_pipeline(self):
        stages = []

        # Text processing pipeline
        for col in self.text_fts:
            tokenizer = Tokenizer(inputCol=col, outputCol=col+"_words")
            hashingTF = HashingTF(inputCol=col+"_words", outputCol=col+"_rawFeatures", numFeatures=500)
            idf = IDF(inputCol=col+"_rawFeatures", outputCol=col+"_features")
            stages += [tokenizer, hashingTF, idf]

        # Numeric processing pipeline
        for col in self.num_fts:
            # print(f"Processing numeric column: {col}")
            assembler = VectorAssembler(inputCols=[col], outputCol=col+"_vec", handleInvalid="skip")
            scaler = MinMaxScaler(inputCol=col+"_vec", outputCol=col+"_scaled")
            stages += [assembler, scaler]

        # # Categorical processing pipeline
        for col in self.category_fts:
            # indexer = StringIndexer(inputCol=col, outputCol=col+"_indexed", handleInvalid="keep")
            indexer = StringIndexer(inputCol=col, outputCol=col+"_indexed", handleInvalid="keep")
            encoder = OneHotEncoder(inputCol=col+"_indexed", outputCol=col+"_encoded")
            stages += [indexer, encoder]

        # Assemble all processed columns into a single feature vector
        assembler_inputs = [col+"_features" for col in self.text_fts] + [col+"_scaled" for col in self.num_fts] + [col+"_encoded" for col in self.category_fts]
        # assembler_inputs = [col+"_features" for col in self.text_fts] + [col+"_scaled" for col in self.num_fts]
        # print(f"Assembler inputs: {assembler_inputs}")
        assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
        stages += [assembler]
        # print(stages)
        # Create pipeline
        pipeline = Pipeline(stages=stages)
        # import pdb; pdb.set_trace()

        return pipeline

    def fit_transform(self, df):
        df = self.fillna_numerical(df)
        df = self.replace_empty_with_unknown(df)
        self.pipeline = self.init_data_pipeline()
        # df.show(5)
        # df.printSchema()
        # columns = df.columns
        # print(columns)
        # import pdb; pdb.set_trace()
        self.pipeline_model = self.pipeline.fit(df)
        transformed_data = self.pipeline_model.transform(df)
        transformed_data.select('features').show(5)
        return transformed_data

    def init_model(self):
        print("------ Init model ------")
        if self.model_name == 'random_forest':
            self.model = RandomForestRegressor(featuresCol="features", labelCol="rating", predictionCol='rating_prediction', numTrees=100, seed=self.seed)
        elif self.model_name == 'lr':
            self.model = LinearRegression(featuresCol="features", labelCol="rating", 
                                          predictionCol='rating_prediction')
        elif self.model_name == 'gbt':
            self.model = GBTRegressor(featuresCol="features", labelCol='rating', 
                                      predictionCol='rating_prediction')
        else:
            raise ValueError(f"Unsupported model_name: {self.model_name}. Supported models are 'random_forest', 'lr', 'gbt'.")

    def load_model(self):
        print("------ Load model")
        self.pipeline_model = PipelineModel.load(f"{self.pipeline_dir}/pipeline_model")
        if self.model_name == 'lr':
            self.model = LinearRegressionModel.load(f"{self.model_dir}/linear_regression_model")
        elif self.model_name == 'random_forest':
            self.model = RandomForestRegressionModel.load(f"{self.model_dir}/random_forest_model")
        elif self.model_name == 'gbt':
            self.model = GBTRegressionModel.load(f"{self.model_dir}/gbt_model")
        else:
            raise ValueError(f"Unsupported model_name: {self.model_name}. Supported models are 'lr', 'random_forest', 'gbt'.")

    def hdfs_exists(self, path):
        sc = SparkSession.builder.getOrCreate().sparkContext
        java_import(sc._jvm, 'org.apache.hadoop.fs.FileSystem')
        java_import(sc._jvm, 'org.apache.hadoop.fs.Path')
        fs = sc._jvm.FileSystem.get(sc._jsc.hadoopConfiguration())
        return fs.exists(sc._jvm.Path(path))

    def train(self, df):
        pipeline_model_path = f"{self.pipeline_dir}/pipeline_model"
        if self.model_name == 'lr':
            model_path = f"{self.model_dir}/linear_regression_model"
        elif self.model_name == 'random_forest':
            model_path = f"{self.model_dir}/random_forest_model"
        elif self.model_name == 'gbt':
            model_path = f"{self.model_dir}/gbt_model"
        else:
            raise ValueError(f"Unsupported model_name: {self.model_name}. Supported models are 'lr', 'random_forest', 'gbt'.")

        print("model_path", model_path, os.path.exists(model_path), self.hdfs_exists(model_path))                                           # model_pipeline/model/linear_regression_model, False, True
        print("pipeline_model_path", pipeline_model_path, os.path.exists(pipeline_model_path), self.hdfs_exists(pipeline_model_path))       # model_pipeline/pipeline_dir/pipeline_model, False, True

        # Check if the model and pipeline model already exist in HDFS
        if self.hdfs_exists(pipeline_model_path) and self.hdfs_exists(model_path):
            print("Loading existing model and pipeline...")
            self.load_model()
        else:
            print("Training new model...")
            transformed_data = self.fit_transform(df)
            self.init_model()
            self.model = self.model.fit(transformed_data)
            self.pipeline_model.write().overwrite().save(pipeline_model_path)
            self.model.write().overwrite().save(model_path)
        return self.model

    def predict(self, df):
        pipeline_model_path = f"{self.pipeline_dir}/pipeline_model"
        if self.model_name == 'lr':
            model_path = f"{self.model_dir}/linear_regression_model"
        elif self.model_name == 'random_forest':
            model_path = f"{self.model_dir}/random_forest_model"
        elif self.model_name == 'gbt':
            model_path = f"{self.model_dir}/gbt_model"
        else:
            raise ValueError(f"Unsupported model_name: {self.model_name}. Supported models are 'lr', 'random_forest', 'gbt'.")

        print("model_path", model_path, os.path.exists(model_path), self.hdfs_exists(model_path))                                           # model_pipeline/model/linear_regression_model, False, True
        print("pipeline_model_path", pipeline_model_path, os.path.exists(pipeline_model_path), self.hdfs_exists(pipeline_model_path))       # model_pipeline/pipeline_dir/pipeline_model, False, True

        # Check if the model and pipeline model exist in HDFS
        if not self.hdfs_exists(pipeline_model_path) or not self.hdfs_exists(model_path):
            raise FileNotFoundError(f"Model path {model_path} or pipeline path {pipeline_model_path} does not exist. Please train the model first.")

        self.load_model()
        transformed_data = self.pipeline_model.transform(df)
        predictions = self.model.transform(transformed_data)
        return predictions
    
    def evaluate_model(self, predictions):
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
        
        # Define the CSV file path
        csv_file_path = f"result/{self.model_name}.csv"
        
        # Write the metrics to the CSV file
        with open(csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([self.category_fts, self.text_fts, self.num_fts])
            writer.writerow([current_datetime, self.model_name, rmse, mae, mse])
        
        print(f"Metrics saved to {csv_file_path}")

if __name__ == "__main__":
    args = parse_args()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ModelPipelineExample") \
        .getOrCreate()

    def df_to_list_with_header(df):
        # Collect the data from the DataFrame
        data = df.collect()
        # Get the column names
        header = df.columns
        # Convert rows to list of lists
        rows_as_list = [header] + [[str(cell) if cell is not None else '' for cell in row] for row in data]
        return rows_as_list

    # HDFS file paths
    train_file_path = "hdfs://localhost:9000/user/pth/train_amazon.csv"
    test_file_path = "hdfs://localhost:9000/user/pth/test_amazon.csv"

    # Function to remove ambiguous Unicode characters
    def remove_ambiguous_unicode(df, string_columns):
        for column in string_columns:
            df = df.withColumn(column, F.regexp_replace(F.col(column), "[^\\x00-\\x7F]", ""))
        return df

    # Read train CSV file from HDFS
    train_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("escape", "\"") \
        .load(train_file_path)

    # Read test CSV file from HDFS
    test_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("escape", "\"") \
        .load(test_file_path)

    # Read CSV files into lists
    train_list = df_to_list_with_header(train_df)
    test_list = df_to_list_with_header(test_df)


    # Extract header and data
    train_header = train_list[0]
    train_data = train_list[1:]
    test_header = test_list[0]
    test_data = test_list[1:]

    # Define the schema excluding the specified fields
    schema = StructType([
        StructField("main_category", StringType(), True),
        StructField("store", StringType(), True),
        StructField("title", StringType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("price", StringType(), True),
        StructField("rating_number", StringType(), True),
        StructField("average_rating", StringType(), True)
    ])    

    train_df = spark.createDataFrame(train_data, schema=schema)
    test_df = spark.createDataFrame(test_data, schema=schema) 
    
    train_df = train_df.withColumnRenamed('average_rating', 'rating')
    test_df = test_df.withColumnRenamed('average_rating', 'rating')

    train_df = train_df.withColumnRenamed('title', 'name')
    test_df = test_df.withColumnRenamed('title', 'name')
    
    # feature_dict = {
    #     "category": ["main_category", "store"],
    #     "text": ["name", "parent_asin"],
    #     "num": ["price", "rating_number"]
    # }

    # Modify only feature dict, each time remove 1 feature
    feature_dict = {
        "category": ["main_category", "store"],
        "text": ["name", "parent_asin"],
        "num": ["rating_number"]
    }

    # Remove ambiguous Unicode characters from string columns
    string_columns = feature_dict["category"] + feature_dict["text"]
    train_df = remove_ambiguous_unicode(train_df, string_columns)
    test_df = remove_ambiguous_unicode(test_df, string_columns)


    # Infer schema for numeric columns
    numeric_columns = ["price", "rating", "rating_number"]
    train_df = train_df.select([F.col(c).cast(DoubleType()) if c in numeric_columns else F.col(c) for c in train_df.columns])
    test_df = test_df.select([F.col(c).cast(DoubleType()) if c in numeric_columns else F.col(c) for c in test_df.columns])


    # Initialize ModelPipeLine class
    save_dir = args.save_dir
    model_name = args.model_name  # Can be 'lr', 'random_forest', or 'gbt'
    mode = args.mode
    pipeline = ModelPipeLine(save_dir, model_name, feature_dict)

    if mode == 'train':
        # Train the model
        model = pipeline.train(train_df)
    elif mode == 'test':
        # Make predictions
        predictions = pipeline.predict(test_df)
        predictions.select("name", "rating", "rating_prediction").show()
        pipeline.evaluate_model(predictions)
    else:
        model = pipeline.train(train_df)
        predictions = pipeline.predict(test_df)
        predictions.select("name", "rating", "rating_prediction").show()
        pipeline.evaluate_model(predictions)

    # Stop Spark session
    spark.stop()