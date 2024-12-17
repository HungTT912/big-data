from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, from_csv
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from kafka import KafkaConsumer, KafkaProducer
from joblib import load
import logging
import json
from sklearn.ensemble import IsolationForest
from pyspark.ml import PipelineModel

import numpy as np


class EcommerceDataAnalysis:
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.spark = SparkSession.builder.appName("EcommerceDataAnalysis") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
            .getOrCreate()
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

        self.pipelineModel = PipelineModel.load("./preprocessing_pipeline_model")
        self.clf = load('./isolation_forest.joblib')

    # Function to define Schema of CSV publisher
    def define_schema(self):
        return "customer_id INT, gender STRING, age INT, city STRING, membership_type STRING, total_spend FLOAT, items_purchased INT, average_rating FLOAT, discount_applied BOOLEAN, days_since_last_purchase INT, satisfaction_level STRING"

    # Function to preprocess data
    def preprocess_data(self, df):
        # Drop the "Customer ID" column
        df = df.drop("Customer ID")

        # Handling missing values
        df = df.na.drop()

        # Apply the pre-processing pipeline
        pre_processed_df = self.pipelineModel.transform(df)

        # # Write the pre_processed_df to the console sink
        # query = df \
        #     .writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .start()

        # # Wait for the query to terminate
        # query.awaitTermination()

        return pre_processed_df


    def detect_anomalies(self, df, batch_id):
        print("Schema of batch DataFrame:")
        df.printSchema()

        # Print the content of the DataFrame for debugging
        print("Content of batch DataFrame:")
        df.show()

        # Convert DataFrame to Pandas for local anomaly detection
        pandas_df = df.toPandas()

        # Assuming scaled_features_array is a global variable or loaded from a file
        scaled_features_array = np.array(df.select('scaled_features').rdd.map(lambda x: x[0]).collect())

        # Check if scaled_features_array is not empty
        if scaled_features_array.size > 0:
            # Predict anomalies using the Isolation Forest model
            predictions = self.clf.predict(scaled_features_array)

            # Add predictions as a new column to the DataFrame
            pandas_df["anomaly"] = predictions
            # pandas_df.printSchema()

            # Convert Pandas DataFrame back to Spark DataFrame
            anomaly_df = self.spark.createDataFrame(pandas_df)

            # Filter only anomalies
            anomaly_df = anomaly_df.filter(col("anomaly") == -1)
            # Example: Print the batch DataFrame for demonstration purposes
            print("Batch DataFrame:")
            anomaly_df.show()

            # Write the anomaly results to a sink (e.g., console, Kafka, etc.)
            anomaly_df \
                .write \
                .format("console") \
                .save()
        else:
            print("scaled_features_array is empty. Skipping anomaly detection.")

    def start_streaming(self):
        # Read data from Kafka topic
        kafka_stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .load()

        # Convert the value column from Kafka into a string
        kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

        # Parse JSON data using the defined schema
        parsed_data_df = kafka_stream_df.select(from_csv(col("value"), self.define_schema()).alias("data")).select(
            "data.*")

        # Preprocess the data
        pre_processed_df = self.preprocess_data(parsed_data_df)

        # Apply anomaly detection within the streaming query
        anomaly_df = pre_processed_df \
            .writeStream \
            .foreachBatch(self.detect_anomalies) \
            .start()

        # Wait for the streaming query to terminate
        anomaly_df.awaitTermination()


if __name__ == "__main__":
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "kafka-topic"

    ecommerce_analysis = EcommerceDataAnalysis(kafka_bootstrap_servers, kafka_topic)
    ecommerce_analysis.start_streaming()

