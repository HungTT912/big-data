from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

class KafkaToHDFS:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, hdfs_output_path):
        # Create Spark session with the kafka package included
        self.spark = SparkSession.builder \
            .appName("KafkaToHDFS") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
            .getOrCreate()
        
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.hdfs_output_path = hdfs_output_path

    def define_schema(self):
        return StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("membership_type", StringType(), True),
            StructField("total_spend", FloatType(), True),
            StructField("items_purchased", IntegerType(), True),
            StructField("average_rating", FloatType(), True),
            StructField("discount_applied", BooleanType(), True),
            StructField("days_since_last_purchase", IntegerType(), True),
            StructField("satisfaction_level", StringType(), True)
        ])

    def start_streaming(self):
        # Read data from Kafka topic
        kafka_stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .load()

        # Convert the value column from Kafka into a string
        kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

        # Parse CSV data using the defined schema
        parsed_data_df = kafka_stream_df.select(from_json(col("value"), self.define_schema()).alias("data")).select("data.*")

        # Write the data to HDFS as a Parquet file
        query = parsed_data_df.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", self.hdfs_output_path) \
            .option("checkpointLocation", "/tmp/checkpoints") \
            .start()

        query.awaitTermination()


if __name__ == "__main__":
    kafka_bootstrap_servers = "127.0.0.1:19092"  # Kafka server address
    kafka_topic = "kafka-topic"  # Kafka topic name
    hdfs_output_path = "hdfs://namenode:9000/data/"  # HDFS output path

    # Create an instance of KafkaToHDFS and start streaming
    kafka_to_hdfs = KafkaToHDFS(kafka_bootstrap_servers, kafka_topic, hdfs_output_path)
    kafka_to_hdfs.start_streaming()
