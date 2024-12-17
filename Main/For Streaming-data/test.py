from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import json

# Cấu hình Kafka Consumer
consumer = KafkaConsumer(
    'kafka-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='test-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Tạo SparkSession
# spark = SparkSession.builder.appName("EcommerceDataAnalysis") \
#     .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
#     .getOrCreate()
spark = SparkSession.builder \
    .appName("EcommerceDataAnalysis") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.default.parallelism", "5") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()
# Giảm số lượng partition
spark.conf.set("spark.sql.shuffle.partitions", "5")

# Cấu hình kết nối PostgreSQL
postgres_properties = {
    "user": "external",
    "password": "external",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://external_postgres_db:5432/external?tcpKeepAlive=true&socketTimeout=30&connectTimeout=30&applicationName=SparkApp"
}

print("Listening for messages from Kafka and writing to PostgreSQL...")

# Bộ đệm dữ liệu
buffer = []
batch_size = 10

try:
    for message in consumer:
        print(f"Processing message: {message.value}")
        
        # Parse CSV message
        data = message.value.split(",")
        buffer.append(data)

        if len(buffer) >= batch_size:
            schema = ["customer_id", "gender", "age", "city", "membership_type", "total_spend",
                      "items_purchased", "average_rating", "discount_applied", "days_since_last_purchase", "satisfaction_level"]
            batch_df = spark.createDataFrame(buffer, schema)
            batch_df = batch_df.withColumn("customer_id", col("customer_id").cast("int")) \
                   .withColumn("age", col("age").cast("int")) \
                   .withColumn("total_spend", col("total_spend").cast("double")) \
                   .withColumn("items_purchased", col("items_purchased").cast("int")) \
                   .withColumn("average_rating", col("average_rating").cast("double")) \
                   .withColumn("discount_applied", col("discount_applied").cast("boolean")) \
                   .withColumn("days_since_last_purchase", col("days_since_last_purchase").cast("int"))
            # *** Bổ sung phân tích dữ liệu ***
            # 1. Tạo cột `age_group`
            ecommerce_df_age_groups = batch_df.withColumn(
                "age_group",
                expr("""
                    CASE
                        WHEN age < 35 THEN 'Under 35'
                        WHEN age >= 35 AND age <= 50 THEN 'Between 35-50'
                        ELSE 'Over 50'
                    END
                """)
            )

            # 2. Tổng hợp dữ liệu theo phân khúc
            customer_segments = ecommerce_df_age_groups.groupBy("membership_type", "age_group", "city").agg({
                "total_spend": "mean",
                "items_purchased": "sum"
            })

            # 3. Tổng hợp dữ liệu theo thành phố
            city_segments = ecommerce_df_age_groups.groupBy("city").agg({
                "total_spend": "mean",
                "items_purchased": "sum"
            })

            # 4. Lọc khách hàng "at-risk"
            at_risk_customers = batch_df.filter(
                (batch_df["days_since_last_purchase"] > 30) &
                (batch_df["satisfaction_level"] == "Unsatisfied")
            ).select(
                "customer_id", "city", "total_spend", "days_since_last_purchase", "satisfaction_level"
            )

            # Ghi kết quả vào PostgreSQL
            try:
                # Ghi batch gốc
                batch_df.coalesce(5).write.jdbc(
                    url=postgres_properties["url"], 
                    table="ecommerce_data", 
                    mode="append", 
                    properties=postgres_properties
                )

                # Ghi dữ liệu phân khúc khách hàng
                customer_segments.write.jdbc(
                    url=postgres_properties["url"], 
                    table="customer_segments", 
                    mode="append", 
                    properties=postgres_properties
                )

                # Ghi dữ liệu phân khúc thành phố
                city_segments.write.jdbc(
                    url=postgres_properties["url"], 
                    table="city_segments", 
                    mode="append", 
                    properties=postgres_properties
                )

                # Ghi dữ liệu khách hàng "at-risk"
                at_risk_customers.write.jdbc(
                    url=postgres_properties["url"], 
                    table="at_risk_customers", 
                    mode="append", 
                    properties=postgres_properties
                )
                spark.catalog.clearCache()
                print("Batch data and analysis results inserted into PostgreSQL.")
            except Exception as e:
                print(f"Error inserting into PostgreSQL: {e}")
            finally:
                buffer = []

except KeyboardInterrupt:
    print("Consumer stopped.")
except Exception as e:
    print(f"Error: {e}")
finally:
    if buffer:
        try:
            schema = ["customer_id", "gender", "age", "city", "membership_type", "total_spend",
                      "items_purchased", "average_rating", "discount_applied", "days_since_last_purchase", "satisfaction_level"]
            final_df = spark.createDataFrame(buffer, schema)
            final_df.coalesce(5).write.jdbc(
                url=postgres_properties["url"], 
                table="ecommerce_data", 
                mode="append", 
                properties=postgres_properties
            )
            print("Remaining data inserted into PostgreSQL.")
        except Exception as e:
            print(f"Error inserting remaining data into PostgreSQL: {e}")
    consumer.close()
