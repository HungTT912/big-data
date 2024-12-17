from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, udf, sum, avg, count, row_number, lit, coalesce, stddev
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, BooleanType, StructType, StructField 
from graphframes import GraphFrame
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# Initialize Spark session
spark = SparkSession.builder.appName("EcommerceCustomerAnalytics").getOrCreate()

# Define the schema for e-commerce data
schema = StructType([
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

# Load e-commerce data from CSV (or Kafka)
data_df = spark.read.csv("hdfs://namenode:9000/data/E-commerceCustomerBehavior-Sheet1.csv", header=True, schema=schema)

# Task 1: Data Preprocessing and Customer Segmentation

# Convert satisfaction level to a numerical value for further analysis
data_df = data_df.withColumn(
    "satisfaction_score",
    when(col("satisfaction_level") == "High", 3)
    .when(col("satisfaction_level") == "Medium", 2)
    .when(col("satisfaction_level") == "Low", 1)
    .otherwise(0)
)

# Define a customer segmentation based on total spend and items purchased
def categorize_spend(spend, items_purchased):
    if spend > 1000 and items_purchased > 50:
        return "Premium"
    elif spend > 500:
        return "High"
    elif spend > 200:
        return "Medium"
    else:
        return "Low"

categorize_spend_udf = udf(categorize_spend, StringType())
data_df = data_df.withColumn("customer_segment", categorize_spend_udf("total_spend", "items_purchased"))

# Save preprocessed data for further tracking and analysis
data_df.write.csv("hdfs://namenode:9000/data/preprocessed_customer_data.csv", header=True)

# Task 2: Aggregation of Customer Metrics (Spend, Items, and Rating)

# Compute aggregations for each customer, including total spend, total items purchased, and average rating
agg_df = data_df.groupBy("customer_id").agg(
    sum("total_spend").alias("total_spend"),
    sum("items_purchased").alias("total_items_purchased"),
    avg("average_rating").alias("avg_rating"),
    count("customer_id").alias("purchase_count")
)

# Save aggregated metrics for tracking and reporting
agg_df.write.csv("hdfs://namenode:9000/data/aggregated_customer_data.csv", header=True)
print("DONE Task 1-2")
# Task 3: Customer Ranking and Anomaly Detection with Z-score

# Using a window function, rank customers by total spend and items purchased
window_spec = Window.orderBy(col("total_spend").desc(), col("items_purchased").desc())
ranked_customers = data_df.withColumn("rank", row_number().over(window_spec))

# Save ranked customers to track their performance
ranked_customers.write.csv("hdfs://namenode:9000/data/ranked_customers.csv", header=True)

# Z-score calculation for anomaly detection in total spend
mean_spend = agg_df.select(avg("total_spend")).first()[0]
std_spend = agg_df.select(stddev("total_spend")).first()[0]
z_score_udf = udf(lambda spend: (spend - mean_spend) / std_spend, FloatType())
agg_df = agg_df.withColumn("z_score", z_score_udf("total_spend"))

# Filter out anomalies: customers with Z-scores greater than 2 or less than -2
cleaned_df = agg_df.filter(abs(col("z_score")) <= 2)

# Save the cleaned data for further analysis
cleaned_df.write.csv("hdfs://namenode:9000/data/cleaned_customer_data.csv", header=True)

# Task 4: Pivoting and Unpivoting

# Pivot data to get the total spend by gender
pivot_df = data_df.groupBy("customer_id").pivot("gender").agg(sum("total_spend"))

# Save pivoted data for further analysis
pivot_df.write.csv("hdfs://namenode:9000/data/pivoted_customer_data.csv", header=True)

# Task 5: Graph Processing with GraphFrames (Customer-Product Relationship)

# Create edges for customer-product relationships based on purchases
customer_product_edges = data_df.select("customer_id", "product_id").distinct()
customer_vertices = data_df.select("customer_id").distinct().withColumnRenamed("customer_id", "id")
product_vertices = data_df.select("product_id").distinct().withColumnRenamed("product_id", "id")

# Create a graph with customers and products as vertices
g = GraphFrame(customer_vertices.union(product_vertices), customer_product_edges)

# Perform graph queries to find co-purchased products
co_purchased_products = g.find("(a)-[e]->(b);(a)-[e2]->(b2)").filter("e.product_id = e2.product_id")

# Save the co-purchased products for further analysis
co_purchased_products.write.csv("hdfs://namenode:9000/data/co_purchased_products.csv", header=True)

# Task 6: KMeans Clustering on Customer Data

# Use KMeans clustering to group customers based on their spending and items purchased
vector_assembler = VectorAssembler(inputCols=["total_spend", "items_purchased"], outputCol="features")
kmeans_df = vector_assembler.transform(data_df)
kmeans = KMeans().setK(4).setSeed(1)
model = kmeans.fit(kmeans_df)
clustered_df = model.transform(kmeans_df)

# Save the clustered customer data for tracking
clustered_df.write.csv("hdfs://namenode:9000/data/clustered_customer_data.csv", header=True)

# Task 7: Custom Business Logic Transformation (Customer Value Calculation)

# Custom UDF to calculate customer value based on age, total spend, and satisfaction score
def calculate_customer_value(age, total_spend, satisfaction_score):
    if age < 30:
        return total_spend * 0.5 + satisfaction_score * 10
    elif age < 60:
        return total_spend * 0.8 + satisfaction_score * 5
    else:
        return total_spend * 0.3 + satisfaction_score * 8

customer_value_udf = udf(calculate_customer_value, FloatType())
data_df = data_df.withColumn("customer_value", customer_value_udf("age", "total_spend", "satisfaction_score"))

# Save the final customer value data for tracking and analysis
data_df.write.csv("hdfs://namenode:9000/data/final_customer_value.csv", header=True)

# Task 8: Final Data Integration and Output

# Join all transformed data to create a comprehensive final dataset
final_df = data_df.join(agg_df, "customer_id").join(clustered_df, "customer_id").join(cleaned_df, "customer_id")

# Save the final integrated result for further analysis and reporting
final_df.write.csv("hdfs://namenode:9000/data/final_customer_analysis.csv", header=True)

# Performance Optimization: Caching frequently used datasets
final_df.cache()

# Query optimization: Check the execution plan for the final query
final_df.explain(True)

# End of pipeline
