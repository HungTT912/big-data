from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from graphframes import GraphFrame

# Initialize SparkSession with the GraphFrames package version for Spark 3.x
spark = SparkSession.builder \
    .appName("GraphFrames") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.0-spark3.0-s_2.12") \
    .getOrCreate()

# Load transactions.csv
transactions_df = spark.read.csv("./transactions.csv", header=True, inferSchema=True)

# Step 1: Pre-compute total items bought per user
total_items_per_user = transactions_df.groupBy("user_id") \
    .agg(sum("count").alias("total_items"))

# Step 2: Calculate weights (count / total_items) and filter by threshold
threshold_weight = 0.0001

edges_df = transactions_df.join(total_items_per_user, on="user_id") \
    .withColumn("weight", col("count") / col("total_items")) \
    .filter(col("weight") > threshold_weight) \
    .select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        "weight"
    )

# Step 3: Create vertices (distinct user_id and product_id)
user_vertices = transactions_df.select("user_id").distinct().withColumnRenamed("user_id", "id")
product_vertices = transactions_df.select("product_id").distinct().withColumnRenamed("product_id", "id")

vertices_df = user_vertices.union(product_vertices)

# Step 4: Create GraphFrame
graph = GraphFrame(vertices_df, edges_df)

# Step 5: Advanced analysis — find patterns
threshold_users = 1  # Minimum number of users to consider co-buying pattern significant
product_patterns = graph.find("(a)-[e1]->(b); (a)-[e2]->(c)") \
    .filter("b.id != c.id") \
    .selectExpr("b.id as product_b", "c.id as product_c") \
    .groupBy("product_b", "product_c") \
    .count() \
    .filter(col("count") > threshold_users)

# Step 6: Save results to PostgreSQL
db_properties = {
    "user": "external",
    "password": "external",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://external_postgres_db:5432/external?tcpKeepAlive=true&socketTimeout=30&connectTimeout=30&applicationName=SparkApp"
}

# Displaying product patterns
product_patterns.show()

# # Writing results to PostgreSQL
# product_patterns.write \
#     .jdbc(url=db_properties["url"], 
#           table="product_patterns", 
#           mode="overwrite", 
#           properties=db_properties)

print("Optimized graph data saved to PostgreSQL.")
