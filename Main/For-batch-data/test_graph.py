from pyspark.sql import SparkSession
from graphframes import GraphFrame

# Initialize Spark session with GraphFrames package
spark = SparkSession.builder \
    .appName("GraphFramesExample") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.0-spark3.0-s_2.12") \
    .getOrCreate()
# Create vertices DataFrame
vertices = spark.createDataFrame([
    ("1", "Alice"),
    ("2", "Bob"),
    ("3", "Charlie"),
    ("4", "David"),
    ("5", "Eve")
], ["id", "name"])

# Create edges DataFrame
edges = spark.createDataFrame([
    ("1", "2", "friend"),
    ("2", "3", "follow"),
    ("3", "4", "friend"),
    ("4", "5", "follow"),
    ("5", "1", "friend")
], ["src", "dst", "relationship"])

# Create the GraphFrame
g = GraphFrame(vertices, edges)

# Display the graph
g.vertices.show()
g.edges.show()

# Example: Running PageRank algorithm
# results = g.pageRank(resetProbability=0.15, maxIter=10)

# Show the PageRank results
# results.vertices.select("id", "pagerank").show()
print("--------------------------------------------------------------DONE----------------------------------")
# Stop the Spark session
spark.stop()
