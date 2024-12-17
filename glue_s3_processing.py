import boto3
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from io import StringIO
import pandas as pd

# Initialize PySpark session
spark = SparkSession.builder \
    .appName("Local AWS Glue Simulation") \
    .getOrCreate()

# Use boto3 to read CSV file from a public S3 bucket
s3 = boto3.client('s3')
bucket = 'bigdata-ecommerce'
key = 's3://bigdata-ecommerce/E-commerceCustomerBehavior.csv'

response = s3.get_object(Bucket=bucket, Key=key)
content = response['Body'].read().decode('utf-8')

# Convert the CSV content to a Pandas DataFrame
data = pd.read_csv(StringIO(content))

# Convert the Pandas DataFrame to a Spark DataFrame
df = spark.createDataFrame(data)

# Filter customers with age < 30 and count them
filtered_data = df.filter(df.age < 30)
count_age_under_30 = filtered_data.count()

# Output the result
print(f"Number of customers with age < 30: {count_age_under_30}")

# Optionally save the result
filtered_data.write.csv("s3://bigdata-ecommerce/customers_under_30.csv", mode="overwrite")