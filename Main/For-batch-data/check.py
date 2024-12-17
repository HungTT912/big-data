from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import matplotlib.pyplot as plt

''' 
 Comprehensive E-Commerce Customer Behavior Analysis
 Q1 : 	Can we segment customers based on their demographic information (Age, Gender, City) 
 and shopping behaviors (Total Spend, Number of Items Purchased, Membership Type)
'''
# Initializing Spark Session
spark = SparkSession.builder.appName("CustomerSegmentation").getOrCreate()

# Loading The Dataset
customer_df = spark.read.csv("hdfs://namenode:9000/data/E-commerceCustomerBehavior-Sheet1.csv", header=True, inferSchema=True)

# Checking the number of rows in the DataFrame
row_count = customer_df.count()
print(f"The dataset contains {row_count} rows.")
import time
time.sleep(1000100)
