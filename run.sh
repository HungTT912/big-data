docker exec -it namenode bin/bash

docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/E-commerceCustomerBehavior-Sheet1.csv namenode:/data/E-commerceCustomerBehavior-Sheet1.csv
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/products.csv namenode:/data/products.csv
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/transactions.csv namenode:/data/transactions.csv


docker exec -it namenode hdfs dfs -put /data/E-commerceCustomerBehavior-Sheet1.csv /E-commerceCustomerBehavior-Sheet1.csv
docker exec -it namenode hdfs dfs -put /data/products.csv /data/products.csv
docker exec -it namenode hdfs dfs -put /data/transactions.csv /data/transactions.csv


docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For-batch-data/Customer_Analysis.py spark-master:Analysis.py 
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For-batch-data/check.py spark-master:check.py 
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For-batch-data/advanced_analysis.py spark-master:advanced_analysis.py 
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For-batch-data/graph_analysis.py spark-master:graph_analysis.py 
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For-batch-data/Customer_clustering_model.py spark-master:Customer_clustering_model.py 

advanced_analysis
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For\ Streaming-data/kafka_consumer_to_hdfs.py spark-master:kafka_consumer_to_hdfs.py

# docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/E-commerceCustomerBehavior-Sheet1.csv spark-master:E-commerceCustomerBehavior-Sheet1.csv
# docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/products.csv spark-master:products.csv
# docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/E-commerceCustomerBehavior-Sheet1.csv spark-master:E-commerceCustomerBehavior-Sheet1.csv


docker exec -it spark-master bash 
cd ..
opt/spark/bin/spark-submit --master spark://989eccb1080f:7077  Analysis.py -d
opt/spark/bin/spark-submit --master local[4]  check.py -d
opt/spark/bin/spark-submit --master local[4]  advanced_analysis.py -d
opt/spark/bin/spark-submit --master local[4] graph_analysis.py -d
opt/spark/bin/spark-submit --conf spark.kafka.bootstrap.servers=kafka1:19092 --master spark://989eccb1080f:7077  kafka_consumer_to_hdfs.py -d
opt/spark/bin/spark-submit --master spark://989eccb1080f:7077  Customer_clustering_model.py  -d

/********
/opt/spark/bin/spark-submit \
  --conf spark.kafka.bootstrap.servers=kafka1:19092 \
  --master spark://989eccb1080f:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  kafka_consumer_to_hdfs.py -d



hdfs dfs -put /data/E-commerceCustomerBehavior-Sheet1.csv /E-commerceCustomerBehavior-Sheet1.csv

docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For\ Streaming-data/kafka_consumer_to_hdfs.py kafka1:/opt/kafka_consumer_to_hdfs.py
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For\ Streaming-data/test.py kafka1:/opt/test.py
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For\ Streaming-data/kafka_consumer_ecommerce_to_pg.py kafka1:/opt/kafka_consumer_ecommerce_to_pg.py
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For\ Streaming-data/kafaka_consumer_ecommerce_to_console.py kafka1:/opt/kafaka_consumer_ecommerce_to_console.py
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For\ Streaming-data/kafka_consumer_detect_anamolies.py kafka1:/opt/kafka_consumer_detect_anamolies.py
docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For\ Streaming-data/kafka_producer_ecommerce.py kafka1:/opt/kafka_producer_ecommerce.py

docker cp /mnt/c/Users/Admin/OneDrive/Desktop/vs.code/300baicodethieunhi/2024.1/big_data/E-commerce-Customer-Behavior-Analysis/Main/For\ Streaming-data/kafka_producer_ecommerce.py kafka1:/opt/kafka_producer_ecommerce.py

docker exec -it kafka1 /bin/bash
python /opt/kafka_producer_ecommerce.py
python /opt/kafka_consumer_detect_anamolies.py
python /opt/kafaka_consumer_ecommerce_to_console.py
python /opt/kafka_consumer_ecommerce_to_pg.py
python /opt/test.py
python /opt/kafka_consumer_to_hdfs.py


curl -O https://bootstrap.pypa.io/get-pip.py
python3 get-pip.py
pip install kafka-python
pip install pyspark


cd C:\Users\Admin\OneDrive\Desktop\vs.code\300baicodethieunhi\2024.1\big_data\E-commerce-Customer-Behavior-Analysis
wsl

python Main/For-batch-data/graph_analysis.py

docker build -t cluster-apache-spark:3.0.2 .