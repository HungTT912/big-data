from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


def preprocessing(customer_data):
    '''
    Description: Performs data preprocessing, including column selection, encoding, assembling features, and standardizing features.
    Input: customer_data (DataFrame)
    Output: Preprocessed DataFrame (scaled_customer_seg)
    '''
    # Select columns
    customer_seg_data = customer_data[['Membership Type', 'Total Spend', 'Items Purchased']]
    
    # Encoding 'Membership Type'
    customer_seg = customer_seg_data.replace(['Bronze', 'Silver', 'Gold'], ['0', '1', '2'], 'Membership Type')
    customer_seg = customer_seg.withColumn("Membership Type", customer_seg['Membership Type'].cast('integer'))

    # Assemble features into a single vector
    assembler = VectorAssembler(inputCols=['Membership Type', 'Total Spend', 'Items Purchased'], outputCol="features")
    customer_seg = assembler.transform(customer_seg)

    # Standardize features
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    scaler_model = scaler.fit(customer_seg)
    scaled_customer_seg = scaler_model.transform(customer_seg)

    return scaled_customer_seg


def kmeans_clustering(scaled_customer_seg):

    '''
    Description: Performs KMeans clustering on the preprocessed data and evaluates clustering performance.
    Input: Preprocessed DataFrame (scaled_customer_seg)
    Output: Tuple of Silhouette score and DataFrame with cluster distribution counts
    '''
    # Train KMeans model
    kmeans = KMeans(featuresCol="scaled_features", k=4, seed=42)
    
    # Make predictions
    predictions = kmeans.fit(scaled_customer_seg).transform(scaled_customer_seg)

    # Evaluate clustering performance
    silhouette = ClusteringEvaluator().evaluate(predictions)
    print(f"Silhouette score: {silhouette}\n")

    # Get cluster distribution
    cluster_distribution = predictions.select("prediction").toPandas()

    # Count the occurrences of each cluster label
    cluster_distribution_counts = cluster_distribution["prediction"].value_counts().sort_index()

    return silhouette, cluster_distribution_counts



def print_clusters(predictions):
    ''' 
    Description: Prints information related to each cluster, including samples and descriptive statistics.
    Input: Predictions DataFrame (predictions)
    Output: None (Prints cluster information)
    '''
    # Print information related to each cluster
    for cluster_label, count in cluster_distribution_counts.items():
        cluster_items = predictions.filter(predictions['prediction'] == cluster_label).select(['Membership Type', 'Total Spend', 'Items Purchased'])
        cluster_info = cluster_items.describe().toPandas()
        print(f"\nCluster {cluster_label} Sample:\n")
        cluster_items.show(5)
        print(f"\nCluster {cluster_label} Information:\n")
        print(cluster_info)


def main():
    # Initializing Spark Session
    spark = SparkSession.builder.appName("CustomerSegmentation").getOrCreate()
    
    # Assuming you have a DataFrame named 'customer_df'
    customer_data = spark.read.csv("E-commerce Customer Behavior.csv", header=True, inferSchema=True)

    # Preprocessing
    scaled_customer_seg = preprocessing(customer_data)
    # KMeans clustering
    silhouette, cluster_distribution_counts = kmeans_clustering(scaled_customer_seg)
    # Print clusters
    print_clusters(predictions)

# Call the main function
main()

