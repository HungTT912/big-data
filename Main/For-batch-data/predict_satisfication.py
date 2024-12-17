from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def preprocess_data(df):
    """
    Preprocess the input DataFrame by dropping 'Customer ID' column,
    handling missing values, and encoding string columns.

    Parameters:
    df (pyspark.sql.DataFrame): Input DataFrame.

    Returns:
    Tuple[pyspark.sql.DataFrame, list]: Transformed DataFrame and list of StringIndexer models.
    """
    # Drop 'Customer ID' column
    df = df.drop("Customer ID")

    # Handle missing values
    df = df.na.drop()

    # Encode string columns
    string_cols = [col_name for col_name, data_type in df.dtypes if data_type == "string"]
    indexers = [StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index", handleInvalid='skip').fit(df) for col_name in string_cols]

    transformed_df = df
    for indexer in indexers:
        transformed_df = indexer.transform(transformed_df)

    return transformed_df.drop(*string_cols), indexers

def split_data(df, split_ratios=[0.8, 0.2], seed=42):
    """
    Split the DataFrame into training and testing sets based on the specified ratios.

    Parameters:
    df (pyspark.sql.DataFrame): Input DataFrame.
    split_ratios (list): List of ratios for train-test split.
    seed (int): Seed for reproducibility.

    Returns:
    Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]: Training and testing DataFrames.
    """
    train_df, test_df = df.randomSplit(split_ratios, seed)
    return train_df, test_df

def create_logistic_regression_pipeline(train_df):
    """
    Create a logistic regression model pipeline with stages for StringIndexers, VectorAssembler, and Logistic Regression.

    Parameters:
    train_df (pyspark.sql.DataFrame): Training DataFrame.

    Returns:
    pyspark.ml.Pipeline: Trained pipeline.
    """
    vec_assembler = VectorAssembler(inputCols=train_df.columns, outputCol='features')
    logistic_model = LogisticRegression(featuresCol='features', labelCol='Satisfaction Level_index', predictionCol='prediction')

    pipeline = Pipeline(stages=[vec_assembler, logistic_model]).fit(train_df)
    return pipeline

def evaluate_model(predictions):
    """
    Evaluate the model using MulticlassClassificationEvaluator with accuracy metric.

    Parameters:
    predictions (pyspark.sql.DataFrame): DataFrame with model predictions.

    Returns:
    float: Model accuracy.
    """
    evaluator = MulticlassClassificationEvaluator(labelCol="Satisfaction Level_index", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    return accuracy

def main():
    # Initializing Spark Session
    spark = SparkSession.builder.appName("CustomerSegmentation").getOrCreate()
    
    # Assuming you have a DataFrame named 'customer_df'
    customer_df = spark.read.csv("E-commerce Customer Behavior.csv", header=True, inferSchema=True)

    # Preprocess data
    transformed_df, indexers = preprocess_data(customer_df)

    # Split data
    train_df, test_df = split_data(transformed_df)

    # Create and train logistic regression pipeline
    pipeline = create_logistic_regression_pipeline(train_df)

    # Make predictions
    predictions = pipeline.transform(test_df)

    # Evaluate the model
    accuracy = evaluate_model(predictions)
    print(f"Accuracy: {accuracy}")

# Call the main function
main()
