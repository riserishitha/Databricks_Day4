from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Sales ETL") \
    .getOrCreate()

# Load CSV file
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

print("Original Data")
df.show()

# -------------------------------
# Transformation
# -------------------------------

# groupBy is a TRANSFORMATION (lazy operation)
grouped_df = df.groupBy("category")

print("Transformation created (groupBy) - no execution yet")

# -------------------------------
# Action
# -------------------------------

# count() is an ACTION (triggers execution)
result = grouped_df.count()

print("Action triggered (count)")
result.show()

# -------------------------------
# Execution Plan
# -------------------------------

print("Spark Execution Plan")
result.explain()

# Stop Spark session
spark.stop()