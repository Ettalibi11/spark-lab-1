import sys
import os
import time
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# -----------------------------------------
# Create SparkSession
# -----------------------------------------
# Initialize the SparkSession, which represents the entry point to Spark SQL.
# - appName: logical name of the Spark application shown in Spark UI.
# - master: URL of the Spark cluster; here using the local Docker Spark master.
# - spark.driver.memory: memory allocated to the driver process.
spark = SparkSession.builder \
    .appName("Day1-HelloSpark") \
    .master("local") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Display basic application metadata for verification and debugging.
print("=" * 50)
print("Spark Application Started!")
print("=" * 50)
print(f"App Name: {spark.sparkContext.appName}")       # Name used by the SparkContext
print(f"Spark Version: {spark.version}")               # Spark version running
print(f"Master: {spark.sparkContext.master}")          # Master URL used by this driver
print(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")  
# Default number of parallel tasks (depends on cluster resources)
print("=" * 50)

# -----------------------------------------
# Sample Dataset
# -----------------------------------------
# Create a small in-memory dataset to demonstrate DataFrame operations.
data = [
    ("Alice", 34, "Data Engineer"),
    ("Bob", 45, "Data Scientist"),
    ("Charlie", 28, "ML Engineer"),
    ("Diana", 32, "Analytics Engineer"),
    ("Eve", 29, "Data Analyst")
]

# Convert Python list → Spark DataFrame with explicit column names.
df = spark.createDataFrame(data, ["name", "age", "role"])

print("\nOur First DataFrame:")
df.show()  # Display records in tabular form

print("\nDataFrame Schema:")
df.printSchema()  # Show structure, types, and nullable info

print(f"\nTotal Records: {df.count()}")  # Trigger action to count rows
print(f"Number of Columns: {len(df.columns)}")  # Count DataFrame columns

print("\nPeople over 30:")
df.filter(df.age > 30).show()  # Apply row-level filter using Spark SQL expressions

print("\nAverage Age:")
df.agg({"age": "avg"}).show()  # Perform aggregation on a column


# -----------------------------------------
# Stop Spark
# -----------------------------------------
# input("Press Enter to exit...")
time.sleep(300)

# spark.stop()  # Optional: stop the SparkSession/Context explicitly.
print("\nSpark Application Stopped!")