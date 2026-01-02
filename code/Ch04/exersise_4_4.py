from pyspark.sql import SparkSession

import pyspark.sql.functions as F
import os

spark = SparkSession.builder.getOrCreate()

# Define the directory containing the data
DIRECTORY = "../../data/broadcast_logs/"

# Read the CSV file into a DataFrame
logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep = "|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd"
)

# Drop unnecessary ID columns
columns = [col for col in logs.columns if 'ID' not in col]
logs_clean = logs.select(*columns)

# Show the first 5 rows of the cleaned DataFrame
logs_clean.show(5, False)
