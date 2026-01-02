from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# Read the sample broadcast logs data
logs_raw = spark.read.csv("../../data/broadcast_logs/BroadcastLogs_2018_Q3_M8_sample.CSV")

# Show the first five rows of the raw logs data
logs_raw.show(5, False)

# Print the schema of the raw logs data
logs_raw.printSchema()