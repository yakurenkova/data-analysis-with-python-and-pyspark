from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.appName(
    "Counting distinct words from a book."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def count_distinct_words(file_path):
    # count distinct words in the given file
    results = (
        spark.read.text(file_path)
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
        .distinct()
        .count()
    )
    return results

# If you need to read multiple text files, replace `1342-0` by `*`.
file_path = "../../data/gutenberg_books/1342-0.txt"
print(count_distinct_words(file_path))