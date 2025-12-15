from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.appName(
    "Counting word occurences from a book."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def count_word_occurences(file_path):
    # Count word occurences in the given file
    results = (
        spark.read.text(file_path)
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
        .groupby(F.col("word"))
        .count()
    )
    return results

# If you need to read multiple text files, replace `1342-0` by `*`.
file_path = "../../data/gutenberg_books/1342-0.txt"

# Display the 5 words that appear only once in the book
(
    count_word_occurences(file_path)
    .where(F.col("count") == 1)
    .select("word")
    .show(5)
)