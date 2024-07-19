from time import time, sleep
from datetime import datetime

from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_date, col, explode, split, lower, trim, desc

start_time = time()
spark = SparkSession \
    .builder \
    .appName("partiel") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

remover = StopWordsRemover(inputCol="word", outputCol="filtered_word")

github_file = "input/full.csv"

schema = StructType([
    StructField("commit", StringType(), False),
    StructField("author", StringType(), False),
    StructField("date", StringType(), False),
    StructField("message", StringType(), False),
    StructField("repo", StringType(), False),
])

github_df = spark.read \
    .option("header", "true") \
    .load(github_file, format="csv", schema=schema)

format_string = "EEE MMM d H:m:s y Z"

github_df = github_df.withColumn("date", to_date(col("date"), format_string))

not_null_df = github_df.where(col("repo").isNotNull())

# Question 1
not_null_df \
    .groupBy("repo") \
    .count() \
    .sort("count", ascending=False) \
    .limit(10) \
    .show()

# Question 2
spark_df = not_null_df.where(col("repo") == "apache/spark")
print(spark_df.groupBy("author").count().sort("count", ascending=False).first())

# Question 3
today = datetime.now()
five_years_ago = today.replace(year=today.year - 5)

spark_df \
    .where(col("date") > five_years_ago) \
    .groupBy("author") \
    .count() \
    .sort("count", ascending=False) \
    .limit(10) \
    .show()

# Question 4
remover.transform(github_df.withColumn('word', split(lower('message'), ' ')).na.drop(subset="word")) \
    .withColumn('words', explode(col("filtered_word"))) \
    .select("words") \
    .filter(trim(col("words")) != "") \
    .groupby("words") \
    .count() \
    .orderBy(desc('count')) \
    .limit(10).show()

# github_df.withColumn('word', explode(split('message', ' '))) \
#    .select(lower(col("word")).alias("word")) \
#    .groupBy('word') \
#    .count() \
#    .filter((trim(col("word")) != "") & (~col("word").isin(stopwords))) \
#    .orderBy(desc('count')) \
#    .limit(10) \
#    .show()

print("--- %s seconds ---" % (time() - start_time))

sleep(120)
