from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType , IntegerType
# from pyspark.sql.streaming import Trigger

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ProductReviewSummary") \
    .getOrCreate()

# Define schema for data
schema = StructType([
    StructField("primaryCategories", StringType(), True),
    StructField("reviews", StructType([
        StructField("rating", DoubleType(), True)
    ]))
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "product-reviews") \
    .load()

# Parse JSON data
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Calculate average review rating by category
result_df = parsed_df.groupBy("primaryCategories").agg(avg("reviews.rating").alias("avg_rating"))

# Write to MySQL
def write_to_mysql(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/reviewsdb") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "average_ratings") \
        .option("user", "root") \
        .option("password", "") \
        .mode("append") \
        .save()

query = result_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql) \
    .start()

# query.awaitTermination()
