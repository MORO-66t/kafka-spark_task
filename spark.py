from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType , IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("sparkConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "mysql:mysql-connector-java:8.0.40") \
    .getOrCreate()


schema = StructType([
    StructField("primaryCategories", StringType(), True),
    StructField("reviews_rating",DoubleType() , True)
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "product-reviews") \
    .option("startingOffsets", "latest") \
    .load()
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.primaryCategories", "data.reviews_rating")


flattened_df = parsed_df.select(
    col("primaryCategories").alias("primaryCategories"),
    col("reviews_rating").alias("reviews_rating")
)

transformed_df = flattened_df.groupBy("primaryCategories").agg(avg("reviews_rating").cast("double").alias("reviews_rating"))

def write_to_mysql(batch_df, batch_id):
    jdbc_url = "jdbc:mysql://localhost:3306/reviewsdb"
    properties = {
        "user": "root",
        "password": "0plm0plM@",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    batch_df.show()
    batch_df.write.jdbc(url=jdbc_url, table="average_ratings", mode="append", properties=properties)

    
query = transformed_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .start().awaitTermination() 
