from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, from_json  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType  
  
# Initialize SparkSession  
spark = SparkSession \  
    .builder \  
    .appName("Ad Data Processing and Storage") \  
    .getOrCreate()  
  
# Define schema for ad impressions and clicks/conversions data  
impressions_schema = StructType([  
    StructField("ad_creative_id", StringType()),  
    StructField("user_id", StringType()),  
    StructField("timestamp", TimestampType()),  
    StructField("website", StringType())  
])  
  
clicks_schema = StructType([  
    StructField("event_timestamp", TimestampType()),  
    StructField("user_id", StringType()),  
    StructField("ad_campaign_id", StringType()),  
    StructField("conversion_type", StringType())  
])  
  
# Read ad impressions and clicks/conversions data from Kafka  
impressions_df = spark \  
    .readStream \  
    .format("kafka") \  
    .option("kafka.bootstrap.servers", "localhost:9092") \  
    .option("subscribe", "ad_impressions_topic") \  
    .load()  
  
clicks_df = spark \  
    .readStream \  
    .format("kafka") \  
    .option("kafka.bootstrap.servers", "localhost:9092") \  
    .option("subscribe", "clicks_and_conversions_topic") \  
    .load()  
  
# Deserialize the data  
impressions_df = impressions_df.selectExpr("CAST(value AS STRING)") \  
                               .select(from_json(col("value"), impressions_schema).alias("data")) \  
                               .select("data.*")  
  
clicks_df = clicks_df.selectExpr("CAST(value AS STRING)") \  
                     .select(from_json(col("value"), clicks_schema).alias("data")) \  
                     .select("data.*")  
  
# Data validation, filtering, and deduplication  
impressions_df = impressions_df.filter(col("user_id").isNotNull() & col("ad_creative_id").isNotNull())  
impressions_df = impressions_df.dropDuplicates()  
  
clicks_df = clicks_df.filter(col("user_id").isNotNull() & col("ad_campaign_id").isNotNull())  
clicks_df = clicks_df.dropDuplicates()  
  
# Correlate ad impressions with clicks and conversions  
joined_df = impressions_df.join(clicks_df, "user_id")  
  
# Write the processed data to Parquet format  
# Partitioning the data on 'date' column and bucketing on 'user_id' column  
joined_df.write \  
    .partitionBy("date") \  
    .bucketBy(100, "user_id") \  
    .sortBy("timestamp") \  
    .parquet("/path/to/save/data.parquet", mode="overwrite")  
