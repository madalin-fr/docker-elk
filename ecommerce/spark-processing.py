from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import DataFrame
import logging


# Initialize logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FakeEcommerce Data Analysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Kafka configuration
kafka_bootstrap_servers = "broker:29092,broker2:29094"

# Read data from 'ecommerce_customers' topic
customerSchema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("account_created", StringType(), True),
    StructField("last_login", TimestampType(), True),
])

customerDF = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json("value", customerSchema).alias("data"))
                .select("data.*")
                .withWatermark("last_login", "2 hours")
                )


# Read data from 'ecommerce_products' topic
productSchema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier", StringType(), True),
    StructField("rating", DoubleType(), True)
])
productDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_products") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", productSchema).alias("data")) \
    .select("data.*") \
    .withColumn("processingTime", current_timestamp())  # Add processing timestamp

productDF = productDF.withWatermark("processingTime", "2 hours")


# Read data from 'ecommerce_transactions' topic
transactionSchema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("date_time", TimestampType(), True),  
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True)
])
transactionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_transactions") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", transactionSchema).alias("data")) \
    .select("data.*")

transactionDF = transactionDF.withColumn("processingTime", current_timestamp())
transactionDF = transactionDF.withWatermark("processingTime", "2 hours")


# Read data from 'ecommerce_product_views' topic
productViewSchema = StructType([
    StructField("view_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("view_duration", IntegerType(), True)
])
productViewDF = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                 .option("subscribe", "ecommerce_product_views")
                 .option("startingOffsets", "earliest")
                 .load()
                 .selectExpr("CAST(value AS STRING)")
                 .select(from_json("value", productViewSchema).alias("data"))
                 .select("data.*")
                 .withColumn("timestamp", col("timestamp").cast("timestamp"))
                 .withWatermark("timestamp", "1 hour")
                 )
productViewDF = productViewDF.withColumn("processingTime", current_timestamp())
productViewDF = productViewDF.withWatermark("processingTime", "2 hours")


# Read data from 'ecommerce_system_logs' topic
systemLogSchema = StructType([
    StructField("log_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("level", StringType(), True),
    StructField("message", StringType(), True)
])

systemLogDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_system_logs") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", systemLogSchema).alias("data")) \
    .select("data.*")

systemLogDF = systemLogDF.withColumn("processingTime", current_timestamp())
systemLogDF = systemLogDF.withWatermark("processingTime", "2 hours")


# Read data from 'ecommerce_user_interactions' topic
userInteractionSchema = StructType([
    StructField("interaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("interaction_type", StringType(), True),
    StructField("details", StringType(), True)
])

userInteractionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_user_interactions") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", userInteractionSchema).alias("data")) \
    .select("data.*")

userInteractionDF = userInteractionDF.withColumn("processingTime", current_timestamp())
userInteractionDF = userInteractionDF.withWatermark("processingTime", "2 hours")


#This analysis  focus on demographics and account activity.
customerAnalysisDF = (customerDF
                      .groupBy(
                          window(col("last_login"), "1 day"),  # Windowing based on last_login
                          "gender"
                      )
                      .agg(
                          count("customer_id").alias("total_customers"),
                          max("last_login").alias("last_activity")
                      )
                     )

# Analyzing product popularity and stock status with windowing
productAnalysisDF = productDF \
    .groupBy(
        window(col("processingTime"), "1 hour"),  # Window based on processingTime
        "category"
    ) \
    .agg(
        avg("price").alias("average_price"),
        sum("stock_quantity").alias("total_stock")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("average_price"),
        col("total_stock")
    )


#Analyzing sales data
salesAnalysisDF = transactionDF \
    .groupBy(
        window(col("processingTime"), "1 hour"),  # Window based on processingTime
        "product_id"
    ) \
    .agg(
        count("transaction_id").alias("number_of_sales"),
        sum("quantity").alias("total_quantity_sold"),
        approx_count_distinct("customer_id").alias("unique_customers")  # Use approx_count_distinct
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_id"),
        col("number_of_sales"),
        col("total_quantity_sold"),
        col("unique_customers")
    )
                               
# Understanding customer interest in products.
productViewsAnalysisDF = productViewDF \
    .withWatermark("timestamp", "2 hours") \
    .groupBy(
        window(col("timestamp"), "1 hour"), 
        "product_id"
    ) \
    .agg(
        count("view_id").alias("total_views"),
        avg("view_duration").alias("average_view_duration")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_id"),
        col("total_views"),
        col("average_view_duration")
    )


# User Interaction Analysis
interactionAnalysisDF = userInteractionDF \
    .withWatermark("timestamp", "2 hours") \
    .groupBy(
        window(col("timestamp"), "1 hour"), 
        "interaction_type"
    ) \
    .agg(
        count("interaction_id").alias("total_interactions"),
        approx_count_distinct("customer_id").alias("unique_users_interacted")  # Use approx_count_distinct
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("interaction_type"),
        col("total_interactions"),
        col("unique_users_interacted")
    )




