import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


schema = StructType([
    StructField("crime_id",StringType(),True),
    StructField("original_crime_type_name",StringType(),True),
    StructField("call_date",StringType(),True),
    StructField("call_date_time",StringType(),True),
    StructField("call_time",StringType(),True),
    StructField("call_date_time",StringType(),True),
    StructField("address", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("common_location", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("report_date", StringType(), True)
])

def run_spark_job(spark):
    """
    Reads and aggregates data
    Input parameters: spark session object
    """
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "udacity.sfcrime.analytics.police.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 100) \
        .option("maxOffsetPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")
    kafka_df.printSchema()

    service_table = kafka_df\
        .select(psf.from_json(psf.col("value"), schema).alias("DF"))\
        .select("DF.*")
    service_table.printSchema()

    distinct_table = service_table.select("original_crime_type_name", "disposition").distinct()
    distinct_table.printSchema()

    # count the number of original crime type
    agg_df = distinct_table.select("original_crime_type_name").groupby("original_crime_type_name").agg({"original_crime_type_name": "count"})

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    query = agg_df \
            .writeStream \
            .format("console") \
            .outputMode("complete") \
            .trigger(once=True) \
            .option("truncate", False) \
            .start()
    
    logger.info("RECENT PROGRESS:\n", query.recentProgress, "\n")
    logger.info("LAST PROGRESS:\n", query.lastProgress, "\n")
    logger.info("STATUS:\n", query.status, "\n")

    # attach a ProgressReporter
    query.awaitTermination()

    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine = True)

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    radio_code_df.printSchema()

    join_query = distinct_table \
                .join(radio_code_df, on=["disposition"], how="left") \
                .select("original_crime_type_name","description") \
                .writeStream \
                .format("console") \
                .trigger(processingTime="30 seconds") \
                .option('truncate', False) \
                .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .config("spark.driver.memory", "3g") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("info")
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
