# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os

if __name__ == "__main__":

    spark = SparkSession.builder.appName("WindowOperationKafka2Hdfs").getOrCreate()
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    kafkaDataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.0.0.60:9092,10.0.0.78:9092,10.0.0.238:9092").option("subscribe", "filtered-metrics").load()

    stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    schema = StructType().add("hostname", StringType())\
                    .add("window_start", StringType())\
                    .add("window_end", StringType())\
                    .add("avg_cpu_usage_total", StringType())\
                    .add("avg_dsk_total_writ", StringType())\
                    .add("avg_dsk_total_read", StringType())\
                    .add("avg_mem_usage_used", StringType())\
                    .add("key", StringType())

    jsonParsedDataFrame = stringFormattedDataFrame.select(from_json(stringFormattedDataFrame.value, schema).alias("windowed_metrics"))
    
    formattedDataFrame = jsonParsedDataFrame.select(\
                                                col("windowed_metrics.hostname").alias("hostname"), \
                                                col("windowed_metrics.window_start").alias("window_start"), \
                                                col("windowed_metrics.window_end").alias("window_end"), \
                                                col("windowed_metrics.avg_cpu_usage_total").alias("avg_cpu_usage_total"), \
                                                col("windowed_metrics.avg_dsk_total_writ").alias("avg_dsk_total_writ"), \
                                                col("windowed_metrics.avg_dsk_total_read").alias("avg_dsk_total_read"), \
                                                col("windowed_metrics.avg_mem_usage_used").alias("avg_mem_usage_used"))

    withMonth = formattedDataFrame.withColumn("ts", from_unixtime(unix_timestamp("window_start", "yyyy-MM-dd HH:mm:ss"))) \
                                  .select(date_format("ts", "yyyyMM").alias("month"), "*")

    hdfsQuery = withMonth.writeStream \
                     .partitionBy("hostname", "month") \
                     .outputMode("append") \
                     .format("json") \
                     .option("path", "/tmp/metrics_windowed/") \
                     .option("checkpointLocation", "/tmp/checkpoint/WindowOperationKafka2Hdfs_02/") \
                     .start()

    consoleQuery = withMonth.writeStream.outputMode("append").format("console").start()
    consoleQuery.awaitTermination()


