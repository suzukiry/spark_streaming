# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os

if __name__ == "__main__":

    spark = SparkSession.builder.appName("ApacheWindowOperationKafka2Kudu").getOrCreate()
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    kafkaDataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.0.0.60:9092,10.0.0.78:9092,10.0.0.238:9092").option("subscribe", "httpd-access").load()

    stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    schema = StructType().add("host", StringType())\
                            .add("user", StringType())\
                            .add("method", StringType())\
                            .add("path", StringType())\
                            .add("code", StringType())\
                            .add("size", StringType())\
                            .add("referer", StringType())\
                            .add("agent", StringType())\
                            .add("timestamp", StringType())

    jsonParsedDataFrame = stringFormattedDataFrame.select(from_json(stringFormattedDataFrame.value, schema).alias("apache_access"))

    formattedDataFrame = jsonParsedDataFrame.select(
                                                col("apache_access.host").alias("host"), \
                                                col("apache_access.timestamp").alias("time"), \
                                                col("apache_access.user").alias("access_user"), \
                                                col("apache_access.method").alias("access_method"), \
                                                col("apache_access.path").alias("access_path"), \
                                                col("apache_access.code").cast('int').alias("access_code"), \
                                                col("apache_access.size").cast('int').alias("access_size"), \
                                                col("apache_access.referer").alias("access_referer"), \
                                                col("apache_access.agent").alias("access_agent"))

    timestampedDataFrame = formattedDataFrame \
                                .withColumn("ts", to_timestamp(formattedDataFrame.time, 'yyyy/MM/dd HH:mm:ss'))

    analyzeBase = timestampedDataFrame.select("ts", "host", "access_user", "access_method", "access_path", "access_code", "access_size", "access_referer", "access_agent")

    windowedAvg = analyzeBase.withWatermark("ts", "2 minutes")\
                                .groupBy(window(analyzeBase.ts, "1 minutes", "1 minutes"), analyzeBase.host, analyzeBase.access_code, analyzeBase.access_path, analyzeBase.access_agent)\
                                .count()

    timeExtractedAvg = windowedAvg.select(col("host").alias("host"), \
                                    col("window.start").alias("window_start"),\
                                    col("window.end").alias("window_end"),\
                                    col("access_code").alias("access_code"),\
                                    col("access_path").alias("access_path"),\
                                    col("access_agent").alias("access_agent"),\
                                    col("count").alias("count"))

    filteredAvg = timeExtractedAvg.filter((col('access_code') == 404)).filter((col('count') > 2))

    finalizedDataFrame = filteredAvg.select(col("host").cast('string').alias("host"),\
                                    col("window_start").cast('string').alias("window_start"),\
                                    col("window_end").cast('string').alias("window_end"),\
                                    col("access_code").cast('string').alias("access_code"),\
                                    col("access_path").cast('string').alias("access_path"),\
                                    col("access_agent").cast('string').alias("access_agent"),\
                                    col("count").cast('string').alias("count"))

    kuduDataFrame = finalizedDataFrame.withColumn("key",concat_ws('_',col("host"),col("window_start")))

    kuduMaster = "10.0.0.212"
    tableName = "filtered_apache_access"
    operation = "upsert"
    kuduQuery = kuduDataFrame.writeStream.outputMode("update") \
                        .format("kudu") \
                        .option("kudu.master", kuduMaster) \
                        .option("kudu.table", tableName) \
                        .option("kudu.operation", operation) \
                        .option("checkpointLocation", "/tmp/checkpoint/ApacheWindowOperationKafka2Kudu") \
                        .start()


    consoleQuery = kuduDataFrame.writeStream.outputMode("update").format("console").start()
    consoleQuery.awaitTermination()
    
