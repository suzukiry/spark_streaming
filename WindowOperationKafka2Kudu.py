# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os

if __name__ == "__main__":

    spark = SparkSession.builder.appName("WindowOperationKafka2Kudu").getOrCreate()
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    kafkaDataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.0.0.60:9092,10.0.0.78:9092,10.0.0.238:9092").option("subscribe", "server-metrics").load()

    stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    cpuSchema = StructType().add("usr", StringType()).add("sys", StringType()).add("idl", StringType()).add("wai", StringType()).add("hiq", StringType()).add("siq", StringType())
    memSchema = StructType().add("used", StringType()).add("free", StringType()).add("buff", StringType()).add("cach", StringType())
    netSchema = StructType().add("recv", StringType()).add("send", StringType())
    pagSchema = StructType().add("in", StringType()).add("out", StringType())
    sysSchema = StructType().add("int", StringType()).add("csw", StringType())
    dskSchema = StructType().add("read", StringType()).add("writ", StringType())

    dstSchema = StructType().add("dsk/total", dskSchema).add("memory_usage", memSchema).add("net/total", netSchema).add("paging", pagSchema).add("system", sysSchema).add("total_cpu_usage", cpuSchema)

    schema = StructType().add("dstat", dstSchema).add("hostname", StringType()).add("@timestamp", StringType())

    jsonParsedDataFrame = stringFormattedDataFrame.select(from_json(stringFormattedDataFrame.value, schema).alias("server_metrics"))

    formattedDataFrame = jsonParsedDataFrame.select(
                                                col("server_metrics.hostname").alias("hostname"), \
                                                col("server_metrics.@timestamp").alias("time"), \
                                                col("server_metrics.dstat.total_cpu_usage.usr").cast('float').alias("cpu_usage_usr"), \
                                                col("server_metrics.dstat.total_cpu_usage.sys").cast('float').alias("cpu_usage_sys"), \
                                                col("server_metrics.dstat.total_cpu_usage.wai").cast('float').alias("cpu_usage_wai"), \
                                                col("server_metrics.dstat.total_cpu_usage.hiq").cast('float').alias("cpu_usage_hiq"), \
                                                col("server_metrics.dstat.total_cpu_usage.siq").cast('float').alias("cpu_usage_siq"), \
                                                col("server_metrics.dstat.dsk/total.writ").cast('float').alias("dsk_total_writ"), \
                                                col("server_metrics.dstat.dsk/total.read").cast('float').alias("dsk_total_read"), \
                                                col("server_metrics.dstat.memory_usage.used").cast('float').alias("mem_usage_used"))

    timestampedDataFrame = formattedDataFrame \
                                .withColumn("timestamp", to_timestamp(formattedDataFrame.time, 'yyyy/MM/dd HH:mm:ss'))

    timestampedDataFrame = timestampedDataFrame \
                                .withColumn("cpu_usage_total", \
                                        timestampedDataFrame.cpu_usage_usr + \
                                        timestampedDataFrame.cpu_usage_sys + \
                                        timestampedDataFrame.cpu_usage_wai + \
                                        timestampedDataFrame.cpu_usage_hiq + \
                                        timestampedDataFrame.cpu_usage_siq)

    analyzeBase = timestampedDataFrame.select("timestamp", "hostname", "cpu_usage_total", "dsk_total_writ", "dsk_total_read", "mem_usage_used")

    windowedAvg = analyzeBase.withWatermark("timestamp", "3 minutes")\
                                .groupBy(window(analyzeBase.timestamp, "2 minutes", "1 minutes"), analyzeBase.hostname)\
                                .avg("cpu_usage_total", "dsk_total_writ", "dsk_total_read", "mem_usage_used")

    timeExtractedAvg = windowedAvg.select(col("hostname").alias("hostname"), \
                                    col("window.start").alias("window_start"),\
                                    col("window.end").alias("window_end"),\
                                    col("avg(cpu_usage_total)").alias("avg_cpu_usage_total"),\
                                    col("avg(dsk_total_writ)").alias("avg_dsk_total_writ"),\
                                    col("avg(dsk_total_read)").alias("avg_dsk_total_read"),\
                                    col("avg(mem_usage_used)").alias("avg_mem_usage_used"))

    #filteredAvg = timeExtractedAvg.filter(timeExtractedAvg.avg_cpu_usage_total > 50)
    filteredAvg = timeExtractedAvg.filter(timeExtractedAvg.avg_dsk_total_writ > 1000000)


    finalizedDataFrame = filteredAvg.select(col("hostname").cast('string').alias("hostname"),\
                                    col("window_start").cast('string').alias("window_start"),\
                                    col("window_end").cast('string').alias("window_end"),\
                                    col("avg_cpu_usage_total").cast('string').alias("avg_cpu_usage_total"),\
                                    col("avg_dsk_total_writ").cast('string').alias("avg_dsk_total_writ"),\
                                    col("avg_dsk_total_read").cast('string').alias("avg_dsk_total_read"),\
                                    col("avg_mem_usage_used").cast('string').alias("avg_mem_usage_used"))


    kuduDataFrame = finalizedDataFrame.withColumn("key",concat_ws('_',col("hostname"),col("window_start")))

    kuduMaster = "10.0.0.212"
    tableName = "filtered_metrics"
    operation = "upsert"
    kuduQuery = kuduDataFrame.writeStream.outputMode("update") \
                        .format("kudu") \
                        .option("kudu.master", kuduMaster) \
                        .option("kudu.table", tableName) \
                        .option("kudu.operation", operation) \
                        .option("checkpointLocation", "/tmp/checkpoint/WindowOperationKafka2Kudu_Kudu") \
                        .start()

    kafkaQuery = kuduDataFrame.selectExpr("to_json(struct(*)) AS value")\
                        .writeStream.outputMode("update")\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers", "10.0.0.60:9092,10.0.0.78:9092,10.0.0.238:9092")\
                        .option("topic", "filtered-metrics")\
                        .option("checkpointLocation", "/tmp/checkpoint/WindowOperationKafka2Kudu_Kafka")\
                        .start()\

    consoleQuery = kuduDataFrame.writeStream.outputMode("update").format("console").start()
    consoleQuery.awaitTermination()
    
