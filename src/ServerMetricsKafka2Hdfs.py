# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = SparkSession.builder.appName("ServerMetricsKafka2Hdfs").getOrCreate()
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    kafkaDataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.1.2.14:9092,10.1.2.7:9092,10.1.2.11:9092").option("subscribe", "tp-server-metrics").load()

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

    formattedDataFrame = jsonParsedDataFrame.select(\
                                                col("server_metrics.hostname").alias("hostname"), \
                                                col("server_metrics.@timestamp").alias("time"), \
                                                col("server_metrics.dstat.total_cpu_usage.usr").alias("cpu_usage_usr"), \
                                                col("server_metrics.dstat.total_cpu_usage.sys").alias("cpu_usage_sys"), \
                                                col("server_metrics.dstat.total_cpu_usage.idl").alias("cpu_usage_idl"), \
                                                col("server_metrics.dstat.total_cpu_usage.wai").alias("cpu_usage_wai"), \
                                                col("server_metrics.dstat.total_cpu_usage.hiq").alias("cpu_usage_hiq"), \
                                                col("server_metrics.dstat.total_cpu_usage.siq").alias("cpu_usage_siq"), \
                                                col("server_metrics.dstat.memory_usage.used").alias("mem_usage_used"), \
                                                col("server_metrics.dstat.memory_usage.buff").alias("mem_usage_buff"), \
                                                col("server_metrics.dstat.memory_usage.free").alias("mem_usage_free"), \
                                                col("server_metrics.dstat.memory_usage.cach").alias("mem_usage_cach"), \
                                                col("server_metrics.dstat.dsk/total.writ").alias("dsk_total_writ"), \
                                                col("server_metrics.dstat.dsk/total.read").alias("dsk_total_read"), \
                                                col("server_metrics.dstat.paging.in").alias("paging_usage_in"), \
                                                col("server_metrics.dstat.paging.out").alias("paging_usage_out"), \
                                                col("server_metrics.dstat.net/total.recv").alias("net_total_recv"), \
                                                col("server_metrics.dstat.net/total.send").alias("net_total_send"), \
                                                col("server_metrics.dstat.system.int").alias("system_usage_int"), \
                                                col("server_metrics.dstat.system.csw").alias("system_usage_csw"))

    withMonth = formattedDataFrame.withColumn("ts", from_unixtime(unix_timestamp("time", "yyyy/MM/dd HH:mm:ss"))) \
                                  .select(date_format("ts", "yyyyMM").alias("month"), "*")
    hdfsQuery = withMonth.writeStream \
                     .partitionBy("hostname", "month") \
                     .outputMode("append") \
                     .format("parquet") \
                     .option("path", "/tmp/server_metrics/") \
                     .option("checkpointLocation", "/tmp/checkpoint/ServerMetricsKafka2Hdfs") \
                     .start()

    consoleQuery = withMonth.writeStream.outputMode("append").format("console").start()
    consoleQuery.awaitTermination()

