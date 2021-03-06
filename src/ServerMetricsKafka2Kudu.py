# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = SparkSession.builder.appName("ServerMetricsKafka2Kudu").getOrCreate()
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    kafkaDataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.1.2.8:9092,10.1.2.13:9092,10.1.2.10:9092").option("subscribe", "tp-server-metrics").load()

    stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    cpuSchema = StructType().add("usr", StringType()).add("sys", StringType()).add("idl", StringType()).add("wai", StringType()).add("hiq", StringType()).add("siq", StringType())
    memSchema = StructType().add("used", StringType()).add("free", StringType()).add("buff", StringType()).add("cach", StringType())
    dskSchema = StructType().add("read", StringType()).add("writ", StringType())
    pagSchema = StructType().add("in", StringType()).add("out", StringType())
    netSchema = StructType().add("recv", StringType()).add("send", StringType())
    sysSchema = StructType().add("int", StringType()).add("csw", StringType())

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

    dstatDataFrame = formattedDataFrame.withColumn("key",concat_ws('_',col("hostname"),col("time")))

    kuduMaster = "10.1.2.16"
    tableName = "dstat_metrics"
    operation = "upsert"
    kuduQuery = dstatDataFrame.writeStream.outputMode("append") \
                        .format("kudu") \
                        .option("kudu.master", kuduMaster) \
                        .option("kudu.table", tableName) \
                        .option("kudu.operation", operation) \
                        .option("checkpointLocation", "/tmp/checkpoint/ServerMetricsKafka2Kudu") \
                        .start()

    consoleQuery = dstatDataFrame.writeStream.outputMode("update").format("console").start()
    consoleQuery.awaitTermination()

