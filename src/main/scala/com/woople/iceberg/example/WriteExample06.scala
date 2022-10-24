package com.woople.iceberg.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object WriteExample06 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.name", "local")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    val tableName = "local.iceberg_db.logs01"

    val structType = new StructType()
      .add("uuid", StringType, false)
      .add("level", StringType)
      .add("ts", TimestampType)
      .add("message", StringType)

    val df = spark.read
      .format("csv")
      .option("charset", "utf-8")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(structType)
      .load("data/logs1.csv")

//    spark.createDataFrame(df.rdd, structType)
//      .writeTo(tableName).append()

//    df.withColumn("uuid", col("uuid").isNotNull)
//      .writeTo(tableName).append()

    df.printSchema()
    df.writeTo(tableName).append()

    spark.sql(
      s"""
              select * from ${tableName}
        """).show(false)

    spark.stop()
  }
}

