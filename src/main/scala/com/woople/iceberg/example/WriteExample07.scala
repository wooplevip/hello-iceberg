package com.woople.iceberg.example

import org.apache.spark.sql.SparkSession

object WriteExample07 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.name", "local")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
      .getOrCreate()

    val tableName = "local.iceberg_db.ob001"

    val df = spark.read
      .format("binaryFile")
      .load("data/logs1.csv")

    df.writeTo(tableName).createOrReplace()

    spark.sql(
      s"""
              select base64(content) from ${tableName}
        """).show(false)

    spark.stop()
  }
}

