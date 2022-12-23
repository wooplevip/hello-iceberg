package com.woople.iceberg.example

import org.apache.spark.sql.SparkSession

object TimestampExample {
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

    val df = spark.sql(
      """
        SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887, 'CET') as cet,
        make_timestamp(2014, 12, 28, 6, 30, 45.887, 'UTC+8') as utc,
        make_timestamp(2014, 12, 28, 6, 30, 45.887, 'Asia/Shanghai') as china,
        current_timezone() as tz,
        to_timestamp('2022-08-02 17:10:30') as a,
        to_timestamp('2015-03-05T09:32:05.359') as b,
        to_timestamp('2022-08-02 17:10:30+06:00') as c
        """)

    df.printSchema()
    df.show(false)


    spark.stop()
  }
}

