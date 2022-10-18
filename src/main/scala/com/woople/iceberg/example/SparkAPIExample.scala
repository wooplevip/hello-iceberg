package com.woople.iceberg.example

import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

object SparkAPIExample {
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

    val table = Spark3Util.loadIcebergTable(spark, "local.iceberg_db.movielens_ratings")

    println(table.schemas())


    spark.stop()
  }
}

