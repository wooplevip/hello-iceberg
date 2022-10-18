package com.woople.iceberg.example

import org.apache.spark.sql.SparkSession

object QuickstartExample {
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

    spark.read
      .format("csv")
      .option("charset", "utf-8")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("data/movielens_ratings.csv")
      .writeTo("local.iceberg_db.movielens_ratings")
      .tableProperty("format-version", "2")
      .create()

    spark.sql("select *  from local.iceberg_db.movielens_ratings ").show(false)

    spark.stop()
  }
}

