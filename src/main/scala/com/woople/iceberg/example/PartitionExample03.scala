package com.woople.iceberg.example

import org.apache.spark.sql.SparkSession

object PartitionExample03 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.name", "local")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      //.config("spark.sql.sources.partitionOverwriteMode", "static")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("charset", "utf-8")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("data/movielens_ratings.csv")

    spark.sql(
      """
        INSERT OVERWRITE local.iceberg_db.movielens_ratings
        select * from local.iceberg_db.movielens_ratings
        """)

    df.writeTo("local.iceberg_db.movielens_ratings").append()

    spark.sql("select * from local.iceberg_db.movielens_ratings").show(false)

    spark.stop()
  }
}

