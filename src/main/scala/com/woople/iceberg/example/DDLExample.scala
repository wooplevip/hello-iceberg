package com.woople.iceberg.example

import org.apache.spark.sql.SparkSession

object DDLExample {
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

    spark.sql("delete  from local.iceberg_db.movielens_ratings where userId=4").show(false)

    spark.stop()
  }
}

