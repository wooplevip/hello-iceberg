package com.woople.iceberg.example

import org.apache.iceberg.spark.IcebergSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes

object QueryExample {
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

    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket4", DataTypes.IntegerType, 4)

    spark.sql("select *  from local.iceberg_db.movielens_ratings where iceberg_bucket4(userId)==0").show(false)

    spark.stop()
  }
}

