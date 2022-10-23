package com.woople.iceberg.example

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession

object SparkExpireSnapshotsExample {
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

    val t = System.currentTimeMillis()
    val table = Spark3Util.loadIcebergTable(spark, "local.iceberg_db.movielens_ratings")

    val result = SparkActions
      .get(spark)
      .expireSnapshots(table)
      .expireOlderThan(t)
      .retainLast(1)
      .execute()

    println(s"result.deletedDataFilesCount() = ${result.deletedDataFilesCount()}")
    println(s"result.deletedManifestsCount() = ${result.deletedManifestsCount()}")
    println(s"result.deletedManifestListsCount() = ${result.deletedManifestListsCount()}")
    println(s"result.deletedEqualityDeleteFilesCount() = ${result.deletedEqualityDeleteFilesCount()}")
    println(s"result.deletedPositionDeleteFilesCount() = ${result.deletedPositionDeleteFilesCount()}")

    spark.stop()
  }
}

