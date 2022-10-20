package com.woople.iceberg.example

import org.apache.spark.sql.SparkSession
import org.apache.iceberg.spark.IcebergSpark
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.DataTypes

object PartitionExample02 {
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

    val df = spark.read
      .format("csv")
      .option("charset", "utf-8")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("data/movielens_ratings.csv")

    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket4", DataTypes.LongType, 4)

    val fields = df.schema.fields.map(_.toDDL).mkString(", ")
    val partitionField = "bucket(4, userId)"

    spark.sql(
      s"""
        CREATE TABLE local.iceberg_db.movielens_ratings (
        $fields
        )
        USING iceberg
        PARTITIONED BY ($partitionField)
        TBLPROPERTIES ('format-version'='2')
        """)

    df.sortWithinPartitions(expr("iceberg_bucket4(userId)"))
      .writeTo("local.iceberg_db.movielens_ratings").append()

    spark.stop()
  }
}

