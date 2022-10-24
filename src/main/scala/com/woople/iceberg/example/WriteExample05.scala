package com.woople.iceberg.example

import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object WriteExample05 {
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

    /**
     * 如果通过iceberg创建的是timestamp数据类型，需要配置
     * spark.sql.iceberg.handle-timestamp-without-timezone为true
     * */

    val tableName = "local.iceberg_db.logs01"
    val table = Spark3Util.loadIcebergTable(spark, tableName)

    val schemaStr = table.schema().asStruct().fields().asScala.map(x=>s"${x.name} ${x.`type`().toString}  ").mkString(",")
    println("schemaStr: " + schemaStr)

    val df = spark.read
      .format("csv")
      .option("charset", "utf-8")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schemaStr)
      .load("data/logs1.csv")

    df.printSchema()
    df.writeTo(tableName).append()

    spark.sql(
      s"""
              select * from ${tableName}
        """).show(false)

    spark.stop()
  }
}

