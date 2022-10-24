package com.woople.iceberg.example

import org.apache.commons.lang3.StringUtils
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object WriteExample02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.name", "local")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    /**
     *  INSERT OVERWRITE A
     *  SELECT * FROM B
     *
     * spark.sql.sources.partitionOverwriteMode默认为static
     * static：会将A表清空，将B表的查询结果插入A表
     *         如果带分区字段则只会更新分区，不支持隐藏分区
     *
     * dynamic：只会将相同分区内的数据清空，将结果插入
     *          对于A表没有的分区会新建分区插入数据
     *
     * */



    //只覆盖2022-10-23日期下所有小时分区数据
    spark.sql(
      """
              INSERT OVERWRITE local.iceberg_db.logs01
              SELECT *
              FROM local.iceberg_db.logs02
              WHERE cast(ts as date) = '2022-10-23'
        """).show(false)

    spark.sql(
      """
              SELECT * FROM local.iceberg_db.logs01
        """).show(false)

    spark.stop()
  }
}

