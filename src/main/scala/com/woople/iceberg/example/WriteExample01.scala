package com.woople.iceberg.example

import org.apache.commons.lang3.StringUtils
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

import collection.JavaConverters._

object WriteExample01 {
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

    val tableName = "local.iceberg_db.logs01"

    //通过spark只能创建timestamp类型，此类型映射到iceberg为timestamp with timezone，即timestamptz
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS ${tableName} (
            uuid string ,
            level string ,
            ts timestamp ,
            message string)
        USING iceberg
        PARTITIONED BY (level, hours(ts))
        """)

    val table = Spark3Util.loadIcebergTable(spark, tableName)
    /**
     * table.schema()拿到的是iceberg到类型，也就是ts这列返回类型为timestamptz
     * 如果将timestamptz传给spark，spark是不支持timestamptz这种数据类型的
     * 所以必须将timestamptz转成timestamp才可以
     * */
    val schemaStr = table.schema().asStruct().fields().asScala.map(x=>s"${x.name} ${toSparkDataType(x.`type`().toString)}").mkString(",")

    spark.read
      .format("csv")
      .option("charset", "utf-8")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schemaStr)
      .load("data/logs1.csv")
      .writeTo(tableName).append()

    spark.sql(
      s"""
              select * from ${tableName}
        """).show(false)

    spark.stop()
  }

  def toSparkDataType(icebergDataType: String): String ={
    if (StringUtils.containsIgnoreCase(icebergDataType, "timestamp")){
      "timestamp"
    }else{
      icebergDataType
    }
  }
}

