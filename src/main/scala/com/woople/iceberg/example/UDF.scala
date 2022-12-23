package com.woople.iceberg.example

object UDF {
  def concat(str: String, prefix: String): String ={
    prefix + str
  }

}
