package com.huc.sparkSql1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object Test03_Read {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val read: DataFrameReader = spark.read
    val dataFrame: DataFrame = read.json("input/user.json")

    dataFrame.show()

    // 4. 关闭sparkSession
    spark.close()
  }
}
