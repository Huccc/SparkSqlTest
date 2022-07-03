package com.huc.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import java.util.Properties

object Test11_MysqlCon {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 3. 使用sparkSession
    val read: DataFrameReader = spark.read

    // todo 使用read的特定数据模式读取jdbc里面的数据
    val properties: Properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    val dataFrame: DataFrame = read.jdbc("jdbc:mysql://hadoop102:3306/gmall", "user_info", properties)

//    dataFrame.show()

    // todo 使用标准化的方式读取jdbc
    val dataFrame1: DataFrame = read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()

    dataFrame1.show()

    // 4. 关闭sparkSession
    spark.close()
  }
}
