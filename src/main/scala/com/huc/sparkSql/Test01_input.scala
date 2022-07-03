package com.huc.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Test01_input {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    // 读取数据
    val df: DataFrame = spark.read.json("input/user.json")

    // 可视化
    df.show()

    // 4. 关闭sparkSession,释放资源
    spark.close()
  }
}
