package com.huc.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test05_UDF {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val dataFrame: DataFrame = spark.read.json("user.json")

    // 创建视图
    dataFrame.createOrReplaceTempView("user")

    // 编写sql查询数据
    val dataFrame1: DataFrame = spark.sql(
      """
        |select
        | name,
        | age
        |from
        | user
        |where
        | age<20
        |""".stripMargin)

    dataFrame1.createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        | *
        |from
        | t1
        |""".stripMargin).createOrReplaceTempView("t2")



    dataFrame1.show()

    // 4. 关闭sparkSession
    spark.close()
  }
}
