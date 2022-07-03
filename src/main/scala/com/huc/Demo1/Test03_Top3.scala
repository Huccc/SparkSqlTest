package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test03_Top3 {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    spark.sql(
      """
        |select
        |
        |from
        |   user
        |""".stripMargin)

    // 4. 关闭sparkSession
    spark.close()
  }
}
