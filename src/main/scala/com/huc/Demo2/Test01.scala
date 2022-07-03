package com.huc.Demo2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val read: DataFrameReader = spark.read

    val dataFrame: DataFrame = read.json("input/user.json")

    // 创建视图
    dataFrame.createOrReplaceTempView("user")

    spark.sql(
      """
        |select
        |  *
        |from
        |  user
        |""".stripMargin).show()

    spark.close()
  }
}
