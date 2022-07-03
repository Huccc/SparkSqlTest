package com.huc.sparkSql1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

object Test04_Write {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val read: DataFrameReader = spark.read
    val dataFrame: DataFrame = read.json("input/user.json")
    read.csv("input/user.txt").show()

//    dataFrame.write.save("output")
//    dataFrame.write.format("json").save("output1")

    // 写出的模式有四种
    // 默认使用 报错 模式
    dataFrame.write.mode(SaveMode.Overwrite).format("json").save("output")

    // 4. 关闭sparkSession
    spark.close()
  }
}
