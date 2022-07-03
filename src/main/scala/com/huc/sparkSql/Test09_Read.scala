package com.huc.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object Test09_Read {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val reader: DataFrameReader = spark.read

    // 直接读取特定类型的文件
    reader.csv("input/user.txt").show()
    reader.json("input/user.json").show()

    // spark默认的是列式存储的文件
    // 标准化读取数据
    // spark.read.format("…")[.option("…")].load("…")
    // format("…")：指定加载的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"text"
    // load("…")：在"csv"、"jdbc"、"json"、"orc"、"parquet"和"text"格式下需要传入加载数据路径
    // option("…")：在"jdbc"格式下需要传入JDBC相应参数，url、user、password和dbtable
    val frame: DataFrame = reader.format("json").load("input/user.json")

    val dataFrame: DataFrame = reader.format("csv").load("input/user.txt")

    dataFrame.show()

    frame.show()


    // 4. 关闭sparkSession
    spark.close()
  }
}
