package com.huc.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Test04_DSAndDF {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val dataframe: DataFrame = spark.read.json("input/user.json")
    dataframe.show()

    // 导入隐式转换
    // DataFrame=>Dataset
    import spark.implicits._
    // df的列名需要和ds的属性名对应上才能进行转换
    val ds: Dataset[User] = dataframe.as[User]
    ds.show()

    // ds转换为df
    val dataFrame1: DataFrame = ds.toDF()

    // ds转换为rdd 类型不会丢，原本什么类型，还是什么类型，不会丢类型
    val rdd: RDD[User] = ds.rdd

    // 4. 关闭sparkSession
    spark.close()
  }
}
