package com.huc.sparkSql1

import com.huc.sparkSql1.Test05_RDDAndDataFormat.User
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}

object Test06_DSandDF {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val read: DataFrameReader = spark.read
    val frame: DataFrame = read.json("input/user.json")
    frame.show()

    // 导入隐式转换
    import spark.implicits._

    // dataFrame=>Dataset
    val ds: Dataset[User] = frame.as[User]
    ds.show()

    // ds 转换为df
    val frame1: DataFrame = ds.toDF()

    // ds转换为rdd 类型不会丢 ，原本是什么类型，还是什么类型，不会丢类型
    val rdd: RDD[User] = ds.rdd


    // 4. 关闭sparkSession
    spark.close()
  }
}
