package com.huc.sparkSql1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object Test02_UDF {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val read: DataFrameReader = spark.read
    val dataFrame: DataFrame = read.json("input/user.json")

    // 创建视图
    dataFrame.createOrReplaceTempView("user")

    // 注册
    spark.udf.register("change",(name:String)=>s"$name gongzi")

    spark.sql(
      """
        |select
        |   change(name) as newname,
        |   age
        |from
        |   user
        |""".stripMargin).show()

    // 4. 关闭sparkSession
    spark.close()
  }
}
