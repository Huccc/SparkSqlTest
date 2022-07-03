package com.huc.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test06_CustomUDF {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val dataFrame: DataFrame = spark.read.json("input/user.json")

    // 创建视图
    dataFrame.createOrReplaceTempView("user")

    // 需要先注册自定义的udf函数
    spark.udf.register("changeStr", (name: String) => s"$name gongzi")

    spark.sql(
      """
        |select
        |   changeStr(name) as newName,
        |   age
        |from
        |   user
        |""".stripMargin).show()




    // 4. 关闭sparkSession
    spark.close()
  }
}
