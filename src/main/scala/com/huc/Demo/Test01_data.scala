package com.huc.Demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test01_data {
  def main(args: Array[String]): Unit = {
    // 修改程序系统的用户名称   对应访问权限
    System.setProperty("HADOOP_USER_NAME","atguigu")
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // 3. 使用sparkSession
    spark.sql("select * from user_visit_action").show(false)

    // 4. 关闭sparkSession
    spark.close()
  }
}
