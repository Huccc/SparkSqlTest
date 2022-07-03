package com.huc.Demo

import org.apache.spark.SparkConf
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test02_Top3 {
  def main(args: Array[String]): Unit = {
    // 修改程序系统的用户名称  对应的访问权限
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // 3. 使用sparkSession
    // 步骤一：过滤掉不是点击商品的数据  同时将3张表格聚合在一起   找出需要的数据
//    spark.sql("show tables").show()

    spark.sql(
      """
        |select
        |   c.area,
        |   c.city_name,
        |   p.product_name
        |from
        |   user_visit_action u
        |join
        |   city_info c
        |on
        |   u.city_id=c.city_id
        |join
        |   product_info p
        |on
        |   u.click_product_id = p.product_id
        |where
        |   u.click_product_id != -1
        |""".stripMargin).createOrReplaceTempView("t1")


    // 步骤二：聚合相同区域下的点击商品  统计次数
    spark.sql(
      """
        |select
        |   area,
        |   product_name,
        |   count(*) counts
        |from
        |   t1
        |group by
        |   area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    // 步骤三：开窗排序取top3
    spark.sql(
      """
        |select
        |   area,
        |   product_name,
        |   counts,
        |   rank()over(partition by area order by counts desc) rk
        |from
        |   t2
        |""".stripMargin).createOrReplaceTempView("t3")

    // 步骤四：取top3
    val dataFrame: DataFrame = spark.sql(
      """
        |select
        |   *
        |from
        |   t3
        |where
        |   t3.rk<=3
        |""".stripMargin)

//    val margin: String =
//      s"""
//         |select
//         |  *
//         |from
//         |  t3
//         |where
//         |  t3.rk<=3
//         |""".stripMargin


    dataFrame.show()

    // 4. 关闭sparkSession
    spark.close()
  }
}
