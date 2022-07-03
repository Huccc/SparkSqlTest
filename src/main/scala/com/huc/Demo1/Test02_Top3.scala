package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test02_Top3 {
  def main(args: Array[String]): Unit = {

    //
    System.setProperty("HADOOP_USER_NAME","atguigu")

    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // 3. 使用sparkSession
    spark.sql(
      """
        |select
        |   c.area,c.city_id,p.product_name
        |from
        |   user_visit_action u
        |join
        |   product_info p
        |on
        |   u.click_product_id=p.product_id
        |join
        |   city_info c
        |on
        |   u.city_id=c.city_id
        |where
        |   u.click_product_id != -1
        |
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |   area,product_name,count(*) count
        |from
        |   t1
        |group by
        |   area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |area,product_name,count,
        |rank()over(partition by area order by count desc) rk
        |from
        |   t2
        |
        |""".stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select
        |   area,product_name,count,rk
        |from
        |   t3
        |where
        |   rk<=3
        |""".stripMargin).show()

    // 4. 关闭sparkSession
    spark.close()
  }
}
