package com.huc.Demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

import scala.collection.{breakOut, mutable}

object Test03_Top3 {
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

    // 注册自定义函数
    spark.udf.register("remark", functions.udaf(new MyCityRemark))

    // 步骤二：聚合相同区域下的点击商品  统计次数
    spark.sql(
      """
        |select
        |   area,
        |   product_name,
        |   count(*) counts,
        |   remark(city_name) cities
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
        |   cities,
        |   counts,
        |   rank()over(partition by area order by counts desc) rk
        |from
        |   t2
        |""".stripMargin).createOrReplaceTempView("t3")

    // 步骤四：取top3
    val dataFrame: DataFrame = spark.sql(
      """
        |select
        |   area,
        |   product_name,
        |   cities,
        |   counts
        |from
        |   t3
        |where
        |   t3.rk <= 3
        |""".stripMargin)

    dataFrame.show(false)

    // 4. 关闭sparkSession
    spark.close()
  }

  case class Buffer(map: mutable.Map[String, Long])

  class MyCityRemark extends Aggregator[String, Buffer, String] {

    // 初始化中间对象
    override def zero: Buffer = Buffer(new mutable.HashMap[String, Long]())

    // 分区内累加聚合city_name
    override def reduce(b: Buffer, key: String): Buffer = {
      // 将传入的一个城市名累加进中间对象的map中
      // 如果这个城市已经累加过了，次数加一
      // 如果没有  放入map中，次数为1
      val map: mutable.Map[String, Long] = b.map
      map.put(key, map.getOrElse(key, 0L) + 1L)
      b
    }

    // 合并多个分区累加出来的变量
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      val map1: mutable.Map[String, Long] = b1.map
      val map2: mutable.Map[String, Long] = b2.map
      // 将b2的map合并到b1的map中，之后返回b1即可
      for (elem <- map2) {
        map1.put(elem._1, elem._2 + map1.getOrElse(elem._1, 0L))
      }
      b1
    }

    // 将累加出来的中间对象，计算出中间的结果
    override def finish(reduction: Buffer): String = {
      val map: mutable.Map[String, Long] = reduction.map
      // 按照城市的点击次数进行排序  从大到小
      val tuples: List[(String, Long)] = map.toList.sortWith(_._2 > _._2).take(2)
      // 计算总计的点击次数
      val sum: Long = map.map(_._2).sum
      var result: String = ""
      // 城市的种类超过2个  有其他
      // 调用前两个城市信息
      for (elem <- tuples) {
        val city: String = elem._1
        val count: Long = elem._2
        result += city + " " + (count.toDouble * 100 / sum).formatted("%.1f") + "%" + ","
      }
      if (map.size > 2) {

        // 其他的百分比
        // 前二的百分比
        val fate: Double = tuples.map(_._2).sum.toDouble * 100 / sum
        result += "其他" + (100 - fate).formatted("%.1f") + "%"
      }
      result
    }

    // product 样例类的共同父类
    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}










