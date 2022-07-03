package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

object Test03_Top3_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // 3. 使用sparkSession
    spark.sql(
      """
        |select
        |   c.area,c.city_name,p.product_name
        |from
        |   user_visit_action u
        |join product_info p on u.click_product_id=p.product_id
        |join city_info c on c.city_id=u.city_id
        |where u.click_product_id != -1
        |""".stripMargin).createOrReplaceTempView("t1")

    // 注册自定义函数
    spark.udf.register("remark", functions.udaf(new MyAgg))

    spark.sql(
      """
        |select
        |   area,product_name,count(*) counts,
        |   remark(city_name) cities
        |from
        |   t1
        |group by
        |   area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |   area,product_name,counts,cities,
        |   rank()over(partition by area order by counts desc) rk
        |from
        |   t2
        |""".stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select
        |   area,product_name,cities,counts
        |from
        |   t3
        |where
        |   t3.rk<=3
        |""".stripMargin).show(false)



    // 4. 关闭sparkSession
    spark.close()
  }

  case class Buffer(map: mutable.Map[String, Int])

  class MyAgg extends Aggregator[String, Buffer, String] {
    override def zero: Buffer = Buffer(new mutable.HashMap[String, Int]())

    override def reduce(b: Buffer, a: String): Buffer = {
      val map: mutable.Map[String, Int] = b.map
      map.put(a, 1 + map.getOrElse(a, 0))
      b
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      val map1: mutable.Map[String, Int] = b1.map
      val map2: mutable.Map[String, Int] = b2.map
      for (elem <- map2) {
        map1.put(elem._1, elem._2 + map1.getOrElse(elem._1, 0))
      }
      b1
    }

    override def finish(reduction: Buffer): String = {
      val map: mutable.Map[String, Int] = reduction.map
      val sum1: Int = map.values.sum
      val tuples: List[(String, Int)] = map.toList.sortWith(_._2 > _._2).take(2)
      var result: String = ""
      val sum2: Int = tuples.map(_._2).sum
      for (elem <- tuples) {
        result += elem._1 + " " + (elem._2.toDouble * 100 / sum1).formatted("%.1f") + "%,"
      }
      if (map.size > 2) {
        result += "其他 " + ((1 - sum2.toDouble / sum1) * 100).formatted("%.1f") + "%"
      }
      result
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
