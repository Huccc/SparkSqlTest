package com.huc.sparkSql1

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoder, Encoders, SparkSession, functions}

object Test01_UDAF1 {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val read: DataFrameReader = spark.read
    val dataFrame: DataFrame = read.json("input/user.json")

    // 创建视图
    dataFrame.createOrReplaceTempView("user1")

    // 注册函数
    spark.udf.register("myAvg1", functions.udaf(new myAvg1))

    spark.sql(
      """
        |select
        |   myAvg1(age) as age
        |from
        |   user1
        |""".stripMargin).show()

    // 4. 关闭sparkSession
    spark.close()
  }

  case class Buffer1(var sum: Int, var count: Int)

  class myAvg1 extends Aggregator[Int, Buffer1, Double] {
    override def zero: Buffer1 = Buffer1(0, 0)

    override def reduce(b: Buffer1, a: Int): Buffer1 = {
      b.sum += a
      b.count += 1
      b
    }

    override def merge(b1: Buffer1, b2: Buffer1): Buffer1 = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Buffer1): Double = {
      reduction.sum.toDouble / reduction.count
    }

    override def bufferEncoder: Encoder[Buffer1] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
}
