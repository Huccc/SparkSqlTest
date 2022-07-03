package com.huc.Test

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val read: DataFrameReader = spark.read
    val dataFrame: DataFrame = read.json("input/user.json")

    dataFrame.createOrReplaceTempView("user")

    spark.udf.register("myAvg", functions.udaf(new myAvg))

    spark.sql(
      """
        |select
        |   myAvg(age) as newAge
        |from
        |   user
        |""".stripMargin).show()

    // 4. 关闭sparkSession
    spark.close()
  }

  case class BUF(var sum: Int, var count: Int)

  class myAvg extends Aggregator[Int, BUF, Double] {
    override def zero: BUF = BUF(0, 0)

    override def reduce(b: BUF, a: Int): BUF = {
      b.sum += a
      b.count += 1
      b
    }

    override def merge(b1: BUF, b2: BUF): BUF = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: BUF): Double = {
      reduction.sum.toDouble / reduction.count
    }

    override def bufferEncoder: Encoder[BUF] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
}
