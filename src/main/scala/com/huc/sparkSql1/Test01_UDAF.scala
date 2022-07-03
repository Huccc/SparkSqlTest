package com.huc.sparkSql1

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoder, Encoders, SparkSession, functions}

object Test01_UDAF {
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

    // 注册一个udaf函数
    spark.udf.register("myAvg", functions.udaf(new myAvg))

    spark.sql(
      """
        |select
        |   myAvg(age)
        |from
        |   user
        |""".stripMargin).show()

    // 4. 关闭sparkSession
    spark.close()
  }

  case class Buffer(var sum: Long, var count: Long)

  class myAvg extends Aggregator[Long, Buffer, Double] {

    override def zero: Buffer = new Buffer(0, 0)

    override def reduce(b: Buffer, a: Long): Buffer = {
      b.sum += a
      b.count += 1
      b
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Buffer): Double = {
      reduction.sum.toDouble / reduction.count
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}
