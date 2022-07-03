package com.huc.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoder, Encoders, SparkSession, functions}

object Test08_UDAF {
  def main(args: Array[String]): Unit = {
    // 1.创建一个SparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSql").setMaster("local[*]")

    // 2.创建一个SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3.使用sparkSession
    val read: DataFrameReader = spark.read
    val dataFrame: DataFrame = read.json("input/user.json")

    // 创建视图
    dataFrame.createOrReplaceTempView("user")

    // 注册一个udaf函数
    spark.udf.register("myAvg", functions.udaf(new myAvg))

    // 使用自定义函数
    spark.sql(
      """
        |select
        |   myAvg(age)
        |from
        |   user
        |""".stripMargin).show()

    // 4.关闭SparkSession
    spark.close()
  }

  case class Buffer(var sum: Long, var count: Long)

  // 3个泛型
  // 第一个参数是进入的数据类型
  // 第二个参数是中间保存累加数据的缓存
  // 第三个参数是最终的计算结果
  class myAvg extends Aggregator[Long, Buffer, Double] {

    // 不需要本体，会自己找个地方缓存（和累加器的区别）
    // 初始化中间数据       // 调用了apply方法，或者new Buffer()
    override def zero: Buffer = Buffer(0, 0)

    // 分区内累加每一行数据
    override def reduce(b: Buffer, a: Long): Buffer = {
      b.sum += a
      b.count += 1
      b
    }

    // 分区间合并多个buffer
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    // 计算结果最后的逻辑
    override def finish(reduction: Buffer): Double = {
      reduction.sum.toDouble / reduction.count
    }

    // SparkSQL对传递的对象的序列化操作（编码）
    // 自定义类型就是product  自带类型根据类型选择
    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    // 输出的类型
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
}
