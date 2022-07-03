package com.huc.sparkSql1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}

/**
 * RDD与DataFrame相互转换
 */
object Test05_RDDAndDataFormat {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val read: DataFrameReader = spark.read
    val sc: SparkContext = spark.sparkContext
    val line: RDD[String] = sc.textFile("input/user.txt")
    val rdd: RDD[(String, Long)] = line.map(line => {
      val list: Array[String] = line.split(",")
      (list(0), list(1).toLong)
    })

    // RDD 和 DF、DS转换必须要导的包（隐式转换），spark指的是上面的sparkSession
    import spark.implicits._

    // 普通rdd转换成DF，需要手动为每一列补上列名（补充元数据）
    val df: DataFrame = rdd.toDF("name", "age")
    rdd.collect().foreach(println)
    println()
    df.show()

    // 样例类RDD，数据是一个个的样例类，有类型，有属性名（列名），不缺元数据
    val userRDD: RDD[User] = rdd.map({
      t => {
        User(t._1, t._2)
      }
    })

    // 样例类RDD转换DF，直接toDF转换即可，不需要补充元数据
    val userDF: DataFrame = userRDD.toDF()
    userDF.show()

    // DF=>RDD
    // DF 转换成RDD，直接 .rdd即可，但是要注意转换出来的rdd数据类型会变成Row
    val rdd1: RDD[Row] = df.rdd
    rdd.collect().foreach(println)

    val rdd2: RDD[Row] = userDF.rdd

    rdd2.collect().foreach(println)

    spark.close()
  }
  case class User(name:String,age:Long)
}
