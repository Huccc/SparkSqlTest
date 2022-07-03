package com.huc.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1.RDD 转换为DataFrame
 * 手动转换：RDD.toDF("列名1","列名2")
 * 通过样例类反射转换:UserRDD.map{x=>User(x._1,x._2)}.toDF()
 * 2.DataFrame转换为RDD
 * DataFrame.rdd
 *
 */
object Test02_RDDAndDataFrame {
  def main(args: Array[String]): Unit = {
    // todo 1 创建SparkConf配置文件，并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local[*]")
    // todo 2 利用SparkConf创建sc对象
    val sc: SparkContext = new SparkContext(conf)
    val lineRDD: RDD[String] = sc.textFile("input/user.txt")
    // 普通rdd，数据只有类型，没有列名（缺少元数据）
    val rdd: RDD[(String, Long)] = lineRDD.map({
      line => {
        val fileds: Array[String] = line.split(",")
        (fileds(0), fileds(1).toLong)
      }
    })
    // todo 3 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // RDD 和 DF、DS转换必须要导的包（隐式转换），spark指的是上面的sparkSession
    import spark.implicits._

    // todo RDD=>DF
    // 普通rdd转换成DF，需要手动为每一列补上列名（补充元数据）

    //    val df2: DataFrame = rdd.toDF()
    //    df2.show()
    val df: DataFrame = rdd.toDF("name", "age")

    rdd.collect().foreach(println)

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

    println("DF=>RDD")
    // todo DF=>RDD
    // DF 转换成RDD，直接.rdd即可，但是要注意转换出来的rdd数据类型会变成Row
    val rdd1: RDD[Row] = df.rdd
//    val value: RDD[(String, String)] = rdd1.map(row => {
//      (row.getString(0), row.getString(1))
//    })
//    value.collect().foreach(println)
    val userRDD2: RDD[Row] = userDF.rdd
    rdd1.collect().foreach(println)
    userRDD2.collect().foreach(println)

    // todo 4 关闭资源
    sc.stop()
  }
}

case class User(name: String, age: Long)