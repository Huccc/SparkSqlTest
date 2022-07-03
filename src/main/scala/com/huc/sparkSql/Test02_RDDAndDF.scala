package com.huc.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test02_RDDAndDF {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    // 创建一个RDD
    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("input/user.txt")

    // 对三者进行相互转换，需要使用sparkSession隐式转换
    import spark.implicits._

    // 将rdd转换为df
    // 默认情况下df只有一列value
    val dataFrame: DataFrame = rdd.toDF()

    dataFrame.show()

    // 需要对rdd进行转化为二元组
    val tupleRdd: RDD[(String, String)] = rdd.map(s => {
      val data: Array[String] = s.split(",")
      (data(0), data(1))
    })
    val dataFrame1: DataFrame = tupleRdd.toDF("name", "age")

    dataFrame1.show()

    // 将df转化为rdd
    val rdd1: RDD[Row] = dataFrame1.rdd

    for (elem <- rdd1.collect()) {
      println(elem.getString(0))
      println(elem.getString(1))
    }

    rdd1.collect().foreach(println)

    // 使用样例类相互转换
    // 如果rdd是样例类的数据类型  转化为df的时候会将样例类的属性的去成为列
    val userRdd: RDD[User] = sc.makeRDD(List(User("zhangsan", 10), User("lisi", 20)))

    val frame: DataFrame = userRdd.toDF()

    frame.show()

    // 如果使用样例类的df转换为rdd  会丢失数据类型
    val rdd2: RDD[Row] = frame.rdd

    // 4. 关闭sparkSession
    spark.close()
  }

  case class User(name: String, age: Int){}
}
