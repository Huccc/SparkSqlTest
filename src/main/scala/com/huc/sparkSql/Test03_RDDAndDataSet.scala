package com.huc.sparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 1.RDD 转换为DataSet
 * RDD.map{x=>User(x._1,x._2)}.toDS()
 * SparkSQL 能够自动将包含有样例类的RDD转换成DataSet，样例类定义了table的结构，样例类属性通过反射变成了表的列名。
 * 样例类可以包含诸如Seq或者Array等复杂的结构。
 * 2.DataSet转换为RDD
 * DS.rdd
 */
object Test03_RDDAndDataSet {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc: SparkContext = spark.sparkContext
    // 3. 使用sparkSession
    val userRdd: RDD[User] = sc.makeRDD(List(User("zhangsan", 10), User("lisi", 20)))

    // 相互转换的时候，需要导入隐式转换
    import spark.implicits._

    val ds: Dataset[User] = userRdd.toDS()
    ds.show()

    // 将样例类的ds转换回Rdd
    // 不会丢失类型
    val rdd: RDD[User] = ds.rdd
    for (elem <- rdd.collect()) {
      println(elem.name)
      println(elem.age)
    }

    // 如果是普通类型的rdd转换为ds
    // 一般用于样例类之间的相互转化，不然意义不大
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    val ds1: Dataset[Int] = rdd1.toDS()
    ds1.show()

    // 4. 关闭sparkSession
    spark.close()
  }
}
