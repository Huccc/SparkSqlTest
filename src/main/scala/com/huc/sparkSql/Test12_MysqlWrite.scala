package com.huc.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object Test12_MysqlWrite {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 创建sc
    val sc: SparkContext = spark.sparkContext
    // 3. 使用sparkSession
    val list: List[User] = List(User("zhangsan", 10), User("lisi", 20))

    val rdd: RDD[User] = sc.makeRDD(list)

    // 转化为ds，写出到mysql
    import spark.implicits._
    val ds: Dataset[User] = rdd.toDS()

    ds.show()

    val frame: DataFrame = ds.toDF()

    frame.show()

    // 写出ds到mysql
    // 如果写入的表格设定有主键，需要注意主键不能重复，否则会报错
    ds.write.format("jdbc")
      .option("url","jdbc:mysql://hadoop102:3306/gmall")
      .option("dirver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","user_info1")
      // 注意：不要在向Mysql中写数据的时候使用覆盖模式  会将写入的表整个清空
      .mode(SaveMode.Overwrite)
      .save()

    // 4. 关闭sparkSession
    spark.close()
  }
}
