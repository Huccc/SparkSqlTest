package com.huc.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test1_Hive {
  def main(args: Array[String]): Unit = {
    // 修改程序系统的用户名称  对应访问权限
    System.setProperty("HADOOP_USER_NAME","atguigu")

    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    // 使用外部的hive，需要开启支持hive的开关
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // 3. 使用sparkSession
//    spark.sql(
//      """
//        |show tables
//        |""".stripMargin).show()

    // 读取外部hive中的表格
//    spark.sql(
//      """
//        |insert into user values(1001,'zhangsan')
//        |""".stripMargin).show()

    // 写入外部hive中的数据
//    spark.sql("insert into table score values('1004','04',40)").show()

    spark.sql("select * from score").show()

    // 4. 关闭sparkSession
    spark.close()
  }
}
