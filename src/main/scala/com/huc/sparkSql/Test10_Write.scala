package com.huc.sparkSql

import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

object Test10_Write {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3. 使用sparkSession
    val read: DataFrameReader = spark.read
    val dataframe: DataFrame = read.json("input/user.json")

    // 默认的写出格式是snappy加parquet  有压缩 有列式存储
//    dataframe.write.save("output")

    // 4.2 format指定保存数据类型
    // df.write.format("…")[.option("…")].save("…")
    // format("…")：指定保存的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"text"。
    // save ("…")：在"csv"、"orc"、"parquet"和"text"(单列DF)格式下需要传入保存数据的路径。
    // option("…")：在"jdbc"格式下需要传入JDBC相应参数，url、user、password和dbtable

//    dataframe.write.format("json").save("output2")

    // 写出的模式有四种
    // 默认使用 报错 模式
//    dataframe.write.mode(SaveMode.ErrorIfExists)

    // 追加模式  重新写一个新的文件  推荐在mysql里面写的时候
    dataframe.write.mode(SaveMode.Append).format("json").save("output")

    // 覆盖模式  删除原有的文件夹  重新生成数据
    // 推荐在读取原始数据  进行sparkSQL分析计算完后  写入到新的文件夹时使用
    dataframe.write.mode(SaveMode.Overwrite).format("json").save("output2")

    // 忽略模式  如果写出的文件夹已经存在  不执行操作
    dataframe.write.mode(SaveMode.Ignore).format("json").save("output")

    // 4. 关闭sparkSession
    spark.close()
  }
}









