package com.spark_sql.cn

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLContext {
  def main(args: Array[String]): Unit = {

    val path = "file:///D:/people.json"

    val sparkConf = new SparkConf().setAppName("SQLContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    sc.stop()

  }
}
