package indi.wuyue.spark.test

import org.apache.spark.sql.SparkSession


object TestUI {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("TestUI").master("local[*]").getOrCreate
    import sparkSession.implicits._
    val df = sparkSession.sparkContext.parallelize(1 to 100).toDF().count()
    System.in.read()
  }

}
