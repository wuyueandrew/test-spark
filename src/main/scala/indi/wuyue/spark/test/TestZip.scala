package indi.wuyue.spark.test

import org.apache.spark.sql.SparkSession


object TestZip {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("TestUI").master("local[*]").getOrCreate
    import sparkSession.implicits._
    val df = sparkSession.sparkContext.parallelize(Seq(1,2,3)).toDF()
    System.in.read()
  }

}
