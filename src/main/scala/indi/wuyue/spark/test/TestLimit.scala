package indi.wuyue.spark.test

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SQLExecution

object TestLimit {

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    configuration.addResource(new Path("/Users/wuyue/Documents/tools/spark/hdfs-site.xml"))
    configuration.addResource(new Path("/Users/wuyue/Documents/tools/spark/core-site.xml"))
    configuration.addResource(new Path("/Users/wuyue/Documents/tools/spark/hive-site.xml"))
    val map: util.Iterator[util.Map.Entry[String, String]] = configuration.iterator
    val sparkConf: SparkConf = new SparkConf
    while (map.hasNext) {
      val cof: util.Map.Entry[String, String] = map.next
      sparkConf.set(cof.getKey, cof.getValue)
    }
    val sparkSession: SparkSession = SparkSession.builder.appName("SparkSQLForHive").master("local[*]").config(sparkConf).enableHiveSupport.getOrCreate
    val sql = "SELECT * FROM test_lineage_20211030.test_pre_20211030 limit 10"
    val execution = sparkSession.sessionState.executePlan(sparkSession.sessionState.sqlParser.parsePlan(sql))
    var hiveResponse: Seq[String] = SQLExecution.withNewExecutionId(sparkSession, execution){
      execution.hiveResultString()
    }
    println(hiveResponse)
    System.in.read()
  }

}
