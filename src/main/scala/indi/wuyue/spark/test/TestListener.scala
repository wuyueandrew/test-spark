package indi.wuyue.spark.test

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

object TestListener {

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
    sparkConf.set("spark.sql.queryExecutionListeners",classOf[TestListener].getName)
    sparkConf.set("spark.sql.queryExecutionListeners",classOf[TestListener].getName)
    val sparkSession: SparkSession = SparkSession.builder.appName("SparkSQLForHive").master("local[*]").config(sparkConf).enableHiveSupport.getOrCreate
    val sql = "INSERT OVERWRITE TABLE test_lineage_20211109.test_next_20211109 PARTITION (day_no = 20211021) SELECT id, log_level FROM test_lineage_20211109.test_pre_20211109 WHERE day_no = 20211021 AND log_level = 'INFO'"
    //    sparkSession.sql(sql).show()
    val df = sparkSession.sql(sql)
    //    df.collect()
    //    sparkSession.sparkContext.addSparkListener()
    sparkSession.sparkContext.applicationAttemptId
    val obj = df.show()
//    System.in.read()
  }

}
class TestListener extends SparkListener with QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    println("=====onSuccess=====")
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("=====onApplicationEnd=====")
  }

}
