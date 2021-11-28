package indi.wuyue.spark.parser

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.{DataType, StructType}

object TestParser {

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
    val sparkSession: SparkSession = SparkSession.builder
      .appName("SparkSQLForHive")
      .master("local[*]")
      .config(sparkConf)
      .enableHiveSupport
      .withExtensions(extensions => {
//        extensions.injectParser((_, parser) => new StrictParser(parser))
//        extensions.injectParser((_, parser) => new BlankParser())
      })
      .getOrCreate
    val sql = "SELECT * FROM test_lineage_20211030.test_pre_20211030 limit 10"
    val execution = sparkSession.sessionState.executePlan(sparkSession.sessionState.sqlParser.parsePlan(sql))
    var hiveResponse: Seq[String] = SQLExecution.withNewExecutionId(sparkSession, execution){
      execution.hiveResultString()
    }
    println(hiveResponse)
    System.in.read()
  }

}

class BlankParser extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan = ???

  override def parseExpression(sqlText: String): Expression = ???

  override def parseTableIdentifier(sqlText: String): TableIdentifier = ???

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = ???

  override def parseTableSchema(sqlText: String): StructType = ???

  override def parseDataType(sqlText: String): DataType = ???
}

class StrictParser(parser: ParserInterface) extends ParserInterface {

  override def parsePlan(sqlText: String): LogicalPlan = {
    val logicalPlan = parser.parsePlan(sqlText)
    logicalPlan transform {
      case project @ Project(projectList, _) =>
        projectList.foreach {
          name =>
            if (name.isInstanceOf[UnresolvedStar]) {
              throw new RuntimeException("You must specify your project column set," +
                " * is not allowed.")
            }
        }
        project
    }
    logicalPlan
  }

  override def parseExpression(sqlText: String): Expression = ???

  override def parseTableIdentifier(sqlText: String): TableIdentifier = ???

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = ???

  override def parseTableSchema(sqlText: String): StructType = ???

  override def parseDataType(sqlText: String): DataType = ???

}