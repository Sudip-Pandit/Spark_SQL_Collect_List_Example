import org.apache.spark._
import sys.process._
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext.jarOfObject
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source._

object Collect_list_Obj {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ComplexDataProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder()
      .config("fs.s3a.access.key", "")
      .config("fs.s3a.secret.key", "")
      .getOrCreate()

    val df2 = spark.read.format("json").option("multiline","true")
      .load("file:///D:/data/out1/data1.json")
    df2.show(5)
    df2.printSchema()
    val flattendf = df2.withColumn("students", expr("explode(students)"))
    flattendf.show()
    flattendf.printSchema()
    val complexdf = flattendf.groupBy("address","orgname","trainer")
      .agg(collect_list("students").alias(("students")))
    complexdf.show(5)
    complexdf.printSchema()

  }
}