package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.SparkSession
import org.slf4j.impl.StaticLoggerBinder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.catalyst.plans.Inner
import org.sparkproject.dmg.pmml.True

class DataFinal extends AnyFlatSpec with Matchers {

  // spark初始化
  val spark: SparkSession = SparkSession
    .builder()
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  // read csv
  val dataFrame = spark.read.option("delimiter", ",").option("header", "true").csv("src/main/resources/steam-200k.csv")
  dataFrame.show()
  val result1 = dataFrame.describe().show()

  val df = spark.read.option("delimiter", ",").option("header", "true").csv("src/main/resources/dataset.csv")
  df.show()
  val result2 = df.describe().show()

  // inner join
  val inner_join = dataFrame.join(df).where(dataFrame("playapp_name") ===  df("app_name"))
  //inner_join.show(false)
  // val result3 = inner_join.describe().show()  跑这个要两个半小时

  //选出inner-join里review表的几列
  val review01 = inner_join.select("user id","playapp_name","type","time")
  review01.show()
  // distinct去重
  val review02 = review01.distinct()
  review02.show()

  //df转csv
  review02.coalesce(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("chenye/desktop/data.csv")

}




