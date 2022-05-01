package scala

import org.apache.spark.sql.SparkSession
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class dataFinalSpec extends AnyFlatSpec with Matchers with MockitoSugar{
  val spark = SparkSession.builder
    .master("local[*]")
    .getOrCreate()

}
