import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AlsSpec extends AnyFlatSpec with Matchers{
  behavior of "Als"

  it should "be 0.75" in {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("AlsSpec")
      .getOrCreate()

    val dataFrame = spark.read.option("delimiter", ",").option("header", "true").csv("./collaborativeFiltering/src/test/resources/test.csv")
    val result = Als.MAD(dataFrame)

    result shouldBe 0.75
    spark.stop()
  }
}
