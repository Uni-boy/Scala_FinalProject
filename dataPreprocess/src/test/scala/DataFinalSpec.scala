import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataFinalSpec extends AnyFlatSpec with Matchers{

  behavior of "DataFinal"
  it should "row count is one" in {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataFinalSpec")
      .getOrCreate()

    val dataFrame = spark.read.option("delimiter", ",").option("header", "true").csv("./dataPreprocess/src/test/resources/test.csv")
    val user = DataFinal.getUser(dataFrame).count()

    user shouldBe 1
    spark.stop()
  }

  it should "column count is five" in {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataFinalSpec")
      .getOrCreate()

    val dataFrame1 = spark.read.option("delimiter", ",").option("header", "true").csv("./dataPreprocess/src/test/resources/test1.csv")
    val dataFrame2 = spark.read.option("delimiter", ",").option("header", "true").csv("./dataPreprocess/src/test/resources/test2.csv")
    val game = DataFinal.getGameData(dataFrame1, dataFrame2)
    val num = game.columns.size

    num shouldBe 5
    spark.stop()
  }

  it should "column count is four" in {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataFinalSpec")
      .getOrCreate()

    val dataFrame2 = spark.read.option("delimiter", ",").option("header", "true").csv("./dataPreprocess/src/test/resources/test2.csv")
    val dataFrame3 = spark.read.option("delimiter", ",").option("header", "true").csv("./dataPreprocess/src/test/resources/test3.csv")
    val user = DataFinal.getUserData(dataFrame2, dataFrame3)
    val num = user.columns.size

    num shouldBe 4
    spark.stop()
  }

  it should "row count is two" in {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DataFinalSpec")
      .getOrCreate()

    val dataFrame4 = spark.read.option("delimiter", ",").option("header", "true").csv("./dataPreprocess/src/test/resources/test4.csv")
    val dataFrame5 = spark.read.option("delimiter", ",").option("header", "true").csv("./dataPreprocess/src/test/resources/test5.csv")
    val user = DataFinal.finalUserData(dataFrame4, dataFrame5)
    val num = user.count()

    num shouldBe 2
    spark.stop()
  }
}
