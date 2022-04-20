import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFinal{
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("dataPreprocess")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

    /**
     *  Read steam-200k.csv
     *  Get and merge columns from steam-200k.csv
     *  Create new dataframe userData(userId, gameName, behaviorTime)
     */
    val dataFrame = spark.read.option("delimiter", ",").option("header", "true").csv("steam-200k.csv")
    val schemas = Seq("userId", "gameName", "behaviorTime")
    var userData = dataFrame.select(dataFrame("user_id"), dataFrame("name"), dataFrame("time") + 1)
      .where("behavior_name = 'play'").toDF(schemas: _*)

    /**
     *  Read steam.csv
     *  Get columns and process data from steam.csv
     *  Create new dataframe gameData(gameName, gameTags, gameRating)
     */
    val df = spark.read.option("delimiter", ",").option("header", "true").csv("steam.csv")
    val schema = Seq("gameName", "gameTags", "ratingCount", "gameRating")
    var gameData = df.select(df("name"), df("genres"), df("positive_ratings") + df("negative_ratings"),
      df("positive_ratings") * 10/(df("positive_ratings") + df("negative_ratings"))).toDF(schema: _*)

    userData = userData.join(gameData, userData("gameName") === gameData("gameName"), "leftsemi")
    userData.show()
    gameData = gameData.join(userData, userData("gameName") === gameData("gameName"), "leftsemi")
    gameData.show()
    println(userData.count()) //result: 36289 (valid user data)
    println(gameData.count()) //result: 1724  (valid game data)

    /**
     * TODO: separate userData to 70% train data and 30% test data
     */

    /**
     *
     */
    var gameData_rate = gameData.orderBy(desc("gameRating"))
    //gameData_rate = gameData_rate.groupBy("gameTags")
    gameData_rate.show()
    var gameData_player = gameData.orderBy(desc("ratingCount"))
    //gameData_player = gameData_player.groupBy("gameTags")

    /**
    //df转csv
    review02.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("chenye/desktop/data.csv")
    **/

  }

}




