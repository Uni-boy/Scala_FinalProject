import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFinal{
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("dataPreprocess")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/testdb")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/testdb")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
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
     * Separate userData to 70% train data and 30% test data
     */
    val splitData = userData.randomSplit(Array(0.7, 0.3))
    val trainingSet = splitData(0)
    val testSet = splitData(1)

    /**
    var gameData_rate = gameData.orderBy(desc("gameRating"))
    //gameData_rate = gameData_rate.groupBy("gameTags")
    gameData_rate.show()
    var gameData_player = gameData.orderBy(desc("ratingCount"))
    //gameData_player = gameData_player.groupBy("gameTags")
    gameData_player.show()
     */

    /**
     * save dataframe to mongoDB
     */
    import com.mongodb.spark._
    import com.mongodb.spark.config._
    import org.bson.Document

    MongoSpark.save(trainingSet.write.option("collection", "training").mode("overwrite"))
    println("Reading from the 'training' collection:")
    //MongoSpark.load[Character](SparkSession, ReadConfig(Map("collection" -> "training"), Some(ReadConfig(SparkSession)))).show()

  }

}




