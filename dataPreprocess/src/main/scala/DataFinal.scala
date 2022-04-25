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
    val dataFrame = spark.read.option("delimiter", ",").option("header", "true").csv("./src/main/resources/steam-200k.csv")
    val schemas = Seq("userId", "gameName", "behaviorTime")
    val userData = dataFrame.select(dataFrame("user_id"), dataFrame("name"), dataFrame("time") + 1)
      .where("behavior_name = 'play'").toDF(schemas: _*)

    /**
     *  Read steam.csv
     *  Get columns and process data from steam.csv
     *  Create new dataframe gameData(gameName, gameTags, gameRating, ID)
     */
    import org.apache.spark.sql.types.IntegerType

    val df = spark.read.option("delimiter", ",").option("header", "true").csv("./src/main/resources/steam.csv")
    val schema = Seq("gameName", "gameTags", "ratingCount", "gameRating")
    var gameData = df.select(df("name"), df("genres"), df("positive_ratings") + df("negative_ratings"),
      df("positive_ratings") * 10/(df("positive_ratings") + df("negative_ratings"))).toDF(schema: _*)
    gameData = gameData.withColumn("id", monotonically_increasing_id).withColumn("id", col("id").cast(IntegerType))

    /**
     * TrainSet and TestSet Schema:
     * - id(gameId)
     * - gameName
     * - userId
     * - behaviorTime(Preference)
     */
    val user = userData.as("temp1").join(gameData.as("temp2"), userData("gameName") === gameData("gameName"), "inner")
      .select(col("temp2.id"), col("temp1.gameName"), col("temp1.userId"), col("temp1.behaviorTime"))
    val userBehavior = user.withColumn("userId", col("userId").cast(IntegerType))
    userBehavior.show()

    /**
     * Game Schema:
     * - gameName
     * - gameTags
     * - ratingCount
     * - gameRating
     * - id(gameId)
     */
    gameData = gameData.join(userData, userData("gameName") === gameData("gameName"), "leftsemi")
    gameData.show()

    println(userBehavior.count()) //result: 36302 (valid user data)
    println(gameData.count()) //result: 1724  (valid game data)

    /**
     * Separate userData to 70% train data and 30% test data
     */
    val splitData = userBehavior.orderBy(rand()).randomSplit(Array(0.7, 0.3))
    val trainSet = splitData(0)
    val testSet = splitData(1)

    /**
     * save dataframe to mongoDB:
     * Training.collection   userData to train
     * Test.collection       userData to test
     * Game.collection       gameData
     */
    import com.mongodb.spark._
    import com.mongodb.spark.config._
    import org.bson.Document

    MongoSpark.save(trainSet.write.option("collection", "train").mode("overwrite"))
    MongoSpark.save(testSet.write.option("collection", "test").mode("overwrite"))
    MongoSpark.save(gameData.write.option("collection", "game").mode("overwrite"))

  }

}




