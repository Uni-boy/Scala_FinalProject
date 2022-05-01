import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

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

    import spark.implicits._

    /**
     *  Read steam-200k.csv
     *  Get and merge columns from steam-200k.csv
     *  Create new dataframe userData(userId, gameName, purchase)
     */
    val dataFrame = spark.read.option("delimiter", ",").option("header", "true").csv("./src/main/resources/steam-200k.csv")
    val userData = getUser(dataFrame)

    /**
     * Read steam.csv
     * Get columns and process data from steam.csv
     * Create new dataframe gameData
     * Game Schema:
     * - gameName
     * - gameTags
     * - ratingCount
     * - gameRating
     * - id(gameId)
     */
    val df = spark.read.option("delimiter", ",").option("header", "true").csv("./src/main/resources/steam.csv")
    val gameData = getGameData(df, userData)

    /**
     * User Schema:
     * - id(gameId): Int
     * - userId: Int
     * - purchase(Preference): 0 / 1 (Double)
     */
    val userBehavior = getUserData(userData, gameData)

    /**
     * Temp Table:
     */
    val gameCount = gameData.count().toInt
    val userTemp = userBehavior.dropDuplicates("userId")
      .withColumn("__temporarily__", typedLit((0 until gameCount).toArray))
      .withColumn("id", explode($"__temporarily__"))
      .select(col("id"), col("userId"), col("purchase")*0)
      .distinct()

    val userFin = finalUserData(userTemp, userBehavior)

    /**
     * Separate userData to 70% train data, 20% validation data, and 10% test data
     */
    val splitData = userFin.orderBy(rand()).randomSplit(Array(0.7, 0.2, 0.1))
    val trainSet = splitData(0)
    val validSet = splitData(1)
    val testSet = splitData(2)

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
    MongoSpark.save(validSet.write.option("collection", "validation").mode("overwrite"))
    MongoSpark.save(testSet.write.option("collection", "test").mode("overwrite"))
    MongoSpark.save(gameData.write.option("collection", "game").mode("overwrite"))

    spark.stop()
  }

  def getUser(dataFrame: DataFrame): DataFrame ={
    val schemas = Seq("userId", "gameName", "purchase")
    val userData = dataFrame.select(dataFrame("user_id"), dataFrame("name"), dataFrame("time"))
      .where("behavior_name = 'purchase'").toDF(schemas: _*)
    userData
  }

  def getGameData(df: DataFrame, userData: DataFrame): DataFrame ={
    val schema = Seq("gameName", "gameTags", "ratingCount", "gameRating")
    val game = df.join(userData, userData("gameName") === df("name"), "leftsemi")
    var gameData = game.select(game("name"), game("genres"), game("positive_ratings") + game("negative_ratings"),
      game("positive_ratings") * 10/(game("positive_ratings") + game("negative_ratings"))).toDF(schema: _*)
    gameData = gameData.withColumn("id", monotonically_increasing_id).withColumn("id", col("id").cast(IntegerType))
    gameData
  }

  def getUserData(userData: DataFrame, gameData: DataFrame): DataFrame ={
    val user = userData.as("temp1").join(gameData.as("temp2"), userData("gameName") === gameData("gameName"), "inner")
      .select(col("temp2.id"), col("temp1.gameName"), col("temp1.userId"), col("temp1.purchase"))
    val userBehavior = user.withColumn("userId", col("userId").cast(IntegerType))
      .withColumn("purchase", col("purchase").cast(DoubleType))
    userBehavior
  }

  def finalUserData(userTemp: DataFrame, userBehavior: DataFrame): DataFrame ={
    var userFin = userTemp.as("t1").
      join(userBehavior.as("t2"), userBehavior("userId") === userTemp("userId") && userBehavior("id") === userTemp("id"), "left")
      .select(col("t1.id"), col("t1.userId"), col("t1.(purchase * 0)"), col("t2.purchase"))
      .distinct()
      .na.fill(0)
    val sch = Seq("id", "userId", "purchase")
    userFin = userFin.select(userFin("id"), userFin("userId"), userFin("(purchase * 0)")+ userFin("purchase")).toDF(sch: _*)
    userFin
  }

}




