import org.apache.spark.sql.SparkSession

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
    val user = dataFrame.select(dataFrame("user_id"), dataFrame("name"), dataFrame("time") + 1)
      .where("behavior_name = 'play'")
    val schemas = Seq("userId", "gameName", "behaviorTime")
    val userData = user.toDF(schemas: _*)

    /**
     *  Read steam.csv
     *  Get columns and process data from steam.csv
     *  Create new dataframe gameData(gameName, gameTags, gameRating)
     */
    val df = spark.read.option("delimiter", ",").option("header", "true").csv("steam.csv")
    val game = df.select(df("name"), df("genres"),
      df("positive_ratings") * 10/(df("positive_ratings") + df("negative_ratings")))
    val schema = Seq("gameName", "gameTags", "gameRating")
    val gameData = game.toDF(schema: _*)

    /**
    // inner join
    val inner_join = dataFrame.join(df).where(dataFrame("playapp_name") === df("app_name"))
    //inner_join.show(false)
    // val result3 = inner_join.describe().show()  跑这个要两个半小时

    //选出inner-join里review表的几列
    val review01 = inner_join.select("user id", "playapp_name", "type", "time")
    review01.show()
    // distinct去重
    val review02 = review01.distinct()
    review02.show()

    //df转csv
    review02.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("chenye/desktop/data.csv")
**/
  }

}




