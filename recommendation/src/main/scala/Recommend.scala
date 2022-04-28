import org.apache.spark.sql._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions._

case class UserData(userId: Int, id: Int, purchase: Double)

object Recommend{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("collaborativeFiltering")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/testdb.test")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/testdb")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

    import spark.implicits._

    val test = spark
      .read
      .format("com.mongodb.spark.sql.DefaultSource")
      .load()
      .drop("_id")
      .as[UserData]
      .rdd
      .map(
        userData => (userData.userId, userData.id, userData.purchase)
      )

    val testData = test.map {
      case (userId, id, purchase) => (userId, id)
    }

    val sameModel = MatrixFactorizationModel.load(spark.sparkContext, "../collaborativeFiltering/myModel.model")

    import org.apache.spark.mllib.recommendation.Rating

    val predictions = sameModel
      .predict(testData)
      .map {
        case Rating(userId, id, purchase) => ((userId, id), purchase)
      }

    val originAndPreds = test.map {
      case (userId, id, purchase) => ((userId, id), purchase)
    }.join(predictions)

    val originAndPredDF = originAndPreds.map {
      case ((_1, _2), (_3, _4)) => (_1, _2, _3, _4)
    }.toDF("userId", "id", "purchase", "prediction")

    val originAndPredsDF = originAndPredDF
      .withColumn("prediction",
        when(col("prediction") <= 0.12, 0.0)
          .otherwise(1.0))

    val validPredict = originAndPredsDF.drop("purchase").filter($"prediction" === 1.0)

    /**
     * Save predictions to mongoDB
     */

    MongoSpark.save(validPredict.write.option("collection", "testPred").mode("overwrite"))
  }

}
