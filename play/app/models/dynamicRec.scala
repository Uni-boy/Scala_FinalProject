package models

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import play.modules.reactivemongo.ReactiveMongoApi

import javax.inject.Inject
import scala.concurrent.ExecutionContext

case class UserData(userId: Int, id: Int, purchase: Double)

class dynamicRec @Inject()(
                            implicit ec: ExecutionContext,
                            reactiveMongoApi: ReactiveMongoApi) {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("collaborativeFiltering")
    .master("local[*]")
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/testdb.test")
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/testdb.test")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  val sameModel = MatrixFactorizationModel.load(spark.sparkContext, "../collaborativeFiltering/myModel.model")

  def find(id: Int): Unit = {
    val test = spark
      .read
      .format("com.mongodb.spark.sql.DefaultSource")
      .load()

    import spark.implicits._

    val filter = test
      .filter($"userId" === id)
      .as[User]
      .rdd
      .map(
        userData => (userData.userId, userData.id, userData.purchase)
      )

    val testData = filter.map {
      case (userId, id, purchase) => (userId, id)
    }

    import org.apache.spark.mllib.recommendation.Rating

    val predictions = sameModel
      .predict(testData)
      .map {
        case Rating(userId, id, purchase) => ((userId, id), purchase)
      }

    val originAndPreds = filter.map {
      case (userId, id, purchase) => ((userId, id), purchase)
    }.join(predictions)

    val originAndPredDF = originAndPreds.map {
      case ((_1, _2), (_3, _4)) => (_1, _2, _3, _4)
    }.toDF("userId", "id", "purchase", "prediction")

    /**
     * Prediction result for specific userId
     */
    val originAndPredsDF = originAndPredDF
      .withColumn("prediction",
        when(col("prediction") <= 0.12, 0.0)
          .otherwise(1.0))

    val testPredict = originAndPredsDF.drop("purchase")

    import com.mongodb.spark._

    MongoSpark.save(testPredict.write.option("collection", "testPred").mode("append"))

  }

}