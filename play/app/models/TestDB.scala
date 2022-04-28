package models

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import play.api.libs.json.{Json, OFormat}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import javax.inject.Inject
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Success

case class User(userId: Int, id: Int, purchase: Double)

case class Prediction(userId: Int, id: Int, prediction: Double)

object Prediction {
  implicit val userFormat: OFormat[Prediction] = Json.format[Prediction]

  implicit object RateMoviesHandler extends BSONDocumentWriter[Prediction] with BSONDocumentReader[Prediction] {
    def writeTry(t: Prediction) = Success(BSONDocument(
      "userId" -> t.userId,
      "id" -> t.id,
      "prediction" -> t.prediction
    ))

    def readDocument(doc: BSONDocument) = for {
      userId <- doc.getAsTry[Int]("userId")
      id <- doc.getAsTry[Int]("id")
      purchase <- doc.getAsTry[Int]("prediction")
    } yield Prediction(userId, id, purchase)
  }
}

object User {
  implicit val userFormat: OFormat[User] = Json.format[User]

  implicit object RateMoviesHandler extends BSONDocumentWriter[User] with BSONDocumentReader[User] {
    def writeTry(t: User) = Success(BSONDocument(
      "userId" -> t.userId,
      "id" -> t.id,
      "purchase" -> t.purchase
    ))

    def readDocument(doc: BSONDocument) = for {
      userId <- doc.getAsTry[Int]("userId")
      id <- doc.getAsTry[Int]("id")
      purchase <- doc.getAsTry[Int]("purchase")
    } yield User(userId, id, purchase)
  }
}

class TestRepository @Inject()(
                                implicit ec: ExecutionContext,
                                reactiveMongoApi: ReactiveMongoApi) {

  import reactivemongo.play.json.compat
  import compat.json2bson._

  private def userCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("test"))

  private def predCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("testPred"))

  private def gameCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("game"))


  def getAll: Future[Seq[User]] =
    userCollection.flatMap(_.find(BSONDocument.empty).
      cursor[User]().collect[Seq](100))

  def getRecommendGameId(id: Int) = {
    println(id)
    val gameIds = predCollection.flatMap(_.find(BSONDocument("userId" -> id, "prediction" -> 1)).
      cursor[Prediction]().collect[Seq](100)).map(users => users.map(user => user.id))
    Await.result(gameIds, 5.second)
  }

  def getAllUnpurchaseGames(id: Int) = {
    val ungameIds = userCollection.flatMap(_.find(BSONDocument("userId" -> id, "purchase" -> 0)).
      cursor[User]().collect[Seq](100)).map(users => users.map(user => user.id))
    Await.result(ungameIds, 5.second)
  }

  def update(userId: Int, gameId: Int) = {
    val selector = BSONDocument("userId" -> userId, "id" -> gameId)
    val modifier = BSONDocument("purchase" -> 1)
    val futureUpdate1 = userCollection.map {
      userColl =>
        userColl.update
          .one(
            q = selector
            ,
            u = modifier
            ,
            upsert = false
            ,
            multi = false
          )
    }
  }

  def find(id: Int): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("collaborativeFiltering")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/testdb.test")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/testdb.test")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

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

    val sameModel = MatrixFactorizationModel.load(spark.sparkContext, "../collaborativeFiltering/myModel.model")

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

    spark.stop()

  }



  //  def getMovie(uid: Int): Future[Option[User]] = {
  //    userCollection.flatMap(_.find(BSONDocument("id" -> id)).one[User])
  //  }
  //
  //  def getMovieGMN(mid: Int): Future[Option[MovieGMN]] = {
  //    movieCollection.flatMap(_.find(BSONDocument("mid" -> mid)).one[MovieGMN])
  //  }

}
