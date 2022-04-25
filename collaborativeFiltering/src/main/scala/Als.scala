import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.sql.types.LongType
import com.mongodb.spark.MongoSpark

case class User(userId: Int, id: Int, purchase: Double)

object Als {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("collaborativeFiltering")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/testdb.train")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/testdb.train")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

    import spark.implicits._

    val train = MongoSpark
      .load(spark)
      .drop("_id")
      .as[User]
      .rdd
      .map(user => (user.userId, user.id, user.purchase))

    import org.apache.spark.mllib.recommendation.Rating

    val trainData = train.map(x => Rating(x._1, x._2, x._3))

    val model = new ALS()
      .setRank(100)
      .setIterations(5)
      .setLambda(0.01)
      .run(trainData)

    val test = spark.read.format("com.mongodb.spark.sql.DefaultSource")
      .option("uri", "mongodb://localhost:27017/testdb.validation")
      .load()
      .drop("_id")
      .as[User]
      .rdd
      .map(user => (user.userId, user.id, user.purchase))

    val testData = test.map {
      case (userId, id, purchase) => (userId, id)
    }

    val predictions = model.predict(testData).map {
      case Rating(userId, id, purchase) => ((userId, id), purchase)
    }

    val originAndPreds = test.map{
      case (userId, id, purchase) => ((userId, id), purchase)
    }.join(predictions)

    val originAndPredsDF = originAndPreds.toDF

    originAndPredsDF.show()

    val MSE = originAndPreds.map { case ((userid, id), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    println(MSE)

  }

}