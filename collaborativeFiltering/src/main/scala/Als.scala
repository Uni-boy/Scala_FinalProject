import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.sql.types.LongType
import com.mongodb.spark.MongoSpark

case class User(id: Int, userId: Int, behaviorTime: Double)

object Als{
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
      .drop("gameName")
      .as[User]
      .rdd
      .map(user => (user.id, user.userId, user.behaviorTime))

    import org.apache.spark.mllib.recommendation.Rating

    val trainData = train.map(x => Rating(x._1, x._2, x._3))

    val (rank, iterations, lambda) = (150, 5, 0.01)
    val model = ALS.train(trainData, rank, iterations, lambda)

    val test = spark.read.format("com.mongodb.spark.sql.DefaultSource")
      .option("uri", "mongodb://localhost:27017/testdb.test")
      .load()
      .drop("_id")
      .drop("gameName")
      .as[User]
      .rdd
      .map(user => (user.id, user.userId, user.behaviorTime))



  }

}