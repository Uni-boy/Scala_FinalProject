import org.apache.spark.sql._
import org.apache.spark.mllib.recommendation.ALS
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions._

object Recommend{
  def recommendForUser(): Unit = {
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

    sameModel = ALS.load(spark.sparkContext, "myModel.model")
  }

}
