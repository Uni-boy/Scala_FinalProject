import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import com.mongodb.spark.MongoSpark

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

    val tb_test: DataFrame = MongoSpark.load(spark)
    tb_test.show()






  }

}