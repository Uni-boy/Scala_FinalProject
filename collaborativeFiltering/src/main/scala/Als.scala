import org.apache.spark.sql._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.sql.types.LongType
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions._

case class User(userId: Int, id: Int, purchase: Double)

object Als {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("collaborativeFiltering")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/testdb.train")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/testdb")
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

    val originAndPred = originAndPreds.map{
      case ((_1, _2),(_3, _4)) => (_1, _2, _3, _4)
    }
    val originAndPredDF = originAndPred.toDF("userId", "id", "purchase", "prediction")

    val originAndPredsDF = originAndPredDF.withColumn("prediction",
      when(col("prediction") <= 0.1, 0.0)
        .otherwise(1.0))

    originAndPredsDF.show()

    val MSE = originAndPredsDF.select(abs(originAndPredsDF("prediction") - originAndPredsDF("purchase")))
      .toDF("product").agg(sum("product")/originAndPredsDF.count()).first.get(0)

    println(MSE)

    val validPredict = originAndPredsDF.drop("purchase")

    MongoSpark.save(validPredict.write.option("collection", "validPred").mode("overwrite"))
  }

}