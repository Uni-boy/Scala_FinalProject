package model

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * User Dataset: userData.csv
 *
 * userId           user_id
 * gameName         name
 * behaviorTime     time
 */
case class User(userId: Int, gameName: String, behaviorTime: Int)

/**
 * User Dataset: steam.csv
 *
 * gameName         name
 * gameTags         genres
 * ratingCount      positive_ratings+negative_ratings
 * gameRating       positive_ratings/rating_sum
 */
case class Game(gameName: String, gameTags: String, ratingCount: Long, gameRating: Double)

/**
 * @param uri MongoDB connection
 * @param db  MongoDB Database
 */
case class MongoConfig(uri: String, db: String)

object DataLoad {


}

