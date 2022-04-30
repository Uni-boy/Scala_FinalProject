package scala

import models.{Prediction, User}
import org.scalatest.{FlatSpec, Matchers}
import reactivemongo.api.bson.BSONDocument

import scala.util.Success

class TestDBTest extends FlatSpec with Matchers {

  behavior of "User"

  it should "work for Game" in {
    val x = User.apply(33, 44, 1)
    x should matchPattern {
      case User(_, _, _) =>
    }
  }

  it should "work for writeTry()" in {
    val x = User.apply(33, 44, 1)
    User.RateMoviesHandler.writeTry(x) should matchPattern {
      case Success(BSONDocument(_)) =>
    }
  }

  it should "work for readDocument()" in {
    val x = User.apply(33, 44, 1)
    User.RateMoviesHandler.writeTry(x) match {
      case Success(i) => {
        User.RateMoviesHandler.readDocument(i) should matchPattern {
          case Success(User(_, _, _)) =>
        }
      }
    }
  }

  behavior of "Prediction"

  it should "work for Game" in {
    val x = Prediction.apply(33, 44, 1)
    x should matchPattern {
      case Prediction(_, _, _) =>
    }
  }

  it should "work for writeTry()" in {
    val x = Prediction.apply(33, 44, 1)
    Prediction.RateMoviesHandler.writeTry(x) should matchPattern {
      case Success(BSONDocument(_)) =>
    }
  }

  it should "work for readDocument()" in {
    val x = Prediction.apply(33, 44, 1)
    Prediction.RateMoviesHandler.writeTry(x) match {
      case Success(i) => {
        Prediction.RateMoviesHandler.readDocument(i) should matchPattern {
          case Success(Prediction(_, _, _)) =>
        }
      }
    }
  }
}