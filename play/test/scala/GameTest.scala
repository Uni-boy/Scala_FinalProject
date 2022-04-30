package scala

import models.Game
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import reactivemongo.api.bson.BSONDocument

import scala.util.Success

class GameTest extends AnyFlatSpec with Matchers {

  behavior of "Game"

  it should "work for Game" in {
    val game = Game.apply(1, "Call of Duty")
    game should matchPattern {
      case Game(_, _) =>
    }
  }

  it should "work for writeTry()" in {
    val x = Game.apply(1, "Call of Duty")
    Game.RateMoviesHandler.writeTry(x) should matchPattern {
      case Success(BSONDocument(_)) =>
    }
  }

  it should "work for readDocument()" in {
    val x = Game.apply(1, "Call of Duty")
    Game.RateMoviesHandler.writeTry(x) match {
      case Success(i) => {
        Game.RateMoviesHandler.readDocument(i) should matchPattern {
          case Success(Game(_, _)) =>
        }
      }
    }

  }


}
