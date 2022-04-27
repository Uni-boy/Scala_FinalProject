package models

import play.api.libs.json.{Json, OFormat}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import javax.inject.Inject
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Success

/**
 * Created by Riccardo Sirigu on 10/08/2017.
 */
case class Game(id: Int, gameName: String)

object Game {
  implicit val userFormat: OFormat[Game] = Json.format[Game]

  implicit object RateMoviesHandler extends BSONDocumentWriter[Game] with BSONDocumentReader[Game] {
    def writeTry(t: Game) = Success(BSONDocument(
      "id" -> t.id,
      "gameName" -> t.gameName
    ))

    def readDocument(doc: BSONDocument) = for {
      gameId <- doc.getAsTry[Int]("id")
      gameName <- doc.getAsTry[String]("gameName")
    } yield Game(gameId, gameName)
  }
}

class gameRepository @Inject()(
                                implicit ec: ExecutionContext,
                                reactiveMongoApi: ReactiveMongoApi) {

  import reactivemongo.play.json.compat
  import compat.json2bson._

  private def gameCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("game"))


  def getGame(id: Int): Game = {
    val eventualMaybeGame = gameCollection.flatMap(_.find(BSONDocument("id" -> id)).one[Game])
    Await.result(eventualMaybeGame, 5.second) match {
      case Some(v) => v
      case None => Game.apply(0, "")
    }
  }

}
