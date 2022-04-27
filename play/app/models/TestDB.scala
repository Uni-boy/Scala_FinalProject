package models

import javax.inject.Inject
import play.api.libs.json.{Json, OFormat}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.bson.{BSONDocument, BSONDocumentReader, BSONDocumentWriter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
 * Created by Riccardo Sirigu on 10/08/2017.
 */
case class User( userId: Int, id: Int, purchase: Int)

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

  import reactivemongo.play.json.compat,
  compat.json2bson._

  private def userCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("test"))

  private def predCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("testPred"))

  private def gameCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection[BSONCollection]("game"))


  def getAll: Future[Seq[User]] =
    userCollection.flatMap(_.find(BSONDocument.empty).
      cursor[User]().collect[Seq](100))

  def getRecommendGameId(id : Int)  = {
    val gameIds = userCollection.flatMap(_.find(BSONDocument("userId" -> id, "prediction" -> 1)).
       cursor[User]().collect[Seq](100)).map(users => users.map(user => user.id))
    gameIds
  }



//  def getMovie(uid: Int): Future[Option[User]] = {
//    userCollection.flatMap(_.find(BSONDocument("id" -> id)).one[User])
//  }
//
//  def getMovieGMN(mid: Int): Future[Option[MovieGMN]] = {
//    movieCollection.flatMap(_.find(BSONDocument("mid" -> mid)).one[MovieGMN])
//  }

}
