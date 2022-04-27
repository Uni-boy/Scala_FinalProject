package models

import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.commands.WriteResult

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by Riccardo Sirigu on 10/08/2017.
 */
case class Todo(
                 _id: Option[UUID], // Avoid using BSONObjectID, not to couple model with DB
                 title: String,
                 completed: Option[Boolean])

object Todo {

  import play.api.libs.json._

  implicit val todoFormat: OFormat[Todo] = Json.format[Todo]
}

class TodoRepository @Inject()(
                                implicit ec: ExecutionContext,
                                reactiveMongoApi: ReactiveMongoApi) {

  import reactivemongo.play.json.compat
  import compat.json2bson._

  private def todosCollection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection("todos"))

  def getAll: Future[Seq[Todo]] =
    todosCollection.flatMap(_.find(BSONDocument.empty).
      cursor[Todo]().collect[Seq](100))

  def getTodo(id: UUID): Future[Option[Todo]] =
    todosCollection.flatMap(_.find(BSONDocument("_id" -> id)).one[Todo])

  def addTodo(todo: Todo): Future[WriteResult] =
    todosCollection.flatMap(_.insert.one(
      todo.copy(_id = Some(UUID.randomUUID()))))

  def updateTodo(id: UUID, todo: Todo): Future[Option[Todo]] = {
    val updateModifier = BSONDocument(
      f"$$set" -> BSONDocument(
        "title" -> todo.title,
        "completed" -> todo.completed)
    )

    todosCollection.flatMap(_.findAndUpdate(
      selector = BSONDocument("_id" -> id),
      update = updateModifier,
      fetchNewObject = true).map(_.result[Todo])
    )
  }

  def deleteTodo(id: UUID): Future[Option[Todo]] =
    todosCollection.flatMap(_.findAndRemove(
      selector = BSONDocument("_id" -> id)).map(_.result[Todo]))
}
