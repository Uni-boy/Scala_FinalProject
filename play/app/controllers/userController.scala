package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam, ApiResponse, ApiResponses}
import javax.inject.Inject
import models.{ User, TestRepository}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

/**
  * Created by Riccardo Sirigu on 10/08/2017.
  */
@Api(value = "/Statics Recommendation")
class userController @Inject()(
  cc: ControllerComponents,
  testRepo: TestRepository) extends AbstractController(cc) {

  @ApiOperation(
    value = "Find all Users",
    response = classOf[User],
    responseContainer = "List"
  )
  def getAllUsers = Action.async {
    testRepo.getAll.map{ user =>
      Ok(Json.toJson(user))
    }
  }


  @ApiOperation(
    value = "Recommendation List for a User",
    response = classOf[User],
    responseContainer = "List"
  )
  def getAllUsers = Action.async {
    testRepo.getAll.map{ user =>
      Ok(Json.toJson(user))
    }
  }
}
