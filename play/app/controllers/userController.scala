package controllers

import io.swagger.annotations.{Api, ApiOperation}
import models.{TestRepository, User}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import javax.inject.Inject
import scala.concurrent.ExecutionContext.Implicits.global

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
    testRepo.getAll.map { user =>
      Ok(Json.toJson(user))
    }
  }
}
