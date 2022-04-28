package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam}
import models.{TestRepository, User, gameRepository}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import javax.inject.Inject

@Api(value = "/Recommendation")
class userController @Inject()(
                                cc: ControllerComponents,
                                testRepo: TestRepository, gameRepo: gameRepository) extends AbstractController(cc) {

  @ApiOperation(
    value = "Find all unpurchased games of the specific user",
    response = classOf[User]
  )
  def getUnpurchaseGames(@ApiParam(value = "The id of the User to show the unpurchased games") userId: Int) = Action {
    req =>
      val ints = testRepo.getAllUnpurchaseGames(userId)
      val eventualMaybeGames = ints.map {
        gameId => gameRepo.getGame(gameId)
      }
      Ok(Json.toJson(eventualMaybeGames))
  }


}
