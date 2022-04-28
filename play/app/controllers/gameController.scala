package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam}
import models.{TestRepository, User, gameRepository}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import javax.inject.Inject

@Api(value = "/Statics Recommendation")
class gameController @Inject()(
                                cc: ControllerComponents,
                                gameRepo: gameRepository, userRepo: TestRepository) extends AbstractController(cc) {

  @ApiOperation(
    value = "Find all Games recommended for a user",
    response = classOf[User]
  )
  def getGames(@ApiParam(value = "The id of the User to recommend") userId: Int) = Action {
    req =>
      val ints = userRepo.getRecommendGameId(userId)
      val eventualMaybeGames = ints.map {
        gameId => gameRepo.getGame(gameId)
      }
      Ok(Json.toJson(eventualMaybeGames))
  }


}
