package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam}
import models.{TestRepository, User, gameRepository, dynamicRec}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import javax.inject.Inject

@Api(value = "/Dynamic Recommendation")
class userController @Inject()(
                                cc: ControllerComponents,
                                userRepo: TestRepository, gameRepo: gameRepository, dynamicRepo: dynamicRec) extends AbstractController(cc) {

  @ApiOperation(
    value = "Find all unpurchased games of the specific user",
    response = classOf[User]
  )
  def getUnpurchaseGames(@ApiParam(value = "The id of the User to show the unpurchased games") userId: Int) = Action {
    req =>
      val ints = userRepo.getAllUnpurchaseGames(userId)
      val eventualMaybeGames = ints.map {
        gameId => gameRepo.getGame(gameId)
      }
      Ok(Json.toJson(eventualMaybeGames))
  }

  @ApiOperation(
    value = "Dynamic recommendation",
    response = classOf[User]
  )
  def getDynamicRec(@ApiParam(value = "The id of the User wanna get recommendation") userId: Int,
                    @ApiParam(value = "The id of the game the user not yet purchased") gameId: Int) = Action {
    req =>
      userRepo.update(userId, gameId)
      dynamicRepo.find(userId)
      val ints = userRepo.getAllUnpurchaseGames(userId)
      val eventualMaybeGames = ints.map {
        gameId => gameRepo.getGame(gameId)
      }
      Ok(Json.toJson(eventualMaybeGames))
  }


}
