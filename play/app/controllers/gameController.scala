package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam}
import models.{TestRepository, User, dynamicRec, gameRepository}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import javax.inject.Inject

/**
 * Created by Riccardo Sirigu on 10/08/2017.
 */
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
      println(ints.size)
      ints.foreach(println)
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
      dynamicRec.find(userId)
      val ints = userRepo.getAllUnpurchaseGames(userId)
      val eventualMaybeGames = ints.map {
        gameId => gameRepo.getGame(gameId)
      }
      Ok(Json.toJson(eventualMaybeGames))
  }

}
