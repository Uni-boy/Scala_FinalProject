package controllers

import io.swagger.annotations.{Api, ApiOperation, ApiParam}
import models.{TestRepository, User, gameRepository}
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

}
