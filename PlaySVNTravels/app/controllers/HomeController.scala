package controllers

import javax.inject._

import play.api.mvc._
import spark.StreamingCheapFlights

import scala.concurrent.{ExecutionContext, Future}

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents) (implicit ec: ExecutionContext) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {
    val abc = Future{ new StreamingCheapFlights}
    Ok(views.html.index(s"Your new application is ready. Your message:"))
  }

}



