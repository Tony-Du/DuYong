package cn.mallcoo.medusa.data.center.web

import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import ch.qos.logback.classic.LoggerContext
import cn.mallcoo.medusa.data.center.web.ParserActor.{GetSQL, Init}
import com.typesafe.config.ConfigFactory
import net.minidev.json.JSONValue
import org.slf4j.LoggerFactory
import spray.json._

import scala.io.StdIn

/**
  * Created by wm on 2017/4/18.OO
  */
// set JAVA_HOME=D:\Java\jdk1.8.0_121 & sbt assembly
// set JAVA_HOME=D:\Java\jdk1.8.0_121 & sbt assemblyPackageDependency
// set JAVA_HOME=D:\Java\jdk1.8.0_121 & sbt package
object Main {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("application")
    val host = config.getString("http.host")
    val port = config.getInt("http.port")

    implicit val system = ActorSystem("data-center", config)
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    //    val  logger = lc.getLogger("CONSOLE")

    //    LogbackReloadJmx.init(lc)


    val logger = Logging(system, "main")

    val listener = system.actorOf(Props[Listener], "listener")

    //    system.eventStream.subscribe(listener, classOf[DeadLetter])

    implicit def myRejectionHandler =
      RejectionHandler.default
        .mapRejectionResponse {
          case res@HttpResponse(status, _, ent: HttpEntity.Strict, _) =>
            res.copy(entity = HttpEntity(
              ContentTypes.`application/json`,
              Respense(status.intValue(), ent.data.utf8String, Nil).toJson.toString
            ))

          case x => x
        }

    implicit def myExceptionHandler: ExceptionHandler =
      ExceptionHandler {
        case ex: Exception =>
          extractUri { uri =>
            logger.error(ex, s"Request to $uri could not be handled normally")
            complete(HttpResponse(
              StatusCodes.InternalServerError,
              entity = HttpEntity(
                ContentTypes.`application/json`,
                Respense(StatusCodes.InternalServerError.intValue, ex.toString, Nil).toJson.toString
              )
            ))
          }
      }

    import scala.concurrent.duration._
    implicit val timeout = Timeout(5.seconds)

    val route = {
      (post & path("query") & entity(as[String])) { data =>
        validate(JSONValue.isValidJson(data), "invalidate json format") {
          val result = listener ? GetSQL(data)
          complete(result.mapTo[Respense])
        }
      } ~ (get & path("init" / "reload")) {
        val result = listener ? Init
        complete(result.mapTo[Respense])
      }
    }

    val bindingFuture = Http().bindAndHandle(route, host, port)
    println(s"Server online at http://$host:$port/")

    while (true) {
      println("Press EXIT to stop...")
      if (StdIn.readLine().equalsIgnoreCase("exit")) {
        println("Press EXIT again to stop...")
        if (StdIn.readLine().equalsIgnoreCase("exit")) {
          bindingFuture
            .flatMap(_.unbind()) // trigger unbinding from the port
            .onComplete { _ =>
            system.terminate()
          } // and shutdown when done
          Thread.sleep(1000)
          System.exit(0)

        } else println(s"cancel to stop...")
      }
    }


  }

}
