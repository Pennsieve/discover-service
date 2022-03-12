// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.pennsieve.discover.{ Config, Ports }
import com.pennsieve.discover.search._
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }

object Main extends App with StrictLogging {
  val config: Config = Config.load

  implicit val system: ActorSystem = ActorSystem("discover-service")
  implicit val executionContext: ExecutionContext = system.dispatcher

  implicit val ports: Ports = Ports(config)

  var exitCode: Int = 0
  try {
    Await.result(Search.buildSearchIndex(ports), 30.minutes)
    logger.info("Done")
  } catch {
    case e: Exception => {
      e.printStackTrace()
      exitCode = 1
    }
  } finally {
    logger.info("Shutting down...")

    Await.result(for {
      _ <- Future(ports.db.close())
      _ <- Http().shutdownAllConnectionPools()
      _ <- system.terminate()
    } yield (), 5.seconds)

    sys.exit(exitCode)
  }
}
