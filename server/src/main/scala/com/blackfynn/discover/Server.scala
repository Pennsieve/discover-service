// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.pennsieve.discover.handlers._
import com.pennsieve.discover.notifications.SQSNotificationHandler
import com.pennsieve.service.utilities.MigrationRunner
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext }

object DatabaseMigrator extends StrictLogging {

  def run(configuration: PostgresConfiguration): Unit = {
    val migrator =
      new MigrationRunner(
        configuration.jdbcURL,
        configuration.user,
        configuration.password,
        schema = Some(configuration.schema),
        scriptLocation = Some("classpath:db/migration")
      )

    val (count, _) = migrator.run()
    logger.info(
      s"Ran $count migrations on ${configuration.schema} in ${configuration.jdbcURL}"
    )
  }
}

object Server extends App with StrictLogging {
  val config: Config = Config.load

  implicit val system: ActorSystem = ActorSystem("discover-service")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  implicit val ports: Ports = Ports(config)

  DatabaseMigrator.run(config.postgres)

  val notificationHandler = new SQSNotificationHandler(
    ports,
    config.sqs.region,
    config.sqs.queueUrl,
    config.sqs.parallelism
  )

  val killswitch = notificationHandler.graph().run()
  logger.info("Started notification stream")

  sys.addShutdownHook {
    logger.info("Shutting down stream")
    killswitch.shutdown()
  }

  // format: off
  val routes: Route =
    Route.seal(

      // In order to differentiate public and private routes, NGINX adds
      // a '/public' prefix to external requests to Discover.
      // Strip the prefix and pass the request to the public handlers.
      pathPrefix("public") {
        DatasetHandler.routes(ports) ~
        TagHandler.routes(ports) ~
        SearchHandler.routes(ports) ~
        FileHandler.routes(ports) ~
          MetricsHandler.routes(ports) ~
          OrganizationHandler.routes(ports)
      } ~

      // Any URLs without the /public prefix must be internal. Send them to the
      // JWT-protected service-level routes.
      HealthcheckHandler.routes(ports) ~
        SyncHandler.routes(ports) ~
      PublishHandler.routes(ports) ~
      SearchHandler.routes(ports) ~
      DatasetHandler.routes(ports) ~
        OrganizationHandler.routes(ports) ~
        FileHandler.routes(ports)
    )
  // format: on

  Http().bindAndHandle(routes, config.host, config.port)
  logger.info(s"Server online at http://${config.host}:${config.port}")

  Await.result(system.whenTerminated, Duration.Inf)
}
