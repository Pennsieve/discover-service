// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import com.pennsieve.discover.Ports
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.server.healthcheck.{
  HealthcheckResource,
  HealthcheckHandler => GuardrailHandler
}
import com.pennsieve.discover.server.healthcheck.{ HealthcheckHandler => Huh }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

class HealthcheckHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext
) extends GuardrailHandler {

  override def healthcheck(
    respond: HealthcheckResource.HealthcheckResponse.type
  )(
  ): Future[HealthcheckResource.HealthcheckResponse] = {

    ports.db
      .run(sql"select 1".as[Int])
      .map(
        result =>
          if (result.contains(1))
            respond.OK
          else {
            ports.logger.noContext.error(result.toString)
            respond.InternalServerError("Postgres: ${result.toString}")
          }
      )
      .recover {
        case NonFatal(e) => respond.InternalServerError(e.toString)
      }
  }
}

object HealthcheckHandler {
  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ) = HealthcheckResource.routes(new HealthcheckHandler(ports))
}
