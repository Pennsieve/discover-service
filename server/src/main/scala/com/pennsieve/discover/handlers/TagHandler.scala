// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import com.pennsieve.discover.server.tag.{
  TagHandler => GuardrailHandler,
  TagResource => GuardrailResource
}
import com.pennsieve.discover._
import com.pennsieve.discover.db.PublicDatasetsMapper
import com.pennsieve.discover.logging.logRequestAndResponse

import scala.concurrent.{ ExecutionContext, Future }

/**
  * Handler for public endpoints for getting tags associated with published datasets.
  */
class TagHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext
) extends GuardrailHandler {

  override def getTags(
    respond: GuardrailResource.getTagsResponse.type
  )(
  ): Future[GuardrailResource.getTagsResponse] = {
    ports.db
      .run(PublicDatasetsMapper.getTagCounts)
      .map { tags =>
        GuardrailResource.getTagsResponse
          .OK(tags.toIndexedSeq)
      }
  }
}

object TagHandler {

  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      GuardrailResource.routes(new TagHandler(ports))
    }
}
