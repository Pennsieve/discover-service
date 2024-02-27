// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import scala.concurrent.{ ExecutionContext, Future }
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import com.pennsieve.discover._
import com.pennsieve.discover.server.file.{
  FileHandler => GuardrailHandler,
  FileResource => GuardrailResource
}
import com.pennsieve.discover.db.{ PublicFileVersionsMapper, PublicFilesMapper }
import com.pennsieve.discover.logging.{
  logRequestAndResponse,
  DiscoverLogContext
}
import com.pennsieve.discover.models.{ FileTreeNode, FileTreeNodeDTO }
import com.pennsieve.discover.server.definitions.{
  FileTreePage,
  FileTreeWithOrgPage
}

/**
  * Handler for public endpoints for getting files from their sourcePackageId
  */
class FileHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext
) extends GuardrailHandler {

  implicit val config: Config = ports.config

  val defaultFileLimit = 100
  val defaultFileOffset = 0

  override def getFileFromSourcePackageId(
    respond: GuardrailResource.GetFileFromSourcePackageIdResponse.type
  )(
    sourcePackageId: String,
    limit: Option[Int],
    offset: Option[Int]
  ): Future[GuardrailResource.GetFileFromSourcePackageIdResponse] = {

    val query = for {
      response <- PublicFileVersionsMapper.getFileFromSourcePackageId(
        sourcePackageId,
        limit = limit.getOrElse(defaultFileLimit),
        offset = offset.getOrElse(defaultFileOffset)
      )
    } yield response

    ports.db
      .run(query)
      .map {
        case (total, _, Nil) =>
          GuardrailResource.GetFileFromSourcePackageIdResponse
            .NotFound(sourcePackageId)
        case (total, Some(organizationId), files) =>
          GuardrailResource.GetFileFromSourcePackageIdResponse
            .OK(
              FileTreeWithOrgPage(
                totalCount = total,
                limit = limit.getOrElse(defaultFileLimit),
                offset = offset.getOrElse(defaultFileOffset),
                organizationId = organizationId,
                files = files.map { f =>
                  FileTreeNodeDTO(FileTreeNode(f._1, f._2))
                }.toVector
              )
            )
      }

  }
}

object FileHandler {
  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      GuardrailResource.routes(new FileHandler(ports))
    }
}
