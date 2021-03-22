// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.handlers

import scala.concurrent.{ ExecutionContext, Future }
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.blackfynn.discover._
import com.blackfynn.discover.server.file.{
  FileHandler => GuardrailHandler,
  FileResource => GuardrailResource
}

import com.blackfynn.discover.db.PublicFilesMapper
import com.blackfynn.discover.logging.{
  logRequestAndResponse,
  DiscoverLogContext
}
import com.blackfynn.discover.models.{ FileTreeNode, FileTreeNodeDTO }
import com.blackfynn.discover.server.definitions.FileTreePage

/**
  * Handler for public endpoints for getting files from their sourcePackageId
  */
class FileHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext,
  materializer: ActorMaterializer
) extends GuardrailHandler {

  implicit val config: Config = ports.config

  val defaultFileLimit = 100
  val defaultFileOffset = 0

  override def getFileFromSourcePackageId(
    respond: GuardrailResource.getFileFromSourcePackageIdResponse.type
  )(
    sourcePackageId: String,
    limit: Option[Int],
    offset: Option[Int]
  ): Future[GuardrailResource.getFileFromSourcePackageIdResponse] = {

    val query = for {

      response <- PublicFilesMapper.getFileFromSourcePackageId(
        sourcePackageId,
        limit = limit.getOrElse(defaultFileLimit),
        offset = offset.getOrElse(defaultFileOffset)
      )

    } yield response

    ports.db
      .run(query)
      .map {
        case (total, Nil) =>
          GuardrailResource.getFileFromSourcePackageIdResponse
            .NotFound(sourcePackageId)
        case (total, files) =>
          GuardrailResource.getFileFromSourcePackageIdResponse
            .OK(
              FileTreePage(
                totalCount = total,
                limit = limit.getOrElse(defaultFileLimit),
                offset = offset.getOrElse(defaultFileOffset),
                files = files.map { f =>
                  FileTreeNodeDTO(FileTreeNode(f))
                }.toIndexedSeq
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
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      GuardrailResource.routes(new FileHandler(ports))
    }
}
