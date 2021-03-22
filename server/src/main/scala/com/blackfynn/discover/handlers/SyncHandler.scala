// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.handlers

import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

import com.blackfynn.discover.db.profile.api.DBIO
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.blackfynn.discover.{ utils, Config, Ports }
import scalikejdbc._
import scalikejdbc.athena._
import com.blackfynn.discover.logging.logRequestAndResponse
import com.blackfynn.discover.server.sync.{
  SyncHandler => GuardrailHandler,
  SyncResource => GuardrailResource
}

import scala.concurrent.{ ExecutionContext, Future }
import com.blackfynn.discover.db.DatasetDownloadsMapper
import com.blackfynn.discover.models.{ DatasetDownload, DownloadOrigin }

class SyncHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext,
  materializer: ActorMaterializer
) extends GuardrailHandler {

  override def syncAthenaDownloads(
    respond: GuardrailResource.syncAthenaDownloadsResponse.type
  )(
    startDate: LocalDate,
    endDate: LocalDate
  ): Future[GuardrailResource.syncAthenaDownloadsResponse] = {
    val realEndDate = endDate.plusDays(1)
    val athenaDownloads =
      ports.athenaClient.getDatasetDownloadsForRange(startDate, realEndDate)

    val query = for {
      databaseDownloads <- DatasetDownloadsMapper
        .getDatasetDownloadsByDateRange(
          OffsetDateTime.of(startDate, LocalTime.MIDNIGHT, ZoneOffset.UTC),
          OffsetDateTime.of(realEndDate, LocalTime.MIDNIGHT, ZoneOffset.UTC)
        )

      cleanDownloads = utils.cleanAthenaDownloads(
        athenaDownloads,
        databaseDownloads
      )

      newDownloads <- DBIO.sequence(cleanDownloads.map { d =>
        DatasetDownloadsMapper
          .create(d.datasetId, d.version, d.origin, d.requestId, d.downloadedAt)
      })
    } yield (newDownloads)

    ports.db
      .run(query)
      .map {
        case _: List[DatasetDownload] => {
          GuardrailResource.syncAthenaDownloadsResponse.OK
        }
      }
  }
}

object SyncHandler {

  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      GuardrailResource.routes(new SyncHandler(ports))
    }
}
