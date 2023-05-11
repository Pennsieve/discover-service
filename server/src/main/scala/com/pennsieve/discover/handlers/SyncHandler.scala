// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

import com.pennsieve.discover.db.profile.api.DBIO
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import com.pennsieve.discover.{ utils, Config, Ports }
import scalikejdbc._
import scalikejdbc.athena._
import com.pennsieve.discover.logging.logRequestAndResponse
import com.pennsieve.discover.server.sync.{
  SyncHandler => GuardrailHandler,
  SyncResource => GuardrailResource
}

import scala.concurrent.{ ExecutionContext, Future }
import com.pennsieve.discover.db.DatasetDownloadsMapper
import com.pennsieve.discover.models.{ DatasetDownload, DownloadOrigin }

class SyncHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext,
  system: ActorSystem
) extends GuardrailHandler {

  def sinceMillis(startNanos: Long): Double =
    (System.nanoTime() - startNanos) / 1e6

  override def syncAthenaDownloads(
    respond: GuardrailResource.SyncAthenaDownloadsResponse.type
  )(
    startDate: LocalDate,
    endDate: LocalDate
  ): Future[GuardrailResource.SyncAthenaDownloadsResponse] = {
    val startNanos = System.nanoTime()
    val logger = ports.logger.noContext
    val realEndDate = endDate.plusDays(1)
    val athenaDownloads =
      ports.athenaClient.getDatasetDownloadsForRange(startDate, realEndDate)
    logger.info(
      s"got ${athenaDownloads.size} downloads from Athena; since start: ${sinceMillis(startNanos)} ms"
    )

    val query = for {
      databaseDownloads <- DatasetDownloadsMapper
        .getDatasetDownloadsByDateRange(
          OffsetDateTime.of(startDate, LocalTime.MIDNIGHT, ZoneOffset.UTC),
          OffsetDateTime.of(realEndDate, LocalTime.MIDNIGHT, ZoneOffset.UTC)
        )

      _ = logger.info(s"got ${databaseDownloads.size} downloads from Postgres")

      cleanDownloads = utils.cleanAthenaDownloads(
        athenaDownloads,
        databaseDownloads
      )

      _ = logger.info(
        s"got ${cleanDownloads.size} downloads from deduplication"
      )

      newDownloads <- DBIO.sequence(cleanDownloads.map { d =>
        DatasetDownloadsMapper
          .create(d.datasetId, d.version, d.origin, d.requestId, d.downloadedAt)
      })
      _ = logger.info(
        s"adding ${newDownloads.size} new download records to Postgres"
      )
    } yield (newDownloads)

    val response = ports.db
      .run(query)
      .map {
        case _: List[DatasetDownload] => {
          GuardrailResource.SyncAthenaDownloadsResponse.OK
        }
      }
    logger.info(s"completed Athena sync in ${sinceMillis(startNanos)} ms")
    response
  }
}

object SyncHandler {

  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      GuardrailResource.routes(new SyncHandler(ports))
    }
}
