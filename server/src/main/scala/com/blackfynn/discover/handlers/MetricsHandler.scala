// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import java.time.LocalDate

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.pennsieve.discover.Ports
import com.pennsieve.discover.logging.logRequestAndResponse
import com.pennsieve.discover.server.metrics.{
  MetricsHandler => GuardrailHandler,
  MetricsResource => GuardrailResource
}

import scala.concurrent.{ ExecutionContext, Future }
import com.pennsieve.discover.db.DatasetDownloadsMapper

class MetricsHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext,
  materializer: ActorMaterializer
) extends GuardrailHandler {

  override def getDatasetDownloadsSummary(
    respond: GuardrailResource.getDatasetDownloadsSummaryResponse.type
  )(
    startDate: LocalDate,
    endDate: LocalDate
  ): Future[GuardrailResource.getDatasetDownloadsSummaryResponse] = {
    val realEndDate = endDate.plusDays(1)
    ports.db
      .run(
        DatasetDownloadsMapper
          .getDatasetDownloadSummaryByDateRange(startDate, realEndDate)
          .map { datasetDownloadsMetrics =>
            {
              GuardrailResource.getDatasetDownloadsSummaryResponse
                .OK(
                  datasetDownloadsMetrics.sortBy(
                    downloadList =>
                      (
                        downloadList.datasetId,
                        downloadList.version,
                        downloadList.origin
                      )
                  )
                )
            }
          }
      )
  }
}

object MetricsHandler {

  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      GuardrailResource.routes(new MetricsHandler(ports))
    }
}
