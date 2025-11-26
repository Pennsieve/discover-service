// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import java.time._
import com.pennsieve.test.EitherValue._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.client.metrics.{
  GetDatasetDownloadsSummaryResponse,
  MetricsClient
}
import com.pennsieve.discover.models.DownloadOrigin
import com.pennsieve.discover.client.definitions.DatasetDownloadSummaryRow
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricsHandlerSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness {

  def createRoutes(): Route =
    Route.seal(MetricsHandler.routes(ports))

  def createClient(routes: Route): MetricsClient =
    MetricsClient.httpClient(Route.toFunction(routes))

  var metricsClient: MetricsClient = _

  override def afterStart(): Unit = {
    super.afterStart()
    metricsClient = createClient(createRoutes())
  }

  "GET /metrics/dataset/downloads/summary" should {

    "present the dataset download summary for the requested date range" in {
      val ds1 = TestUtilities.createDatasetV1(ports.db)(sourceDatasetId = 1)
      val ds2 = TestUtilities.createDatasetV1(ports.db)(sourceDatasetId = 2)
      TestUtilities.createDatasetDownloadRow(ports.db)(
        ds1.datasetId,
        ds1.version,
        DownloadOrigin.Discover,
        None,
        OffsetDateTime.of(
          LocalDateTime
            .of(LocalDate.of(2020, 11, 10), LocalTime.of(10, 3, 1)),
          ZoneOffset.UTC
        )
      )

      TestUtilities.createDatasetDownloadRow(ports.db)(
        ds2.datasetId,
        ds2.version,
        DownloadOrigin.SPARC,
        None,
        OffsetDateTime.of(
          LocalDateTime
            .of(LocalDate.of(2020, 11, 11), LocalTime.of(14, 37, 49)),
          ZoneOffset.UTC
        )
      )

      val response =
        metricsClient
          .getDatasetDownloadsSummary(
            LocalDate.of(2020, 11, 10),
            LocalDate.of(2020, 11, 11)
          )
          .awaitFinite()
          .value

      response shouldBe
        GetDatasetDownloadsSummaryResponse.OK(
          Vector(
            DatasetDownloadSummaryRow(
              ds1.datasetId,
              ds1.version,
              DownloadOrigin.Discover.entryName,
              1
            ),
            DatasetDownloadSummaryRow(
              ds2.datasetId,
              ds2.version,
              DownloadOrigin.SPARC.entryName,
              1
            )
          )
        )
    }

  }
}
