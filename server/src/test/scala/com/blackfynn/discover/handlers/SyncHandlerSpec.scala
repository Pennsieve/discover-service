// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.handlers

import java.time._
import java.time.temporal.ChronoUnit

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.blackfynn.discover.{ utils, ServiceSpecHarness, TestUtilities }
import com.blackfynn.discover.client.sync.SyncClient
import org.scalatest.{ Matchers, WordSpec }
import com.blackfynn.discover.models.{ DatasetDownload, DownloadOrigin }

class SyncHandlerSpec
    extends WordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness {

  def createRoutes(): Route =
    Route.seal(SyncHandler.routes(ports))

  def createSyncClient(routes: Route): SyncClient =
    SyncClient.httpClient(Route.asyncHandler(routes))

  val syncClient: SyncClient = createSyncClient(createRoutes())

  "GET /metrics/dataset/athena/download/sync" should {

    "perform a fuzzy deduplication of Athena Downloads" in {
      val AthenaDL = List(
        DatasetDownload(
          12,
          1,
          Some(DownloadOrigin.AWSRequesterPayer),
          Some("REQID1"),
          OffsetDateTime.of(2020, 11, 10, 10, 3, 1, 0, ZoneOffset.UTC)
        ),
        DatasetDownload(
          12,
          1,
          Some(DownloadOrigin.AWSRequesterPayer),
          Some("REQID2"),
          OffsetDateTime.of(2020, 11, 10, 10, 3, 6, 0, ZoneOffset.UTC)
        ),
        DatasetDownload(
          12,
          1,
          Some(DownloadOrigin.AWSRequesterPayer),
          Some("REQID3"),
          OffsetDateTime.of(2020, 11, 10, 10, 3, 12, 0, ZoneOffset.UTC)
        )
      )

      val DBDL = List(
        DatasetDownload(
          12,
          1,
          Some(DownloadOrigin.SPARC),
          None,
          OffsetDateTime.of(2020, 11, 10, 10, 3, 2, 0, ZoneOffset.UTC)
        ),
        DatasetDownload(
          12,
          1,
          Some(DownloadOrigin.SPARC),
          Some("REQID3"),
          OffsetDateTime.of(2020, 11, 10, 10, 3, 13, 0, ZoneOffset.UTC)
        )
      )

      utils.cleanAthenaDownloads(AthenaDL, DBDL) shouldBe List(
        DatasetDownload(
          12,
          1,
          Some(DownloadOrigin.AWSRequesterPayer),
          Some("REQID2"),
          OffsetDateTime.of(2020, 11, 10, 10, 3, 6, 0, ZoneOffset.UTC)
        )
      )
    }

    "sync the datasets downloads metrics for the range passed" in {

      val ds1 = TestUtilities.createDatasetV1(ports.db)()
      val ds2 = TestUtilities.createDatasetV1(ports.db)()

      val row1 = TestUtilities.createDatasetDownloadRow(ports.db)(
        ds1.datasetId,
        ds1.version,
        DownloadOrigin.Discover,
        None,
        OffsetDateTime.of(2020, 11, 10, 18, 3, 1, 0, ZoneOffset.UTC)
      )

      val row2 = TestUtilities.createDatasetDownloadRow(ports.db)(
        ds1.datasetId,
        ds1.version,
        DownloadOrigin.AWSRequesterPayer,
        Some("REQID1"),
        OffsetDateTime.of(2020, 11, 10, 10, 3, 1, 0, ZoneOffset.UTC)
      )

      syncClient
        .syncAthenaDownloads(
          LocalDate.of(2020, 11, 10),
          LocalDate.of(2020, 11, 11)
        )
        .await

      val responseMetrics = TestUtilities.getDatasetDownloads(ports.db)(
        LocalDate.of(2020, 11, 10),
        LocalDate.of(2020, 11, 11)
      )

      responseMetrics shouldBe
        List(
          row1,
          row2,
          DatasetDownload(
            1,
            1,
            Some(DownloadOrigin.AWSRequesterPayer),
            Some("REQID2"),
            OffsetDateTime.of(2020, 11, 10, 19, 3, 1, 0, ZoneOffset.UTC)
          )
        )
    }
  }
}
