// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import java.time._
import java.time.temporal.ChronoUnit
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.discover.{ utils, ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.client.sync.SyncClient
import com.pennsieve.discover.clients.MockAthenaClient
import com.pennsieve.discover.models.{ DatasetDownload, DownloadOrigin }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SyncHandlerSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness {

  def createRoutes(): Route =
    Route.seal(SyncHandler.routes(ports))

  def createSyncClient(routes: Route): SyncClient =
    SyncClient.httpClient(Route.toFunction(routes))

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

      val reqId2DownloadTimeInLocal = convertToLocalOffset(
        OffsetDateTime.of(2020, 11, 10, 19, 3, 1, 0, ZoneOffset.UTC) // this is the downloadedAt time of req2 which the MockAthenaClient will return
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
            reqId2DownloadTimeInLocal
          )
        )
    }

    "skip Athena results with datasetID == 0 and set real version for a result with version == 0" in {
      val ds1 = TestUtilities.createDatasetV1(ports.db)()
      val ds1V2 = TestUtilities.createNewDatasetVersion(ports.db)(ds1.datasetId)

      val req1 = DatasetDownload(
        ds1.datasetId,
        ds1.version,
        Some(DownloadOrigin.AWSRequesterPayer),
        Some("REQID1"),
        OffsetDateTime.of(2020, 11, 10, 10, 3, 1, 0, ZoneOffset.UTC)
      )
      val req2 = DatasetDownload(
        ds1.datasetId,
        0, // this should be replaced by ds1V2.version when it is inserted into Postgres
        Some(DownloadOrigin.AWSRequesterPayer),
        Some("REQID2"),
        OffsetDateTime.of(2020, 11, 10, 10, 3, 6, 0, ZoneOffset.UTC)
      )
      val req3 = DatasetDownload(
        0, // this 0 means that it should be ignored since it cannot be inserted into Postgres (violates foreign key constraint)
        1,
        Some(DownloadOrigin.AWSRequesterPayer),
        Some("REQID3"),
        OffsetDateTime.of(2020, 11, 10, 10, 3, 12, 0, ZoneOffset.UTC)
      )
      ports.athenaClient
        .asInstanceOf[MockAthenaClient]
        .datasetDownloadsForRange = Some(List(req1, req2, req3))

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
          req1.copy(downloadedAt = convertToLocalOffset(req1.downloadedAt)),
          req2.copy(
            version = ds1V2.version,
            downloadedAt = convertToLocalOffset(req2.downloadedAt)
          )
        )
    }
  }

  // I believe the DB connection returns the downloadedAt values in DatasetDownloads in the local time offset.
  // Without the conversion to the local offset tests were failing locally on developer machines,
  // but passing in CI where the system timezone is UTC.
  def convertToLocalOffset(utcTime: OffsetDateTime): OffsetDateTime = {
    val localOffset =
      ZoneId
        .systemDefault()
        .getRules()
        .getOffset(utcTime.toInstant)
    utcTime.withOffsetSameInstant(localOffset)
  }
}
