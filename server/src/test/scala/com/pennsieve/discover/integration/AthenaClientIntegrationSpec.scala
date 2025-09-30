// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.integration

import java.time._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.client.sync.{
  SyncAthenaDownloadsResponse,
  SyncClient
}
import com.pennsieve.discover.client.metrics.MetricsClient
import com.pennsieve.discover.clients.{ AthenaClient, AthenaClientImpl }
import com.pennsieve.discover.handlers.SyncHandler
import com.pennsieve.discover.models.DatasetDownload
import com.pennsieve.discover.models.DownloadOrigin.AWSRequesterPayer
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AthenaClientIntegrationSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness {

  "calling the athena client" should {

    "find the proper downloads" in {

      val athenaClient: AthenaClient = new AthenaClientImpl(
        pennsieveTable = config.athena.pennsieveBucketAccessTable,
        sparcTable = config.athena.sparcBucketAccessTable,
        rejoinTable = config.athena.rejoinBucketAccessTable,
        sparcAodTable = config.athena.sparcAodBucketAccessTable
      )

      val athenaDownloads = athenaClient.getDatasetDownloadsForRange(
        LocalDate.of(2020, 11, 11),
        LocalDate.of(2020, 11, 12)
      )

      athenaDownloads shouldBe List(
        DatasetDownload(
          1334,
          1,
          Some(AWSRequesterPayer),
          Some("FK2P0T2H4YEZFHDP"),
          OffsetDateTime.of(2020, 11, 11, 18, 48, 59, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1336,
          1,
          Some(AWSRequesterPayer),
          Some("A068197DADD85E6C"),
          OffsetDateTime.of(2020, 11, 11, 17, 4, 16, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1336,
          1,
          Some(AWSRequesterPayer),
          Some("49CE3A832C359112"),
          OffsetDateTime.of(2020, 11, 11, 17, 7, 53, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1337,
          1,
          Some(AWSRequesterPayer),
          Some("34F165F050A2EE50"),
          OffsetDateTime.of(2020, 11, 11, 18, 3, 16, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1337,
          1,
          Some(AWSRequesterPayer),
          Some("064EB1BAC9883FAF"),
          OffsetDateTime.of(2020, 11, 11, 18, 8, 4, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1338,
          1,
          Some(AWSRequesterPayer),
          Some("B4536A16E4C3FE9C"),
          OffsetDateTime.of(2020, 11, 11, 19, 2, 44, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1338,
          1,
          Some(AWSRequesterPayer),
          Some("E772028CE129B05B"),
          OffsetDateTime.of(2020, 11, 11, 19, 6, 41, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1339,
          1,
          Some(AWSRequesterPayer),
          Some("B8E4AFDE1EB9CC01"),
          OffsetDateTime.of(2020, 11, 11, 20, 2, 27, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1339,
          1,
          Some(AWSRequesterPayer),
          Some("F4D780E149A165FD"),
          OffsetDateTime.of(2020, 11, 11, 20, 7, 20, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1340,
          1,
          Some(AWSRequesterPayer),
          Some("A10A751AB0ADC309"),
          OffsetDateTime.of(2020, 11, 11, 21, 4, 44, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1340,
          1,
          Some(AWSRequesterPayer),
          Some("D1679E1C8EB171AF"),
          OffsetDateTime.of(2020, 11, 11, 21, 8, 14, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1341,
          1,
          Some(AWSRequesterPayer),
          Some("8F8FAE0AE834E731"),
          OffsetDateTime.of(2020, 11, 11, 22, 2, 40, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1341,
          1,
          Some(AWSRequesterPayer),
          Some("798EF5F70F75C935"),
          OffsetDateTime.of(2020, 11, 11, 22, 6, 18, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1342,
          1,
          Some(AWSRequesterPayer),
          Some("88E4D6B780BBDAE6"),
          OffsetDateTime.of(2020, 11, 11, 23, 3, 52, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          1342,
          1,
          Some(AWSRequesterPayer),
          Some("DF2386C74B00036D"),
          OffsetDateTime.of(2020, 11, 11, 23, 8, 51, 0, ZoneOffset.ofHours(-5))
        ),
        DatasetDownload(
          570,
          2,
          Some(AWSRequesterPayer),
          Some("2DE93FE66D511EB4"),
          OffsetDateTime.of(2020, 11, 11, 20, 28, 4, 0, ZoneOffset.ofHours(-5))
        )
      )
    }
  }
}
