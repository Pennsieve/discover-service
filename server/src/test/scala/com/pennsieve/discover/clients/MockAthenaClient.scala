// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import java.time._

import cats.data.EitherT
import cats.implicits._
import com.pennsieve.discover.models.DatasetDownload
import com.pennsieve.discover.models.DownloadOrigin
import com.pennsieve.discover.server.definitions.DatasetPublishStatus

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

class MockAthenaClient extends AthenaClient {

  val defaultDatasetDownloads: List[DatasetDownload] = List(
    DatasetDownload(
      1,
      1,
      Some(DownloadOrigin.AWSRequesterPayer),
      Some("REQID2"),
      OffsetDateTime.of(
        LocalDateTime
          .of(LocalDate.of(2020, 11, 10), LocalTime.of(19, 3, 1)),
        ZoneOffset.UTC
      )
    ),
    DatasetDownload(
      1,
      1,
      Some(DownloadOrigin.AWSRequesterPayer),
      Some("REQID3"),
      OffsetDateTime.of(
        LocalDateTime
          .of(LocalDate.of(2020, 11, 10), LocalTime.of(18, 3, 1)),
        ZoneOffset.UTC
      )
    ),
    DatasetDownload(
      1,
      1,
      Some(DownloadOrigin.AWSRequesterPayer),
      Some("REQID1"),
      OffsetDateTime.of(
        LocalDateTime
          .of(LocalDate.of(2020, 11, 10), LocalTime.of(10, 3, 1)),
        ZoneOffset.UTC
      )
    )
  )

  var datasetDownloadsForRange: Option[List[DatasetDownload]] = None

  def reset() = {
    datasetDownloadsForRange = None
  }

  def getDatasetDownloadsForRange(
    startDate: LocalDate,
    endDate: LocalDate
  )(implicit
    ec: ExecutionContext
  ): List[DatasetDownload] =
    datasetDownloadsForRange.getOrElse(defaultDatasetDownloads)

}
