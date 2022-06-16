// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

final case class DatasetDownload(
  datasetId: Int,
  version: Int,
  origin: Option[DownloadOrigin],
  requestId: Option[String] = None,
  downloadedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object DatasetDownload {
  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled
}
