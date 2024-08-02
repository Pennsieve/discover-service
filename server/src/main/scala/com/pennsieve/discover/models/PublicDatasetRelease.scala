// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

case class PublicDatasetRelease(
  id: Int = 0,
  datasetId: Int,
  datasetVersion: Int,
  origin: String,
  label: String,
  marker: String,
  repoUrl: String,
  labelUrl: Option[String],
  markerUrl: Option[String],
  releaseStatus: Option[String],
  assetFilePrefix: Option[String],
  assetFileId: Option[Int],
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object PublicDatasetRelease {
  val tupled = (this.apply _).tupled
}
