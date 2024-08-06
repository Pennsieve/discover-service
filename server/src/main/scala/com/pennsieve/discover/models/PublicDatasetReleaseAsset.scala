// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.github.tminglei.slickpg.LTree

import java.time.{ OffsetDateTime, ZoneOffset }

case class PublicDatasetReleaseAsset(
  id: Int = 0,
  datasetId: Int,
  datasetVersion: Int,
  releaseId: Int,
  file: String,
  name: String,
  `type`: String,
  size: Long,
  path: LTree,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object PublicDatasetReleaseAsset {
  val tupled = (this.apply _).tupled
}
