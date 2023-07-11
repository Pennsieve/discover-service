// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.github.tminglei.slickpg.LTree
import java.time.{ OffsetDateTime, ZoneOffset }
import java.util.UUID

case class PublicFileVersion(
  id: Int = 0,
  name: String,
  fileType: String,
  size: Long,
  sourcePackageId: Option[String],
  sourceFileUUID: Option[UUID],
  checksum: FileChecksum,
  s3Key: S3Key.File,
  s3Version: String,
  path: LTree,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object PublicFileVersion {
  val tupled = (this.apply _).tupled
}
