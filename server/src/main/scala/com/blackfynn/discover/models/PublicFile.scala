// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import com.github.tminglei.slickpg.LTree

import java.time.{ OffsetDateTime, ZoneOffset }

case class PublicFile(
  name: String,
  datasetId: Int,
  version: Int,
  fileType: String,
  size: Long,
  s3Key: S3Key.File,
  path: LTree,
  sourcePackageId: Option[String],
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  id: Int = 0,
  s3Version: Option[ObjectVersion] = None,
  sourceFileId: Option[String] = None
)

object PublicFile {
  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled
}
