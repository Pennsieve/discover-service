// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.github.tminglei.slickpg.LTree

import java.util.UUID
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{ PublicFileVersion, S3Key }
import com.pennsieve.discover.models.FileChecksum

import java.time.OffsetDateTime

final class PublicFileVersionsTable(tag: Tag)
    extends Table[PublicFileVersion](tag, "public_file_versions") {

  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def name = column[String]("name")
  def fileType = column[String]("file_type")
  def size = column[Long]("size")
  def sourcePackageId = column[Option[String]]("source_package_id")
  def sourceFileUUID = column[Option[UUID]]("source_file_uuid")
  def checksum = column[FileChecksum]("checksum")
  def s3Key = column[S3Key.File]("s3_key")
  def s3Version = column[String]("s3_version")
  def path = column[LTree]("path")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def * =
    (
      id,
      name,
      fileType,
      size,
      sourcePackageId,
      sourceFileUUID,
      checksum,
      s3Key,
      s3Version,
      path,
      createdAt,
      updatedAt
    ).mapTo[PublicFileVersion]
}
