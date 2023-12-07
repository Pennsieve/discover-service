// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ LocalDate, OffsetDateTime, ZoneOffset }

import com.pennsieve.models.PublishStatus

final case class PublicDatasetVersion(
  datasetId: Int,
  version: Int,
  size: Long,
  description: String,
  modelCount: Map[String, Long],
  fileCount: Long,
  recordCount: Long,
  s3Bucket: S3Bucket,
  s3Key: S3Key.Version,
  status: PublishStatus,
  doi: String,
  schemaVersion: PennsieveSchemaVersion,
  // S3 paths in Discover website bucket
  banner: Option[S3Key.File] = None,
  readme: Option[S3Key.File] = None,
  changelog: Option[S3Key.File] = None,
  embargoReleaseDate: Option[LocalDate] = None,
  fileDownloadsCounter: Int = 0,
  datasetDownloadsCounter: Int = 0,
  migrated: Boolean = false,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
) {

  def uri: String = s"s3://$s3Bucket/$s3Key"

  def arn: String = s"arn:aws:s3:::$s3Bucket/$s3Key"

  /**
    * If true, the dataset is under embargo and its data is not publicly available.
    */
  def underEmbargo: Boolean = PublicDatasetVersion.underEmbargo(status)
}

object PublicDatasetVersion {
  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled

  def underEmbargo(status: PublishStatus): Boolean = status in List(
    PublishStatus.EmbargoInProgress,
    PublishStatus.EmbargoFailed,
    PublishStatus.EmbargoSucceeded,
    PublishStatus.ReleaseInProgress,
    PublishStatus.ReleaseFailed
  )
}
