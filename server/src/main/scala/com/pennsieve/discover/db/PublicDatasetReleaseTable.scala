// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.PublicDatasetRelease

import java.time.OffsetDateTime

final class PublicDatasetReleaseTable(tag: Tag)
    extends Table[PublicDatasetRelease](tag, "public_dataset_release") {

  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def datasetId = column[Int]("dataset_id")
  def datasetVersion = column[Int]("dataset_version")
  def origin = column[String]("origin")
  def label = column[String]("label")
  def marker = column[String]("marker")
  def repoUrl = column[String]("repo_url")
  def labelUrl = column[Option[String]]("label_url")
  def markerUrl = column[Option[String]]("marker_url")
  def releaseStatus = column[Option[String]]("release_status")
  def assetFilePrefix = column[Option[String]]("asset_file_prefix")
  def assetFileId = column[Option[Int]]("asset_file_id")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def * =
    (
      id,
      datasetId,
      datasetVersion,
      origin,
      label,
      marker,
      repoUrl,
      labelUrl,
      markerUrl,
      releaseStatus,
      assetFilePrefix,
      assetFileId,
      createdAt,
      updatedAt
    ).mapTo[PublicDatasetRelease]
}

object PublicDatasetReleaseMapper
    extends TableQuery(new PublicDatasetReleaseTable(_)) {}
