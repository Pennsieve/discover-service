// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.github.tminglei.slickpg.LTree
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.PublicDatasetReleaseAsset

import java.time.OffsetDateTime

class PublicDatasetReleaseAssetsTable(tag: Tag)
    extends Table[PublicDatasetReleaseAsset](
      tag,
      "public_dataset_release_assets"
    ) {

  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def datasetId = column[Int]("dataset_id")
  def datasetVersion = column[Int]("dataset_version")
  def releaseId = column[Int]("release_id")
  def file = column[String]("file")
  def name = column[String]("name")
  def `type` = column[String]("type")
  def size = column[Long]("size")
  def path = column[LTree]("path")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def * =
    (
      id,
      datasetId,
      datasetVersion,
      releaseId,
      file,
      name,
      `type`,
      size,
      path,
      createdAt,
      updatedAt
    ).mapTo[PublicDatasetReleaseAsset]

}

object PublicDatasetReleaseAssetMapper
    extends TableQuery(new PublicDatasetReleaseAssetsTable(_)) {}
