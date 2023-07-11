// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.PublicDatasetVersionFile

import java.time.OffsetDateTime

final class PublicDatasetVersionFilesTable(tag: Tag)
    extends Table[PublicDatasetVersionFile](tag, "") {

  def datasetId = column[Int]("dataset_id")
  def datasetVersion = column[Int]("dataset_version")
  def fileId = column[Int]("file_id")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def * =
    (datasetId, datasetVersion, fileId, createdAt, updatedAt)
      .mapTo[PublicDatasetVersionFile]
}

object PublicDatasetVersionFilesTableMapper
    extends TableQuery[PublicDatasetVersionFilesTable](
      new PublicDatasetVersionFilesTable(_)
    ) {}
