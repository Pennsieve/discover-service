// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import akka.Done
import com.github.tminglei.slickpg.LTree
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  PublicDatasetRelease,
  PublicDatasetReleaseAsset,
  PublicDatasetVersion,
  ReleaseAssetFile,
  ReleaseAssetFileType,
  ReleaseAssetListing,
  S3Key
}

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext

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
  def `type` = column[ReleaseAssetFileType]("type")
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
    extends TableQuery(new PublicDatasetReleaseAssetsTable(_)) {

  private def convertPathToTree(path: String): String =
    path
      .split("/")
      .map(_.getBytes(StandardCharsets.UTF_8))
      .map(pgSafeBase64)
      .mkString(".")

  private def buildReleaseAsset(
    version: PublicDatasetVersion,
    release: PublicDatasetRelease,
    file: ReleaseAssetFile
  ): PublicDatasetReleaseAsset =
    PublicDatasetReleaseAsset(
      datasetId = version.datasetId,
      datasetVersion = version.version,
      releaseId = release.id,
      file = file.file,
      name = file.name,
      `type` = file.`type`,
      size = file.size,
      path = LTree(PublicDatasetReleaseAssetMapper.convertPathToTree(file.file))
    )

  def createMany(
    version: PublicDatasetVersion,
    release: PublicDatasetRelease,
    listing: ReleaseAssetListing
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    Done,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] =
    DBIO
      .sequence(
        listing.files
          .map(buildReleaseAsset(version, release, _))
          .grouped(10000)
          .map(this ++= _)
          .toList
      )
      .map(_ => Done)
      .transactionally
}
