// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import akka.Done
import cats.implicits._
import com.github.tminglei.slickpg.LTree
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  AssetTreeNode,
  PublicDataset,
  PublicDatasetRelease,
  PublicDatasetReleaseAsset,
  PublicDatasetVersion,
  ReleaseAssetFile,
  ReleaseAssetFileType,
  ReleaseAssetListing,
  S3Key
}
import slick.jdbc.GetResult
import slick.dbio.{ DBIOAction, Effect }

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

  private def convertPathToTree(key: S3Key.File): String =
    key.value
      .split("/")
      .map(_.getBytes(StandardCharsets.UTF_8))
      .map(pgSafeBase64)
      .mkString(".")

  private def buildAssetNodeGetter(
  ): GetResult[Either[TotalCount, AssetTreeNode]] =
    GetResult(r => {
      val nodeType = r.nextString()

      nodeType match {
        case "count" => Left(TotalCount(r.skip.skip.skip.nextLong()))
        case "file" =>
          Right(
            AssetTreeNode(
              `type` = r.nextString(),
              file = r.nextString(),
              name = r.nextString(),
              size = r.nextLong()
            )
          )
      }
    })

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
      path = LTree(
        PublicDatasetReleaseAssetMapper.convertPathToTree(S3Key.File(file.file))
      )
    )

  def get(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Seq[PublicDatasetReleaseAsset], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.datasetVersion === version.version)
      .result

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

  def fullTree(
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    (TotalCount, Seq[AssetTreeNode]),
    NoStream,
    Effect.Read with Effect
  ] = {
    implicit val getTreeNodeResult
      : GetResult[Either[TotalCount, AssetTreeNode]] =
      buildAssetNodeGetter()

    val datasetId = version.datasetId
    val datasetVersion = version.version

    val result =
      sql"""
        WITH files AS (
          SELECT type,
                 file,
                 name,
                 size
          FROM discover.public_dataset_release_assets
          WHERE dataset_id = $datasetId
          AND dataset_version = $datasetVersion
        ),
        total_count as (
          select null::text AS type,
                 null::text AS file,
                 null::text AS name,
                 (SELECT COALESCE(COUNT(*), 0) FROM files) AS size
        )
        SELECT 'count', * FROM total_count
        UNION (
        SELECT 'file', * FROM files
        )
        ORDER BY 1 ASC
         """.as[Either[TotalCount, AssetTreeNode]]

    for {
      rows <- result
      (counts, nodes) = rows.separate
      totalCount <- counts.headOption
        .map(DBIO.successful(_))
        .getOrElse(DBIO.failed(new Exception("missing 'count'")))
    } yield (totalCount, nodes)
  }

  def childrenOf(
    version: PublicDatasetVersion,
    path: Option[String],
    name: Option[String] = None,
    limit: Int = 100,
    offset: Int = 0
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    (TotalCount, Seq[AssetTreeNode]),
    NoStream,
    Effect.Read with Effect
  ] = {
    implicit val getTreeNodeResult
      : GetResult[Either[TotalCount, AssetTreeNode]] =
      buildAssetNodeGetter()

    val leafChildSelector = path match {
      case Some(path) => convertPathToTree(S3Key.File(path)) + ".*{1}"
      case None => "*{1}"
    }

    val nameSelector = name match {
      case Some(name) => name
      case None => "%"
    }

    val datasetId = version.datasetId
    val datasetVersion = version.version

    val result =
      sql"""
        WITH files AS (
          SELECT type,
                 file,
                 name,
                 size
          FROM discover.public_dataset_release_assets
          WHERE dataset_id = $datasetId
          AND dataset_version = $datasetVersion
          AND path ~ $leafChildSelector::lquery
          AND name LIKE $nameSelector
        ),
        total_count as (
          select null::text AS type,
                 null::text AS file,
                 null::text AS name,
                 (SELECT COALESCE(COUNT(*), 0) FROM files) AS size
        )
        SELECT 'count', * FROM total_count
        UNION (
        SELECT 'file', * FROM files
        )
        ORDER BY 1 ASC
        LIMIT $limit
        OFFSET $offset;
         """.as[Either[TotalCount, AssetTreeNode]]

    for {
      rows <- result
      (counts, nodes) = rows.separate
      totalCount <- counts.headOption
        .map(DBIO.successful(_))
        .getOrElse(DBIO.failed(new Exception("missing 'count'")))
    } yield (totalCount, nodes)
  }
}
