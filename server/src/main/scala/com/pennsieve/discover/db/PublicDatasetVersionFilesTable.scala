// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import akka.Done
import com.pennsieve.discover.db.PublicFileVersionsMapper.buildFileVersion
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  PublicDatasetVersion,
  PublicDatasetVersionFile,
  PublicFileVersion
}
import slick.dbio.DBIOAction

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext

final class PublicDatasetVersionFilesTable(tag: Tag)
    extends Table[PublicDatasetVersionFile](tag, "public_dataset_version_files") {

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
    ) {

  def storeLink(
    datasetId: Int,
    datasetVersion: Int,
    fileId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicDatasetVersionFile,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] = (this returning this) += PublicDatasetVersionFile(
    datasetId = datasetId,
    datasetVersion = datasetVersion,
    fileId = fileId
  )

  def storeLink(
    version: PublicDatasetVersion,
    fileVersion: PublicFileVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicDatasetVersionFile,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] = (this returning this) += PublicDatasetVersionFile(
    datasetId = version.datasetId,
    datasetVersion = version.version,
    fileId = fileVersion.id
  )

  def storeLinks(
    version: PublicDatasetVersion,
    files: Seq[PublicFileVersion]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    Done,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] =
    DBIO
      .sequence(
        files
          .map(buildDatasetVersionFile(version, _))
          .grouped(10000)
          .map((this returning this) ++= _)
          .toList
      )
      .map(_ => Done)
      .transactionally

  def getLinks(
    datasetId: Int,
    versionId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Seq[PublicDatasetVersionFile], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === datasetId)
      .filter(_.datasetVersion === versionId)
      .result

  def buildDatasetVersionFile(
    version: PublicDatasetVersion,
    file: PublicFileVersion
  ): PublicDatasetVersionFile =
    PublicDatasetVersionFile(
      datasetId = version.datasetId,
      datasetVersion = version.version,
      fileId = file.id
    )

}
