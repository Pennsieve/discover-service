// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  PublicContributor,
  PublicDataset,
  PublicDatasetVersion,
  Revision
}

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }
import java.time.{ OffsetDateTime, ZoneOffset }

final class RevisionsTable(tag: Tag) extends Table[Revision](tag, "revisions") {

  def datasetId = column[Int]("dataset_id")
  def version = column[Int]("version")
  def revision = column[Int]("revision")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

  def publicDatasetVersion =
    foreignKey(
      "public_dataset_versions_fk",
      (datasetId, version),
      PublicDatasetVersionsMapper
    )(v => (v.datasetId, v.version))

  def * =
    (datasetId, version, revision, createdAt, updatedAt, id).mapTo[Revision]
}

object RevisionsMapper extends TableQuery(new RevisionsTable(_)) {

  def create(
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Revision, NoStream, Effect.Read with Effect.Write] =
    for {
      latestRevision <- getLatestRevision(version)

      nextRevision = latestRevision.map(_.revision + 1).getOrElse(1)

      revision <- (this returning this) += Revision(
        datasetId = version.datasetId,
        version = version.version,
        revision = nextRevision
      )
    } yield revision

  def getLatestRevision(
    version: PublicDatasetVersion
  ): DBIOAction[Option[Revision], NoStream, Effect.Read] =
    this
      .filter(_.datasetId === version.datasetId)
      .filter(_.version === version.version)
      .sortBy(_.revision.desc)
      .take(1)
      .result
      .headOption

  def getLatestRevisions(
    versions: Seq[PublicDatasetVersion]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    Map[PublicDatasetVersion, Option[Revision]],
    NoStream,
    Effect.Read
  ] = {
    PublicDatasetVersionsMapper
      .getMany(versions)
      .joinLeft(this)
      .on {
        case (version, revision) =>
          version.datasetId === revision.datasetId && version.version === revision.version
      }
      // This only works because the id is auto-incrementing: for any version,
      // the most recent revision will have the largest revision number *and*
      // the largest ID.
      .filter(
        _._2.map(_.id) in
          this
            .groupBy(revision => (revision.version, revision.datasetId))
            .map {
              case (_, revisions) => revisions.map(_.id).max
            }
      )
      .result
      .map(_.toMap)
  }
}
