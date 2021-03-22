// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.db

import com.blackfynn.discover.db.profile.api._
import com.blackfynn.discover.models.{
  PublicContributor,
  PublicDataset,
  PublicDatasetVersion
}
import com.blackfynn.discover.NoDatasetVersionException
import com.blackfynn.models.{ Degree }

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }
import java.time.{ OffsetDateTime, ZoneOffset }

final class PublicContributorsTable(tag: Tag)
    extends Table[PublicContributor](tag, "public_contributors") {

  def firstName = column[String]("first_name")
  def middleInitial = column[Option[String]]("middle_initial")
  def lastName = column[String]("last_name")
  def degree = column[Option[Degree]]("degree")
  def orcid = column[Option[String]]("orcid")
  def datasetId = column[Int]("dataset_id")
  def version = column[Int]("version")
  def sourceContributorId = column[Int]("source_contributor_id")
  def sourceUserId = column[Option[Int]]("source_user_id")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

  def publicDatasetVersion =
    foreignKey(
      "public_dataset_versions_fk",
      (datasetId, version),
      PublicDatasetVersionsMapper
    )(v => (v.datasetId, v.version), onDelete = ForeignKeyAction.Cascade)

  def * =
    (
      firstName,
      middleInitial,
      lastName,
      degree,
      orcid,
      datasetId,
      version,
      sourceContributorId,
      sourceUserId,
      createdAt,
      updatedAt,
      id
    ).mapTo[PublicContributor]
}

object PublicContributorsMapper
    extends TableQuery(new PublicContributorsTable(_)) {

  def create(
    firstName: String,
    middleInitial: Option[String],
    lastName: String,
    degree: Option[Degree],
    orcid: Option[String],
    datasetId: Int,
    version: Int,
    sourceContributorId: Int,
    sourceUserId: Option[Int]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicContributor,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] = {
    val query: DBIOAction[
      PublicContributor,
      NoStream,
      Effect.Read with Effect with Effect.Write
    ] = for {
      result <- (this returning this) += PublicContributor(
        firstName,
        middleInitial,
        lastName,
        degree,
        orcid,
        datasetId,
        version,
        sourceContributorId,
        sourceUserId
      )
    } yield result

    query.transactionally
  }

  def get(id: Int): Query[PublicContributorsTable, PublicContributor, Seq] = {
    this.filter(_.id === id)
  }

  def getContributorsByDatasetAndVersion(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[List[PublicContributor], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.version === version.version)
      .result
      .map(_.toList)

  def deleteContributorsByDatasetAndVersion(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Int, NoStream, Effect.Write with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.version === version.version)
      .delete

  def getDatasetContributors(
    targetDatasets: Seq[(PublicDataset, PublicDatasetVersion)]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Map[(PublicDataset, PublicDatasetVersion), Seq[
    PublicContributor
  ]], NoStream, Effect.Read] = {
    PublicDatasetsMapper
      .join(PublicDatasetVersionsMapper.getMany(targetDatasets.map(_._2)))
      .on(_.id === _.datasetId)
      .join(PublicContributorsMapper)
      .on {
        case ((dataset, version), contributors) =>
          dataset.id === contributors.datasetId && version.version === contributors.version
      }
      .result
      .map {
        _.groupBy {
          case ((dataset, version), _) =>
            (dataset, version)
        }.mapValues(_.map {
          case ((_, _), contributor) => contributor
        })
      }
  }
}
