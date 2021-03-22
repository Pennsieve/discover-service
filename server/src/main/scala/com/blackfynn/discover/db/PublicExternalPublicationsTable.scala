// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.db

import com.blackfynn.discover.db.profile.api._
import com.blackfynn.discover.models.{
  PublicDataset,
  PublicDatasetVersion,
  PublicExternalPublication
}
import com.blackfynn.models.RelationshipType

import scala.concurrent.ExecutionContext
import java.time.OffsetDateTime

final class PublicExternalPublicationsTable(tag: Tag)
    extends Table[PublicExternalPublication](
      tag,
      "public_external_publications"
    ) {

  def doi = column[String]("doi")
  def relationshipType = column[RelationshipType]("relationship_type")
  def datasetId = column[Int]("dataset_id")
  def version = column[Int]("version")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def publicDatasetVersion =
    foreignKey(
      "public_dataset_versions_fk",
      (datasetId, version),
      PublicDatasetVersionsMapper
    )(v => (v.datasetId, v.version), onDelete = ForeignKeyAction.Cascade)

  def * =
    (doi, relationshipType, datasetId, version, createdAt, updatedAt)
      .mapTo[PublicExternalPublication]
}

object PublicExternalPublicationsMapper
    extends TableQuery(new PublicExternalPublicationsTable(_)) {

  def create(
    doi: String,
    relationshipType: RelationshipType,
    datasetId: Int,
    version: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicExternalPublication,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] = {
    val query = (this returning this) += PublicExternalPublication(
      doi = doi,
      relationshipType = relationshipType,
      datasetId = datasetId,
      version = version
    )
    query.transactionally
  }

  def deleteByDatasetAndVersion(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Int, NoStream, Effect.Write with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.version === version.version)
      .delete

  def getByDatasetAndVersion(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[List[PublicExternalPublication], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.version === version.version)
      .result
      .map(_.toList)

  def getExternalPublications(
    targetDatasets: Seq[(PublicDataset, PublicDatasetVersion)]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Map[(PublicDataset, PublicDatasetVersion), Seq[
    PublicExternalPublication
  ]], NoStream, Effect.Read] = {
    PublicDatasetsMapper
      .join(PublicDatasetVersionsMapper.getMany(targetDatasets.map(_._2)))
      .on(_.id === _.datasetId)
      .join(PublicExternalPublicationsMapper)
      .on {
        case ((dataset, version), externalPublications) =>
          dataset.id === externalPublications.datasetId && version.version === externalPublications.version
      }
      .result
      .map {
        _.groupBy {
          case ((dataset, version), _) =>
            (dataset, version)
        }.mapValues(_.map {
          case ((_, _), collection) => collection
        })
      }
  }
}
