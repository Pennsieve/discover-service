// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  PublicCollection,
  PublicDataset,
  PublicDatasetVersion
}

import scala.concurrent.ExecutionContext
import java.time.OffsetDateTime

final class PublicCollectionsTable(tag: Tag)
    extends Table[PublicCollection](tag, "public_collections") {

  def name = column[String]("name")
  def datasetId = column[Int]("dataset_id")
  def version = column[Int]("version")
  def sourceCollectionId = column[Int]("source_collection_id")
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
    (name, datasetId, version, sourceCollectionId, createdAt, updatedAt, id)
      .mapTo[PublicCollection]
}

object PublicCollectionsMapper
    extends TableQuery(new PublicCollectionsTable(_)) {

  def create(
    name: String,
    datasetId: Int,
    version: Int,
    sourceCollectionId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicCollection,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] = {
    val query: DBIOAction[
      PublicCollection,
      NoStream,
      Effect.Read with Effect with Effect.Write
    ] = for {
      result <- (this returning this) += PublicCollection(
        name,
        datasetId,
        version,
        sourceCollectionId
      )
    } yield result

    query.transactionally
  }

  def deleteCollectionsByDatasetAndVersion(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Int, NoStream, Effect.Write with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.version === version.version)
      .delete

  def getCollectionsByDatasetAndVersion(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[List[PublicCollection], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.version === version.version)
      .result
      .map(_.toList)

  def getDatasetCollections(
    targetDatasets: Seq[(PublicDataset, PublicDatasetVersion)]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Map[(PublicDataset, PublicDatasetVersion), Seq[
    PublicCollection
  ]], NoStream, Effect.Read] = {
    PublicDatasetsMapper
      .join(PublicDatasetVersionsMapper.getMany(targetDatasets.map(_._2)))
      .on(_.id === _.datasetId)
      .join(PublicCollectionsMapper)
      .on {
        case ((dataset, version), collections) =>
          dataset.id === collections.datasetId && version.version === collections.version
      }
      .result
      .map {
        _.groupBy {
          case ((dataset, version), _) =>
            (dataset, version)
        }.view
          .mapValues(_.map {
            case ((_, _), collection) => collection
          })
          .toMap
      }
  }
}
