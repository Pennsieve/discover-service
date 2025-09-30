// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.NoDoiCollectionVersionException
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  PublicDataset,
  PublicDatasetDoiCollection,
  PublicDatasetDoiCollectionWithSize,
  PublicDatasetVersion
}
import slick.dbio.{ DBIOAction, Effect }

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext

final class PublicDatasetDoiCollectionsTable(tag: Tag)
    extends Table[PublicDatasetDoiCollection](
      tag,
      "public_dataset_doi_collections"
    ) {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def datasetId = column[Int]("dataset_id")
  def datasetVersion = column[Int]("dataset_version")
  def banners = column[List[String]]("banners")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def * =
    (id, datasetId, datasetVersion, banners, createdAt, updatedAt)
      .mapTo[PublicDatasetDoiCollection]
}

object PublicDatasetDoiCollectionsMapper
    extends TableQuery(new PublicDatasetDoiCollectionsTable(_)) {

  def add(
    doiCollection: PublicDatasetDoiCollection
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicDatasetDoiCollection,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] =
    (this returning this) += doiCollection

  def getVersion(
    datasetId: Int,
    datasetVersion: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicDatasetDoiCollection, NoStream, Effect.Read] =
    this
      .filter(_.datasetId === datasetId)
      .filter(_.datasetVersion === datasetVersion)
      .result
      .headOption
      .flatMap {
        case None =>
          DBIO.failed(
            NoDoiCollectionVersionException(datasetId, datasetVersion)
          )
        case Some(dataset) => DBIO.successful(dataset)
      }

  def get(
    datasetId: Int,
    datasetVersion: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Option[PublicDatasetDoiCollection], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === datasetId)
      .filter(_.datasetVersion === datasetVersion)
      .result
      .headOption

  def getWithSize(
    datasetId: Int,
    datasetVersion: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Option[PublicDatasetDoiCollectionWithSize], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === datasetId)
      .filter(_.datasetVersion === datasetVersion)
      .joinLeft(PublicDatasetDoiCollectionDoisMapper)
      .on {
        case (doiCollection, doi) =>
          doiCollection.datasetId === doi.datasetId && doiCollection.datasetVersion === doi.datasetVersion
      }
      .groupBy {
        case (doiCollection, _) => doiCollection
      }
      .map {
        case (doiCollection, dois) => (doiCollection, dois.length)
      }
      .result
      .headOption
      .map { maybeResult =>
        maybeResult.map {
          case (doiCollection, doiCount) =>
            PublicDatasetDoiCollectionWithSize(
              id = doiCollection.id,
              datasetId = doiCollection.datasetId,
              datasetVersion = doiCollection.datasetVersion,
              banners = doiCollection.banners,
              size = doiCount,
              createdAt = doiCollection.createdAt,
              updatedAt = doiCollection.updatedAt
            )
        }
      }

  def getFor(
    targetDatasets: Seq[(PublicDataset, PublicDatasetVersion)]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    Map[(PublicDataset, PublicDatasetVersion), PublicDatasetDoiCollection],
    NoStream,
    Effect.Read
  ] = {
    PublicDatasetsMapper
      .join(PublicDatasetVersionsMapper.getMany(targetDatasets.map(_._2)))
      .on(_.id === _.datasetId)
      .join(PublicDatasetDoiCollectionsMapper)
      .on {
        case ((dataset, version), doiCollection) =>
          dataset.id === doiCollection.datasetId && version.version === doiCollection.datasetVersion
      }
      .result
      .map {
        _.toMap
      }
  }

  def getForWithSize(
    targetDatasets: Seq[(PublicDataset, PublicDatasetVersion)]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Map[
    (PublicDataset, PublicDatasetVersion),
    PublicDatasetDoiCollectionWithSize
  ], NoStream, Effect.Read] = {
    val baseQuery = PublicDatasetsMapper
      .join(PublicDatasetVersionsMapper.getMany(targetDatasets.map(_._2)))
      .on(_.id === _.datasetId)
      .join(PublicDatasetDoiCollectionsMapper)
      .on {
        case ((dataset, version), doiCollection) =>
          dataset.id === doiCollection.datasetId && version.version === doiCollection.datasetVersion
      }

    baseQuery
      .joinLeft(PublicDatasetDoiCollectionDoisMapper)
      .on {
        case (((dataset, version), _), doiCollectionDois) =>
          dataset.id === doiCollectionDois.datasetId && version.version === doiCollectionDois.datasetVersion
      }
      .groupBy {
        case (base, _) =>
          base // Group by the original result: ((Dataset, Version), DoiCollection)
      }
      .map {
        case (key, group) =>
          (key, group.length) // For each group, get the key and the COUNT
      }
      .result
      .map { results =>
        results.map {
          case (((dataset, version), doiCollection), count) =>
            (dataset, version) -> PublicDatasetDoiCollectionWithSize(
              id = doiCollection.id,
              datasetId = dataset.id,
              datasetVersion = version.version,
              banners = doiCollection.banners,
              size = count,
              createdAt = doiCollection.createdAt,
              updatedAt = doiCollection.updatedAt
            )
        }.toMap
      }
  }

}
