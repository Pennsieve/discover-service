// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.NoDoiCollectionVersionException
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.PublicDatasetDoiCollection
import slick.dbio.{ DBIOAction, Effect }

import java.time.{ OffsetDateTime, ZoneOffset }
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
}
