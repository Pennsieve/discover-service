// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.PublicDatasetDoiCollectionDoi
import slick.dbio.{ DBIOAction, Effect }

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext

final class PublicDatasetDoiCollectionDoisTable(tag: Tag)
    extends Table[PublicDatasetDoiCollectionDoi](
      tag,
      "public_dataset_doi_collection_dois"
    ) {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def datasetId = column[Int]("dataset_id")
  def datasetVersion = column[Int]("dataset_version")
  def doi = column[String]("doi")
  def position = column[Int]("position")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def * =
    (id, datasetId, datasetVersion, doi, position, createdAt, updatedAt)
      .mapTo[PublicDatasetDoiCollectionDoi]
}

object PublicDatasetDoiCollectionDoisMapper
    extends TableQuery(new PublicDatasetDoiCollectionDoisTable(_)) {

  def addDOIs(
    datasetId: Int,
    datasetVersion: Int,
    dois: List[String]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Option[Int], NoStream, Effect.Read with Effect.Write with Effect.Transactional with Effect] =
    this ++= dois.zipWithIndex
      .map(
        z =>
          PublicDatasetDoiCollectionDoi(
            datasetId = datasetId,
            datasetVersion = datasetVersion,
            doi = z._1,
            position = z._2
          )
      )

  def getDOIs(
    datasetId: Int,
    datasetVersion: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[List[String], NoStream, Effect.Read] =
    this
      .filter(_.datasetId === datasetId)
      .filter(_.datasetVersion === datasetVersion)
      .sortBy(_.position.asc)
      .map(_.doi)
      .result
      .map(_.toList)

  def getDOIPage(
    datasetId: Int,
    datasetVersion: Int,
    limit: Int = 10,
    offset: Int = 0
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PagedDoiResult, NoStream, Effect.Read] = {
    // leaving off the () after over because slick adds a pair, I guess because it
    // thinks of it as a function?
    val countOver = SimpleFunction.nullary[Long]("count(*) over ")
    this
      .filter(_.datasetId === datasetId)
      .filter(_.datasetVersion === datasetVersion)
      .sortBy(_.position.asc)
      .map(d => (d, countOver))
      .drop(offset)
      .take(limit)
      .result
      .map { r =>
        val dois = r.map(_._1.doi).toList
        val totalCount = r.headOption.map(_._2).getOrElse(0L)
        PagedDoiResult(limit, offset, totalCount, dois)
      }

  }
}

case class PagedDoiResult(
  limit: Int,
  offset: Int,
  totalCount: Long,
  dois: List[String]
)
