// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import java.time.OffsetDateTime
import com.pennsieve.discover.{
  NoDatasetException,
  NoDatasetForSourcesException
}
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.PublicDataset
import com.pennsieve.discover.server.definitions.{ DatasetMetrics, DatasetTag }
import com.pennsieve.models.{ DatasetType, License }
import com.pennsieve.models.PublishStatus.PublishSucceeded
import slick.dbio.{ DBIOAction, Effect }

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }

final class PublicDatasetsTable(tag: Tag)
    extends Table[PublicDataset](tag, "public_datasets") {

  def name = column[String]("name")
  def sourceOrganizationId = column[Int]("source_organization_id")
  def sourceOrganizationName = column[String]("source_organization_name")
  def sourceDatasetId = column[Int]("source_dataset_id")
  def ownerId = column[Int]("owner_id")
  def ownerFirstName = column[String]("owner_first_name")
  def ownerLastName = column[String]("owner_last_name")
  def ownerOrcid = column[String]("owner_orcid")
  def license = column[License]("license")
  def tags = column[List[String]]("tags")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def datasetType = column[DatasetType]("dataset_type")

  def * =
    (
      name,
      sourceOrganizationId,
      sourceOrganizationName,
      sourceDatasetId,
      // TODO map these to a nested `owner` case class
      ownerId,
      ownerFirstName,
      ownerLastName,
      ownerOrcid,
      license,
      tags,
      createdAt,
      updatedAt,
      id,
      datasetType
    ).mapTo[PublicDataset]
}

object PublicDatasetsMapper extends TableQuery(new PublicDatasetsTable(_)) {

  def get(id: Int): Query[PublicDatasetsTable, PublicDataset, Seq] = {
    this.filter(_.id === id)
  }

  def getDataset(
    id: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicDataset, NoStream, Effect.Read with Effect] = {
    this
      .get(id)
      .result
      .headOption
      .flatMap {
        case None => DBIO.failed(NoDatasetException(id))
        case Some(dataset) => DBIO.successful(dataset)
      }
  }

  def getDatasets(
    tags: Option[List[String]] = None,
    ids: Option[List[Int]] = None,
    datasetType: Option[DatasetType] = None
  ): Query[PublicDatasetsTable, PublicDataset, Seq] = {
    val givenTags: List[String] = tags.toList.flatten.map(_.toLowerCase)

    val givenIds: List[Int] = ids.toList.flatten

    val givenType: List[DatasetType] = datasetType match {
      case Some(t) => List(t)
      case None => DatasetType.values.toList
    }

    if (givenIds.nonEmpty)
      this
        .filter(_.datasetType inSet givenType)
        .filter(_.id inSet givenIds)
        .filter(_.tags @> givenTags.bind)
    else
      this
        .filter(_.datasetType inSet givenType)
        .filter(_.tags @> givenTags.bind)
  }

  def getDatasetsByOrganization(
    sourceOrganizationId: Int
  )(implicit
    executionContext: ExecutionContext
  ): Query[PublicDatasetsTable, PublicDataset, Seq] = {
    this
      .filter(_.sourceOrganizationId === sourceOrganizationId)
  }

  def getDatasetFromSourceIds(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicDataset, NoStream, Effect.Read with Effect] =
    this
      .filter(_.sourceOrganizationId === sourceOrganizationId)
      .filter(_.sourceDatasetId === sourceDatasetId)
      .result
      .headOption
      .flatMap {
        case None =>
          DBIO.failed(
            NoDatasetForSourcesException(sourceOrganizationId, sourceDatasetId)
          )
        case Some(dataset) => DBIO.successful(dataset)
      }

  def getTagCounts(
    implicit
    executionContext: ExecutionContext
  ): DBIOAction[List[DatasetTag], NoStream, Effect.Read] =
    this
      .join(
        PublicDatasetVersionsMapper
          .getLatestDatasetVersions(status = PublishSucceeded)
      )
      .on(_.id === _.datasetId)
      .map(_._1.tags)
      .result
      .map { allTags =>
        allTags.flatten.map(_.toLowerCase)
      }
      .map { tags =>
        tags
          .groupBy(identity)
          .view
          .mapValues(_.size)
          .toList
          .sortBy(_._1)
          .map {
            case (tag, count) => DatasetTag(tag, count)
          }
      }

  def updateDataset(
    dataset: PublicDataset,
    name: String,
    ownerId: Int,
    ownerFirstName: String,
    ownerLastName: String,
    ownerOrcid: String,
    license: License,
    tags: List[String]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicDataset,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] = {
    val updated = dataset.copy(
      name = name,
      ownerId = ownerId,
      ownerFirstName = ownerFirstName,
      ownerLastName = ownerLastName,
      ownerOrcid = ownerOrcid,
      tags = cleanTags(tags),
      license = license
    )

    for {
      _ <- this
        .filter(_.id === dataset.id)
        .update(updated)
    } yield updated
  }

  def createOrUpdate(
    name: String,
    sourceOrganizationId: Int,
    sourceOrganizationName: String,
    sourceDatasetId: Int,
    ownerId: Int,
    ownerFirstName: String,
    ownerLastName: String,
    ownerOrcid: String,
    license: License,
    tags: List[String] = List.empty,
    datasetType: DatasetType = DatasetType.Research
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicDataset,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] = {

    def updateDataset =
      for {
        dataset <- getDatasetFromSourceIds(
          sourceOrganizationId,
          sourceDatasetId
        )
        updated <- this.updateDataset(
          dataset,
          name = name,
          ownerId = ownerId,
          ownerFirstName = ownerFirstName,
          ownerLastName = ownerLastName,
          ownerOrcid = ownerOrcid,
          tags = tags,
          license = license
        )
      } yield updated

    def createDataset = (this returning this) += PublicDataset(
      name,
      sourceOrganizationId,
      sourceOrganizationName,
      sourceDatasetId,
      ownerId,
      ownerFirstName,
      ownerLastName,
      ownerOrcid,
      license,
      cleanTags(tags),
      datasetType = datasetType
    )

    updateDataset.asTry.flatMap {
      case Failure(_: NoDatasetForSourcesException) => createDataset
      case Failure(e) => DBIO.failed(e)
      case Success(s) => DBIO.successful(s)
    }.transactionally
  }

  def cleanTags(tags: List[String]): List[String] =
    tags.map(_.toLowerCase.trim).distinct

  def getOrganizationDatasetMetrics(
    organizationId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIO[Seq[DatasetMetrics]] = {
    sql"""
    SELECT V.dataset_id,
           D.name,
           V.total_size
     FROM  public_datasets D
           INNER JOIN public_dataset_versions V
             ON (V.dataset_id = D.id)
     WHERE (V.dataset_id, V.version) IN (SELECT dataset_id, MAX(version) FROM public_dataset_versions GROUP BY dataset_id)
           AND D.source_organization_id = ${organizationId}
     ORDER BY D.updated_at DESC"""
      .as[(Int, String, Long)]
      .map { r =>
        {
          r.map {
            case (id, name, size) => {
              DatasetMetrics(id = id, name = name, size = size)
            }
          }
        }
      }
  }
}
