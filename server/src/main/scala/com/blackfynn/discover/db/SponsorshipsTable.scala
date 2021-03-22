// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.db

import com.blackfynn.discover.{
  NoSponsorshipException,
  NoSponsorshipForDatasetException
}
import com.blackfynn.discover.db.profile.api._
import com.blackfynn.discover.models.{ PublicDataset, Sponsorship }
import slick.dbio.{ DBIOAction, Effect }
import slick.sql.FixedSqlAction

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }

final class SponsorshipsTable(tag: Tag)
    extends Table[Sponsorship](tag, "sponsorships") {

  def datasetId = column[Int]("dataset_id")
  def title = column[Option[String]]("title")
  def imageUrl = column[Option[String]]("image_url")
  def markup = column[Option[String]]("markup")
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

  def * =
    (datasetId, title, imageUrl, markup, id).mapTo[Sponsorship]

}

object SponsorshipsMapper extends TableQuery(new SponsorshipsTable(_)) {

  private def get(id: Int): Query[SponsorshipsTable, Sponsorship, Seq] = {
    this.filter(_.id === id)
  }

  def maybeGetByDataset(
    dataset: PublicDataset
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Option[Sponsorship], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .result
      .headOption

  def getByDataset(
    dataset: PublicDataset
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Sponsorship, NoStream, Effect.Read with Effect] =
    maybeGetByDataset(dataset)
      .flatMap {
        case None =>
          DBIO.failed(NoSponsorshipForDatasetException(dataset.id))
        case Some(sponsorship) => DBIO.successful(sponsorship)
      }

  def createOrUpdate(
    sourceOrganizationId: Int,
    sourceDatasetId: Int,
    title: Option[String],
    imageUrl: Option[String],
    markup: Option[String]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    Sponsorship,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] = {

    def updateSponsorship(dataset: PublicDataset) =
      for {
        sponsorship <- getByDataset(dataset)
        updated = sponsorship.copy(
          title = title,
          imageUrl = imageUrl,
          markup = markup
        )
        _ <- this
          .get(sponsorship.id)
          .update(updated)
      } yield updated

    def createSponsorship(dataset: PublicDataset) =
      (this returning this) += Sponsorship(
        datasetId = dataset.id,
        title = title,
        imageUrl = imageUrl,
        markup = markup
      )

    PublicDatasetsMapper
      .getDatasetFromSourceIds(sourceOrganizationId, sourceDatasetId)
      .asTry
      .flatMap {
        case Success(d) => {
          updateSponsorship(d).asTry.flatMap {
            case Failure(_: NoSponsorshipForDatasetException) =>
              createSponsorship(d)
            case Failure(e) => DBIO.failed(e)
            case Success(s) => DBIO.successful(s)
          }
        }
        case Failure(e) => DBIO.failed(e)
      }

  }

  def delete(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    Unit,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] =
    for {
      dataset <- PublicDatasetsMapper.getDatasetFromSourceIds(
        sourceOrganizationId,
        sourceDatasetId
      )

      sponsorship <- getByDataset(dataset)

      result <- get(sponsorship.id).delete

    } yield ()
}
