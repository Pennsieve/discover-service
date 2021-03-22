// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.db

import java.time.{ LocalDate, OffsetDateTime, ZoneOffset }

import com.blackfynn.discover.db.profile.api._

import scala.concurrent.ExecutionContext
import com.blackfynn.discover.models._
import com.blackfynn.discover.server.definitions.DatasetDownloadSummaryRow

final class DatasetDownloadsTable(tag: Tag)
    extends Table[DatasetDownload](tag, "dataset_downloads") {

  def datasetId = column[Int]("dataset_id")
  def version = column[Int]("version")
  def origin = column[Option[DownloadOrigin]]("origin")
  def requestId = column[Option[String]]("request_id")
  def downloadedAt = column[OffsetDateTime]("downloaded_at")

  def * =
    (datasetId, version, origin, requestId, downloadedAt).mapTo[DatasetDownload]
}

object DatasetDownloadsMapper extends TableQuery(new DatasetDownloadsTable(_)) {

  def create(
    datasetId: Int,
    version: Int,
    origin: Option[DownloadOrigin],
    requestId: Option[String] = None,
    dateTime: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    DatasetDownload,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] = {
    val query: DBIOAction[
      DatasetDownload,
      NoStream,
      Effect.Read with Effect with Effect.Write
    ] = for {
      result <- (this returning this) += DatasetDownload(
        datasetId,
        version,
        origin,
        requestId,
        dateTime
      )
    } yield result

    query.transactionally
  }

  def getDatasetDownloadsByDatasetAndVersion(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[List[DatasetDownload], NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.version === version.version)
      .result
      .map(_.toList)

  def getDatasetDownloadsByDateRange(
    startDate: OffsetDateTime,
    endDate: OffsetDateTime
  )(implicit
    executionContext: ExecutionContext
  ) = {
    this
      .filter(_.downloadedAt >= startDate)
      .filter(_.downloadedAt < endDate)
      .result
      .map(_.toList)
  }

  def getDatasetDownloadSummaryByDateRange(
    startDate: LocalDate,
    endDate: LocalDate
  )(implicit
    executionContext: ExecutionContext
  ) =
    sql"""
    SELECT dataset_id,
           version,
           origin,
           count(1)
     FROM  dataset_downloads
     WHERE downloaded_at >= '#$startDate'
       and downloaded_at < '#$endDate'
     GROUP BY dataset_id,
           version,
           origin
     ORDER BY dataset_id, version, origin"""
      .as[(Int, Int, String, Int)]
      .map { r =>
        {
          r.map {
            case (datasetId, version, origin, counter) => {
              DatasetDownloadSummaryRow(
                datasetId,
                version,
                DownloadOrigin
                  .withNameOption(origin)
                  .getOrElse(DownloadOrigin.Unknown)
                  .entryName,
                counter
              )
            }
          }
        }
      }
}
