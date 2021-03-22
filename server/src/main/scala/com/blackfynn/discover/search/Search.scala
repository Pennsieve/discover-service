// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.search

import akka.{ Done, NotUsed }
import akka.stream._
import akka.stream.scaladsl._

import cats.data._
import cats.implicits._
import com.pennsieve.discover.Ports
import com.pennsieve.discover.db._
import com.pennsieve.discover.models._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.models.PublishStatus
import com.pennsieve.discover.logging.DiscoverLogContext
import com.pennsieve.discover.models.{ DatasetDocument, FileDocument }
import com.pennsieve.discover.S3Exception
import com.pennsieve.service.utilities.LogContext
import com.sksamuel.elastic4s.Index
import com.typesafe.scalalogging.StrictLogging

import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.{ Decoder, Encoder }

import java.time.OffsetDateTime
import scala.concurrent.{ ExecutionContext, Future }

object Search extends StrictLogging {

  /**
    * Synchronize the Elastic index with the datasets in Postgres.
    */
  def buildSearchIndex(
    ports: Ports
  )(implicit
    executionContext: ExecutionContext,
    materializer: ActorMaterializer
  ): Future[Done] = {
    val query = PublicDatasetsMapper
      .join(
        PublicDatasetVersionsMapper
          .getLatestDatasetVersions(
            List(
              PublishStatus.PublishSucceeded,
              PublishStatus.EmbargoSucceeded,
              PublishStatus.ReleaseInProgress,
              PublishStatus.ReleaseFailed
            )
          )
      )
      .on(_.id === _.datasetId)
      .result
      .map(_.toList)
      .map(Source(_))

    // TODO refactor this into a for-comprehension
    ports.searchClient.buildIndexAndReplaceAlias(
      ports.searchClient.datasetAlias,
      ports.searchClient.datasetMapping
    )(
      datasetIndex =>
        ports.searchClient
          .buildIndexAndReplaceAlias(
            ports.searchClient.fileAlias,
            ports.searchClient.fileMapping
          )(
            fileIndex =>
              ports.searchClient
                .buildIndexAndReplaceAlias(
                  ports.searchClient.recordAlias,
                  ports.searchClient.recordMapping
                )(
                  recordIndex =>
                    Source
                      .fromFutureSource(ports.db.run(query))
                      .mapAsync(1) {
                        case (
                            dataset: PublicDataset,
                            version: PublicDatasetVersion
                            ) => {

                          implicit val logContext = DiscoverLogContext(
                            publicDatasetId = Some(dataset.id),
                            publicDatasetVersion = Some(version.version)
                          )

                          indexDataset(
                            dataset,
                            version,
                            ports,
                            datasetIndex = Some(datasetIndex),
                            fileIndex = Some(fileIndex),
                            recordIndex = Some(recordIndex)
                          ).recover {
                            case e: S3Exception =>
                              ports.log
                                .error(s"Could not get readme from S3", e)
                          }
                        }
                      }
                      .runWith(Sink.ignore)
                )
          )
    )
  }

  /**
    * Collect and index all information about a dataset version into
    * Elasticsearch. All existing information for the dataset is removed.
    */
  def indexDataset(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    ports: Ports,
    datasetIndex: Option[Index] = None,
    fileIndex: Option[Index] = None,
    recordIndex: Option[Index] = None
  )(implicit
    executionContext: ExecutionContext,
    materializer: ActorMaterializer,
    logContext: LogContext
  ): Future[Done] = {

    ports.log.info("Indexing dataset")

    for {

      (contributors, collections, externalPublications, sponsorship, revision) <- ports.db
        .run(PublicDatasetVersionsMapper.getDatasetDetails(dataset, version))

      files = if (version.underEmbargo) Source.empty[PublicFile]
      else
        Source.fromPublisher(
          PublicFilesMapper
            .streamForVersion(version, ports.db)
        )

      records = if (version.underEmbargo) Source.empty[Record]
      else
        ports.s3StreamClient
          .datasetRecordSource(dataset, version)

      readme <- ports.s3StreamClient
        .readDatasetReadme(version, revision)

      _ <- ports.searchClient
        .indexDataset(
          dataset,
          version,
          contributors,
          readme,
          revision,
          collections,
          externalPublications,
          sponsorship,
          files,
          records,
          datasetIndex = datasetIndex,
          fileIndex = fileIndex,
          recordIndex = recordIndex
        )

    } yield Done

  }
}
