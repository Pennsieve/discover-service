// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import java.util.Calendar
import akka.NotUsed
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.alpakka.sqs.scaladsl.{ SqsAckSink, SqsSource }
import akka.stream.scaladsl.{ Flow, Keep, RunnableGraph, Source }
import akka.stream.{ ActorMaterializer, _ }
import cats.data._
import cats.implicits._
import com.blackfynn.discover.models.DoiRedirect
import com.pennsieve.discover.db.{
  PublicCollectionsMapper,
  PublicContributorsMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicExternalPublicationsMapper,
  PublicFilesMapper
}
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.logging.DiscoverLogContext
import com.pennsieve.discover.models._
import com.pennsieve.discover.search.Search
import com.pennsieve.discover.server.definitions.{
  DatasetPublishStatus,
  InternalContributor
}
import com.pennsieve.discover.{ Authenticator, Ports, UnauthorizedException }
import com.pennsieve.doi.client.definitions.PublishDoiRequest
import com.pennsieve.doi.models.{ DoiDTO, DoiState }
import com.pennsieve.models.{ DatasetMetadata, PublishStatus }
import com.pennsieve.service.utilities.LogContext
import io.circe.parser.decode
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient

import java.time.LocalDate
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Handler that listens on an SQS queue for success/failure updates from the
  * publish job.
  *
  * Updating the publish status is idempotent, so the handler does not worry
  * about de-duplication.
  */
class SQSNotificationHandler(
  ports: Ports,
  region: Region,
  queueUrl: String,
  parallelism: Int
)(implicit
  executionContext: ExecutionContext,
  materializer: ActorMaterializer
) {

  implicit val sqsClient: SqsAsyncClient = ports.sqsClient

  def graph(): RunnableGraph[UniqueKillSwitch] = {
    SqsSource(queueUrl)
      .withAttributes(ActorAttributes.supervisionStrategy(error => {
        ports.logger.noContext.error("Stream error", error)
        Supervision.Resume
      }))
      .viaMat(KillSwitches.single)(Keep.right)
      .via(notificationFlow)
      .toMat(SqsAckSink(queueUrl))(Keep.left)
  }

  def notificationFlow: Flow[Message, MessageAction, NotUsed] =
    Flow[Message]
      .mapAsync(parallelism)(handleNotification)

  private def handleNotification(sqsMessage: Message): Future[MessageAction] = {
    decode[SQSNotification](sqsMessage.body) match {
      case Right(message: ScanForReleaseNotification) =>
        releaseEmbargoedDatasets()
          .map(_ => MessageAction.Delete(sqsMessage))
          // Send failed messages back to queue
          .recoverWith {
            case error: Throwable =>
              ports.logger.noContext
                .error(s"Failed to scan and release datasets", error)
              Future.successful(MessageAction.Ignore(sqsMessage))
          }

      case Right(message: JobDoneNotification) =>
        implicit val logContext: LogContext =
          DiscoverLogContext(
            organizationId = Some(message.organizationId),
            datasetId = Some(message.datasetId)
          )

        ports.log.info(s"Decoded $message")

        val query = for {
          publicDataset <- PublicDatasetsMapper
            .getDatasetFromSourceIds(
              sourceOrganizationId = message.organizationId,
              sourceDatasetId = message.datasetId
            )

          version <- PublicDatasetVersionsMapper.getVersion(
            publicDataset.id,
            message.version
          )

          version <- PublicDatasetVersionsMapper.setStatus(
            id = publicDataset.id,
            version = version.version,
            status = (version.underEmbargo, message) match {
              case (false, m: PublishNotification) if m.success =>
                PublishStatus.PublishSucceeded

              case (false, m: PublishNotification) if !m.success =>
                PublishStatus.PublishFailed

              case (true, m: PublishNotification) if m.success =>
                PublishStatus.EmbargoSucceeded

              case (true, m: PublishNotification) if !m.success =>
                PublishStatus.EmbargoFailed

              case (_, m: ReleaseNotification) if m.success =>
                PublishStatus.PublishSucceeded

              case (_, m: ReleaseNotification) if !m.success =>
                PublishStatus.ReleaseFailed
            }
          )
          publishStatus <- PublicDatasetVersionsMapper.getDatasetStatus(
            publicDataset
          )

          (contributors, collections, externalPublications, _, _) <- PublicDatasetVersionsMapper
            .getDatasetDetails(publicDataset, version)

        } yield
          (
            publicDataset,
            version,
            publishStatus,
            contributors,
            collections,
            externalPublications
          )

        (for {
          (
            publicDataset,
            version,
            publishStatus,
            contributors,
            collections,
            externalPublications
          ) <- ports.db.run(query)

          _ <- message match {
            case notification: PublishNotification if notification.success =>
              handleSuccess(
                notification,
                publicDataset,
                version,
                publishStatus,
                contributors,
                collections,
                externalPublications
              )

            case notification: ReleaseNotification if notification.success =>
              handleReleaseSuccess(
                notification,
                publicDataset,
                version,
                publishStatus
              )

            case notification: PublishNotification =>
              handleFailure(
                notification,
                publicDataset,
                version,
                publishStatus,
                s"Version ${version.version} failed to publish"
              )

            case notification: ReleaseNotification =>
              handleFailure(
                notification,
                publicDataset,
                version,
                publishStatus,
                s"Version ${version.version} failed to release"
              )
          }

        } yield MessageAction.Delete(sqsMessage))
        // Send failed messages back to queue
          .recoverWith {
            case error: Throwable =>
              ports.log.error(s"Failed to set status for $message", error)
              Future.successful(MessageAction.Ignore(sqsMessage))
          }
      case Left(error) =>
        // Send unparsable message back to queue
        ports.logger.noContext
          .error(s"Failed to decode $sqsMessage.body", error)
        Future.successful(MessageAction.Ignore(sqsMessage))
    }
  }

  private def handleSuccess(
    message: PublishNotification,
    publicDataset: PublicDataset,
    version: PublicDatasetVersion,
    publishStatus: DatasetPublishStatus,
    contributors: List[PublicContributor],
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication]
  )(implicit
    logContext: LogContext
  ): Future[Unit] =
    for {
      // Read the outputs.json file in S3
      publishResult <- ports.s3StreamClient.readPublishJobOutput(version)
      metadata <- ports.s3StreamClient
        .readDatasetMetadata(version)

      // Update the dataset version with the information in outputs.json
      updatedVersion <- ports.db.run(
        PublicDatasetVersionsMapper.setResultMetadata(
          version = version,
          size = publishResult.totalSize,
          fileCount = metadata.files.length,
          readme = publishResult.readmeKey,
          banner = publishResult.bannerKey
        )
      )
      _ <- ports.pennsieveApiClient
        .putPublishComplete(publishStatus, None)
        .value
        .flatMap(_.fold(Future.failed, Future.successful))

      _ <- publishDoi(
        publicDataset,
        updatedVersion,
        contributors,
        collections,
        externalPublications
      )

      // Store files in Postgres
      _ <- ports.db.run(PublicFilesMapper.createMany(version, metadata.files))

      // Add dataset to search index
      _ <- Search.indexDataset(publicDataset, updatedVersion, ports)

      _ <- ports.s3StreamClient.deletePublishJobOutput(updatedVersion)
    } yield ()

  private def handleFailure(
    notification: JobDoneNotification,
    publicDataset: PublicDataset,
    version: PublicDatasetVersion,
    publishStatus: DatasetPublishStatus,
    message: String
  )(implicit
    logContext: LogContext
  ): Future[Unit] = {
    notification.error.foreach(ports.log.error(message, _))

    for {
      _ <- ports.pennsieveApiClient
        .putPublishComplete(publishStatus, Some(message))
        .value
        .flatMap(_.fold(Future.failed, Future.successful))

      _ <- ports.victorOpsClient.sendAlert(version, notification)
    } yield ()
  }

  private def handleReleaseSuccess(
    message: ReleaseNotification,
    publicDataset: PublicDataset,
    version: PublicDatasetVersion,
    publishStatus: DatasetPublishStatus
  )(implicit
    logContext: LogContext
  ): Future[Unit] =
    for {
      updatedVersion <- ports.db.run(
        PublicDatasetVersionsMapper
          .setS3Bucket(version, ports.config.s3.publishBucket)
      )

      _ <- ports.pennsieveApiClient
        .putPublishComplete(publishStatus, None)
        .value
        .flatMap(_.fold(Future.failed, Future.successful))

      // Add dataset to search index
      _ <- Search.indexDataset(publicDataset, updatedVersion, ports)

    } yield ()

  private def publishDoi(
    publicDataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication]
  ): Future[DoiDTO] = {

    val token = Authenticator.generateServiceToken(
      ports.jwt,
      organizationId = publicDataset.sourceOrganizationId,
      datasetId = publicDataset.sourceDatasetId
    )
    val headers = List(Authorization(OAuth2BearerToken(token.value)))
    val publicationYear: Int = version.embargoReleaseDate
      .map(_.getYear)
      .getOrElse(Calendar.getInstance().get(Calendar.YEAR))
    ports.doiClient.publishDoi(
      doi = version.doi,
      name = publicDataset.name,
      publicationYear = publicationYear,
      contributors = contributors,
      publisher = DoiRedirect.getPublisher(publicDataset),
      url = DoiRedirect.getUrl(ports.config.publicUrl, publicDataset, version),
      owner = Some(
        InternalContributor(
          id = publicDataset.ownerId, //id is not used so the value does not matter
          firstName = publicDataset.ownerFirstName,
          lastName = publicDataset.ownerLastName,
          orcid = Some(publicDataset.ownerOrcid)
        )
      ),
      version = Some(version.version),
      description = Some(version.description),
      license = Some(publicDataset.license),
      collections = collections,
      externalPublications = externalPublications,
      headers = headers
    )
  }

  /**
    * Scan the dataset versions table for datasets that can be released to
    * Discover. If found, kick off the job via API. This is circuitous, but the
    * request needs to pass through API so that the publication log can be
    * updated accordingly.
    */
  private def releaseEmbargoedDatasets(): Future[Unit] = {
    ports.logger.noContext.info("Scanning for datasets to release from embargo")

    for {
      readyForRelease <- ports.db.run(
        PublicDatasetVersionsMapper
          .getLatestDatasetVersions(
            Seq(PublishStatus.EmbargoSucceeded, PublishStatus.ReleaseFailed)
          )
          .filter(_.embargoReleaseDate <= LocalDate.now)
          .join(PublicDatasetsMapper)
          .on(_.datasetId === _.id)
          .result
      )

      // TODO: Should this only start a single job at a time?
      _ <- readyForRelease.toList
        .traverse {
          case (version, dataset) =>
            implicit val logContext: LogContext =
              DiscoverLogContext(
                organizationId = Some(dataset.sourceOrganizationId),
                datasetId = Some(dataset.sourceDatasetId)
              )

            ports.log.info(s"Starting embargo release workflow in API")

            ports.pennsieveApiClient
              .startRelease(
                sourceOrganizationId = dataset.sourceOrganizationId,
                sourceDatasetId = dataset.sourceDatasetId
              )
              .leftSemiflatMap(
                e =>
                  ports.victorOpsClient
                    .sendFailedToStartReleaseAlert(
                      dataset,
                      version,
                      e.getMessage
                    )
                    .map(_ => e)
              )
        }
        .foldF(Future.failed(_), Future.successful(_))

    } yield ()

  }
}
