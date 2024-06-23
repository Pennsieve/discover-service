// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import java.util.Calendar
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.alpakka.sqs.scaladsl.{ SqsAckSink, SqsSource }
import akka.stream.scaladsl.{ Flow, Keep, RunnableGraph, Sink, Source }
import akka.stream._
import akka.stream.alpakka.slick.scaladsl.{ Slick, SlickSession }
import cats.data._
import cats.implicits._
import com.github.tminglei.slickpg.LTree
import com.pennsieve.discover.models.DoiRedirect
import com.pennsieve.discover.db.{
  profile,
  PublicCollectionsMapper,
  PublicContributorsMapper,
  PublicDatasetVersionFilesTableMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicExternalPublicationsMapper,
  PublicFileVersionsMapper,
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
import com.pennsieve.discover.utils.runSequentially
import com.pennsieve.discover.{ Authenticator, Ports, UnauthorizedException }
import com.pennsieve.doi.client.definitions.PublishDoiRequest
import com.pennsieve.doi.models.{ DoiDTO, DoiState }
import com.pennsieve.models.{ DatasetMetadata, FileManifest, PublishStatus }
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
  system: ActorSystem
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

      case Right(message: IndexDatasetRequest) =>
        implicit val logContext: LogContext =
          DiscoverLogContext(
            publicDatasetId = Some(message.datasetId),
            publicDatasetVersion = Some(message.version)
          )
        ports.log.info(s"IndexDatasetRequest ${message}")

        val query = for {
          dataset <- PublicDatasetsMapper.getDataset(message.datasetId)

          version <- PublicDatasetVersionsMapper.getVersion(
            dataset.id,
            message.version
          )
        } yield (dataset, version)

        for {
          (dataset, version) <- ports.db.run(query)

          // Add dataset to search index
          _ <- Search.indexDataset(dataset, version, ports, overwrite = true)
        } yield MessageAction.Delete(sqsMessage)

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
            status = (version.underEmbargo, message, message.success) match {
              case (false, m: PublishNotification, true) =>
                PublishStatus.PublishSucceeded

              case (false, m: PublishNotification, false) =>
                PublishStatus.PublishFailed

              case (true, m: PublishNotification, true) =>
                PublishStatus.EmbargoSucceeded

              case (true, m: PublishNotification, false) =>
                PublishStatus.EmbargoFailed

              case (_, m: ReleaseNotification, true) =>
                PublishStatus.PublishSucceeded

              case (_, m: ReleaseNotification, false) =>
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
      publishResult <- ports.s3StreamClient
        .readPublishJobOutput(version)
      metadata <- ports.s3StreamClient
        .readDatasetMetadata(version)

      _ = {
        ports.log.info(
          s"handleSuccess() dataset: ${version.datasetId} version: ${version.version}"
        )
        ports.log.info(
          s"handleSuccess() publishResult: ${publishResult} (${metadata.files.length} files)"
        )
      }

      _ = println(
        s"handleSuccess() dataset: ${version.datasetId} version: ${version.version}"
      )
      _ = println(
        s"handleSuccess() publishResult: ${publishResult} (${metadata.files.length} files)"
      )

      // Update the dataset version with the information in outputs.json
      _ = println(s"handleSuccess() updating result metadata")
      updatedVersion <- ports.db.run(
        PublicDatasetVersionsMapper.setResultMetadata(
          version = version,
          size = publishResult.totalSize,
          fileCount = metadata.files.length,
          readme = publishResult.readmeKey,
          banner = publishResult.bannerKey,
          changelog = publishResult.changelogKey
        )
      )
      _ = println(s"handleSuccess() updated result metadata")

      _ = println(s"handleSuccess() notify API")
      _ = ports.log.info("handleSuccess() notify API")
      _ <- ports.pennsieveApiClient
        .putPublishComplete(publishStatus, None)
        .value
        .flatMap(_.fold(Future.failed, Future.successful))
      _ = println(s"handleSuccess() notified API")

      _ = println(s"handleSuccess() publish DOI")
      _ = ports.log.info("handleSuccess() publish DOI")
      _ <- publishDoi(
        publicDataset,
        updatedVersion,
        contributors,
        collections,
        externalPublications
      )
      _ = println(s"handleSuccess() published DOI")

      // Store files in Postgres
      _ = println(s"handleSuccess() store files")
      _ = ports.log.info("handleSuccess() store files")
      _ <- updatedVersion.migrated match {
        case true =>
          // Publishing 5x
          println(s"handleSuccess() storing files: Publishing 5x")
          val manifestFile =
            metadata.files.filter(_.path.equals("manifest.json")).head
          val files = manifestFile.copy(
            s3VersionId = publishResult.manifestVersion
          ) :: metadata.files.filterNot(_.path.equals("manifest.json"))

          updatedVersion.version match {
            case 1 =>
              println(
                s"handleSuccess() storing files: Publishing 5x - first publication"
              )
              publishFirstVersion(updatedVersion, files)
            case _ =>
              println(
                s"handleSuccess() storing files: Publishing 5x - subsequent publication"
              )
              publishNextVersionV3(updatedVersion, files)
          }
        case false =>
          // Publishing 4x
          println(s"handleSuccess() storing files: Publishing 5x")
          ports.db.run(PublicFilesMapper.createMany(version, metadata.files))
      }
      _ = println(s"handleSuccess() stored files")

      // Add dataset to search index
      _ = println(s"handleSuccess() index dataset")
      _ = ports.log.info("handleSuccess() index dataset")
      _ <- Search.indexDataset(publicDataset, updatedVersion, ports)
      _ = println(s"handleSuccess() indexed dataset")

      // invoke S3 Cleanup Lambda to delete publishing intermediate files
      _ = println(s"handleSuccess() run S3 clean: TIDY")
      _ = ports.log.info("handleSuccess() run S3 clean: TIDY")
      _ <- ports.lambdaClient.runS3Clean(
        updatedVersion.s3Key.value,
        updatedVersion.s3Bucket.value,
        updatedVersion.s3Bucket.value,
        S3CleanupStage.Tidy,
        updatedVersion.migrated
      )
      _ = println(s"handleSuccess() done")
    } yield ()

  private def publishFirstVersion(
    version: PublicDatasetVersion,
    files: List[FileManifest]
  ): Future[Unit] = {

    implicit val slickSessionCreatedForDbAndProfile: SlickSession =
      SlickSession.forDbAndProfile(ports.db, profile)

    val queryFindAll = for {
      allFileVersions <- PublicFileVersionsMapper.getAll(version.datasetId)
    } yield (allFileVersions)

    for {
      _ <- Future.successful(
        println(
          s"publishFirstVersion() dataset ${version.datasetId} version ${version.version} (${files.length} files)"
        )
      )

//      publicDataset <- ports.db.run(
//        PublicDatasetsMapper.getDataset(version.datasetId)
//      )
//      _ = println(s"publishFirstVersion() publicDataset: ${publicDataset}")
//
//      publicDatasetVersion <- ports.db.run(
//        PublicDatasetVersionsMapper
//          .getVersion(version.datasetId, version.version)
//      )
//      _ = println(
//        s"publishFirstVersion() publicDatasetVersion: ${publicDatasetVersion}"
//      )

      _ = println(s"publishFirstVersion() creating many file versions")
      //_ <- ports.db.run(PublicFileVersionsMapper.createMany(version, files))

//      allFileVersions <- Source(files)
//        .via(
//          Slick.flowWithPassThrough(
//            parallelism = 4,
//            file => PublicFileVersionsMapper.createOne(version, file)
//          )
//        )
//        .runWith(Sink.seq)
//      _ = println(
//        s"publishFirstVersion() created ${allFileVersions.length} file versions"
//      )

      // _ = println(s"publishFirstVersion() finding all file versions")
      // allFileVersions <- ports.db.run(queryFindAll)
      // fileIds = allFileVersions.map(_.id).sorted
      // _ = println(s"publishFirstVersion() fileIds: ${fileIds}")
//      _ = println(s"publishFirstVersion() storing links")
//      _ <- ports.db.run(
//        PublicDatasetVersionFilesTableMapper
//          .storeLinks(version, allFileVersions)
//      )

//      allFileVersionLinks <- Source(allFileVersions)
//        .via(
//          Slick.flowWithPassThrough(
//            parallelism = 4,
//            file =>
//              PublicDatasetVersionFilesTableMapper
//                .storeLink(version, file)
//          )
//        )
//        .runWith(Sink.seq)
//      _ = println(
//        s"publishFirstVersion() stored ${allFileVersionLinks.length} links"
//      )

      fileVersionLinks <- Source(files)
        .via(
          Slick.flowWithPassThrough(
            parallelism = 4,
            file => PublicFileVersionsMapper.createOne(version, file)
          )
        )
        .via(
          Slick.flowWithPassThrough(
            parallelism = 4,
            pfv =>
              PublicDatasetVersionFilesTableMapper
                .storeLink(version, pfv)
          )
        )
        .runWith(Sink.seq)
      _ = println(
        s"publishFirstVersion() created ${fileVersionLinks.length} file versions and links"
      )

    } yield ()
  }

  private def publishNextVersion(
    version: PublicDatasetVersion,
    files: List[FileManifest]
  ): Future[Unit] =
    for {
      _ <- Future.successful(
        println(
          s"publishNextVersionV() dataset ${version.datasetId} version ${version.version} (${files.length} files"
        )
      )
      _ = println(s"publishNextVersionV() find or create")
      fileVersions <- Future.sequence(
        files.map(
          file =>
            ports.db.run(PublicFileVersionsMapper.findOrCreate(version, file))
        )
      )
      _ = println(s"publishNextVersionV() store links")
      _ <- ports.db.run(
        PublicDatasetVersionFilesTableMapper.storeLinks(version, fileVersions)
      )
    } yield ()

  private def publishNextVersionV2(
    version: PublicDatasetVersion,
    files: List[FileManifest]
  ): Future[Unit] = {

    def lookup(
      file: FileManifest
    )(implicit
      version: PublicDatasetVersion
    ): Future[PublicFileVersion] =
      ports.db.run(PublicFileVersionsMapper.findOrCreate(version, file))
    implicit val ver = version
    for {
      _ <- Future.successful(
        println(
          s"publishNextVersionV2() dataset ${version.datasetId} version ${version.version} (${files.length} files"
        )
      )
      _ = println(s"publishNextVersionV2() lookup: find or create")
      fileVersions <- runSequentially(files)(lookup)
      _ = println(s"publishNextVersionV2() store links")
      _ <- ports.db.run(
        PublicDatasetVersionFilesTableMapper.storeLinks(version, fileVersions)
      )
    } yield ()
  }

  private def publishNextVersionV3(
    version: PublicDatasetVersion,
    files: List[FileManifest]
  ): Future[Unit] = {
    val pfvs = files.map(
      file =>
        PublicFileVersion(
          name = file.name,
          fileType = file.fileType.toString,
          size = file.size,
          sourcePackageId = file.sourcePackageId,
          sourceFileUUID = None,
          s3Key = version.s3Key / file.path,
          s3Version = file.s3VersionId.getOrElse("missing"),
          path = LTree(
            PublicFileVersionsMapper
              .convertPathToTree(version.s3Key / file.path)
          ),
          datasetId = version.datasetId,
          sha256 = file.sha256
        )
    )

//    val insertIfNotExists: Flow[PublicFileVersion, PublicFileVersion, NotUsed] =
//      Flow[PublicFileVersion].map(
//        pfv => ports.db.run(PublicFileVersionsMapper.insert(pfv))
//      )

//    for {
//      _ <- Source(pfvs)
//        .mapAsync(1)(pfv => {
//          ports.db.run(PublicFileVersionsMapper.insert(pfv))
//        })
//        .runWith(Sink.seq)
//
//    } yield ()

    implicit val slickSessionCreatedForDbAndProfile: SlickSession =
      SlickSession.forDbAndProfile(ports.db, profile)

    for {
//      fileIds <- Source(pfvs)
//        .via(
//          Slick
//            .flow(parallelism = 4, pfv => PublicFileVersionsMapper.insert(pfv))
//        )
//        .runWith(Sink.seq)
//      //_ = println(s"publishNextVersionV3() fileIds: ${fileIds.toList.sorted}")
//      // TODO: link the fileIds to the dataset version
      fileVersionLinks <- Source(files)
        .via(
          Slick.flowWithPassThrough(
            parallelism = 4,
            file => PublicFileVersionsMapper.findOrCreate(version, file)
          )
        )
        .via(
          Slick.flowWithPassThrough(
            parallelism = 4,
            pfv =>
              PublicDatasetVersionFilesTableMapper
                .storeLink(version, pfv)
          )
        )
        .runWith(Sink.seq)
      _ = println(
        s"publishNextVersionV3() stored and linked ${fileVersionLinks.length}"
      )

    } yield ()
  }

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

    } yield ()
  }

  private def handleReleaseSuccess(
    message: ReleaseNotification,
    publicDataset: PublicDataset,
    version: PublicDatasetVersion,
    publishStatus: DatasetPublishStatus
  )(implicit
    system: ActorSystem,
    logContext: LogContext
  ): Future[Unit] =
    for {
      updatedVersion <- ports.db.run(
        PublicDatasetVersionsMapper
          .setS3Bucket(version, message.publishBucket)
      )

      // if this is a Publishing 5.0 dataset, then update the S3 Version of the Files
      _ <- updatedVersion.migrated match {
        case true =>
          releaseUpdateFileVersions(version)
        case false =>
          Future.successful(())
      }

      // TODO: if migrated, then delete discover-release-results.json

      _ <- ports.pennsieveApiClient
        .putPublishComplete(publishStatus, None)
        .value
        .flatMap(_.fold(Future.failed, Future.successful))

      // Add dataset to search index
      _ <- Search.indexDataset(publicDataset, updatedVersion, ports)

    } yield ()

  private def releaseUpdateFileVersions(
    version: PublicDatasetVersion
  )(implicit
    system: ActorSystem,
    logContext: LogContext
  ): Future[Unit] = {
    for {
      releaseResult <- ports.s3StreamClient.readReleaseResult(version)
      _ <- ports.db.run(
        PublicFileVersionsMapper.updateManyS3Versions(version, releaseResult)
      )

    } yield ()
  }

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
        }
        .foldF(Future.failed(_), Future.successful(_))

    } yield ()

  }
}
