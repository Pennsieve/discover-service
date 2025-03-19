// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.stream.alpakka.slick.scaladsl.{ Slick, SlickSession }
import akka.stream.scaladsl.{ Sink, Source }
import com.pennsieve.auth.middleware.AkkaDirective.authenticateJwt
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.utils.getOrCreateDoi
import com.pennsieve.discover.Authenticator.withServiceOwnerAuthorization
import com.pennsieve.discover.db.{
  PublicCollectionsMapper,
  PublicContributorsMapper,
  PublicDatasetReleaseAssetMapper,
  PublicDatasetReleaseMapper,
  PublicDatasetVersionFilesTableMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicExternalPublicationsMapper,
  PublicFileVersionsMapper
}
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.logging.{
  logRequestAndResponse,
  DiscoverLogContext
}
import com.pennsieve.discover.models.{
  PennsieveSchemaVersion,
  PublicDataset,
  PublicDatasetRelease,
  PublicDatasetVersion,
  S3Bucket,
  S3CleanupStage,
  S3Key
}
import com.pennsieve.discover.notifications.{
  PushDoiRequest,
  SQSMessenger,
  SQSNotificationType
}
import com.pennsieve.discover.{
  Config,
  DoiCreationException,
  DoiServiceException,
  DuplicateDoiException,
  ForbiddenException,
  MissingParameterException,
  Ports,
  PublishJobException,
  UnauthorizedException
}
import com.pennsieve.discover.server.release.{
  ReleaseHandler => GuardrailHandler,
  ReleaseResource => GuardrailResource
}
import com.pennsieve.discover.server.definitions
import com.pennsieve.discover.utils.BucketResolver
import com.pennsieve.doi.models.DoiDTO
import com.pennsieve.models.PublishStatus.PublishSucceeded
import com.pennsieve.models.{
  DatasetType,
  FileManifest,
  PublishStatus,
  RelationshipType
}
import com.pennsieve.service.utilities.LogContext
import io.circe.DecodingFailure
import slick.dbio.{ DBIO, DBIOAction }
import slick.jdbc.TransactionIsolation

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

class ReleaseHandler(
  ports: Ports,
  claim: Jwt.Claim
)(implicit
  system: ActorSystem,
  executionContext: ExecutionContext
) extends GuardrailHandler {
  type PublishResponse = GuardrailResource.PublishReleaseResponse
  type FinalizeResponse = GuardrailResource.FinalizeReleaseResponse

  implicit val config: Config = ports.config

  override def publishRelease(
    respond: GuardrailResource.PublishReleaseResponse.type
  )(
    organizationId: Int,
    datasetId: Int,
    body: definitions.PublishReleaseRequest
  ): Future[PublishResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(organizationId),
      datasetId = Some(datasetId),
      userId = Some(body.ownerId),
      releaseOrigin = Some(body.origin),
      releaseRepoUrl = Some(body.repoUrl),
      releaseLabel = Some(body.label)
    )
    ports.log.info(s"publishRelease() starting request: ${body}")
    val bucketResolver = BucketResolver(ports)
    val (targetS3Bucket, _) =
      bucketResolver.resolveBucketConfig(body.bucketConfig)

    withServiceOwnerAuthorization[PublishResponse](
      claim,
      organizationId,
      datasetId
    ) { _ =>
      getOrCreateDoi(ports, organizationId, datasetId)
        .flatMap { doi =>
          ports.log.info(s"publishRelease() DOI: ${doi}")

          val query = for {
            publicDataset <- PublicDatasetsMapper
              .createOrUpdate(
                name = body.name,
                sourceOrganizationId = organizationId,
                sourceOrganizationName = body.organizationName,
                sourceDatasetId = datasetId,
                ownerId = body.ownerId,
                ownerFirstName = body.ownerFirstName,
                ownerLastName = body.ownerLastName,
                ownerOrcid = body.ownerOrcid,
                license = body.license,
                tags = body.tags.toList,
                datasetType = DatasetType.Release
              )
            _ = ports.log.info(s"publishRelease() dataset: ${publicDataset}")

            (release, version) <- PublicDatasetVersionsMapper
              .getVersionAndRelease(publicDataset, body.label, body.marker)
              .flatMap {
                case Some((release, version))
                    if version.status == PublishSucceeded =>
                  val message =
                    s"release already published [dataset:${version.datasetId}, version${version.version}] ${release.repoUrl} ${release.label} ${release.marker}"
                  ports.log.error(message)
                  DBIO.failed(ForbiddenException(message))
                case Some((release, version)) =>
                  // resume an in progress or failed publication
                  ports.log.info(
                    s"publishRelease() resuming publication of version: ${version} and release: ${release}"
                  )
                  PublicDatasetVersionsMapper
                    .resumeReleasePublishing(version, release)
                case None =>
                  // create version and release
                  ports.log.info(
                    s"publishRelease() creating new version and release"
                  )
                  PublicDatasetVersionsMapper.createNewVersionAndRelease(
                    publicDataset,
                    doi.doi,
                    body.description,
                    body.fileCount,
                    body.size,
                    targetS3Bucket,
                    body.origin,
                    body.repoUrl,
                    body.label,
                    body.marker,
                    body.labelUrl,
                    body.markerUrl,
                    body.releaseStatus
                  )
              }

            _ = ports.log.info(s"publishRelease() version: ${version}")
            _ = ports.log.info(s"publishRelease() release: ${release}")

            _ = ports.log.info(
              s"publishRelease() [request] contributors: ${body.contributors}"
            )
            contributors <- DBIO.sequence(body.contributors.map { c =>
              PublicContributorsMapper
                .create(
                  firstName = c.firstName,
                  middleInitial = c.middleInitial,
                  lastName = c.lastName,
                  degree = c.degree,
                  orcid = c.orcid,
                  datasetId = publicDataset.id,
                  version = version.version,
                  sourceContributorId = c.id,
                  sourceUserId = c.userId
                )
            }.toList)
            _ = ports.log.info(
              s"publishRelease() [stored] contributors: $contributors"
            )

            _ = ports.log.info(
              s"publishRelease() [request] collections: ${body.collections}"
            )
            collections <- DBIO.sequence(
              body.collections
                .getOrElse(IndexedSeq())
                .map { c =>
                  PublicCollectionsMapper
                    .create(
                      name = c.name,
                      datasetId = publicDataset.id,
                      version = version.version,
                      sourceCollectionId = c.id
                    )
                }
                .toList
            )
            _ = ports.log.info(
              s"publishRelease() [stored] collections: $collections"
            )

            _ = ports.log.info(
              s"publishRelease() [request] externalPublications: ${body.externalPublications}"
            )
            externalPublications <- DBIO.sequence(
              body.externalPublications
                .getOrElse(IndexedSeq.empty)
                .map { p =>
                  PublicExternalPublicationsMapper.create(
                    doi = p.doi,
                    relationshipType =
                      p.relationshipType.getOrElse(RelationshipType.References),
                    datasetId = publicDataset.id,
                    version = version.version
                  )
                }
                .toList
            )
            _ = ports.log.info(
              s"publishRelease() [stored] externalPublications:: $externalPublications"
            )

            response = definitions.ReleasePublishingResponse(
              name = publicDataset.name,
              sourceOrganizationName = publicDataset.sourceOrganizationName,
              sourceOrganizationId = publicDataset.sourceOrganizationId,
              sourceDatasetId = publicDataset.sourceDatasetId,
              publishedDatasetId = version.datasetId,
              publishedVersionCount = version.version,
              status = version.status,
              lastPublishedDate = Some(version.createdAt),
              sponsorship = None,
              publicId = version.doi
            )

            _ = ports.log.info(
              s"publishRelease() finished response: ${response}"
            )

          } yield respond.Created(response)

          ports.db
            .run(
              query.transactionally
                .withTransactionIsolation(TransactionIsolation.Serializable)
            )
        }

    }.recover {
      case UnauthorizedException => respond.Unauthorized
      case DecodingFailure(msg, path) =>
        respond.InternalServerError(s"Failed to decode DOI: $msg [$path]")
      case DoiCreationException(e) =>
        respond.BadRequest(s"Failed to create a DOI for the dataset: $e")
      case DoiServiceException(e) =>
        respond.InternalServerError(
          s"Failed to communicate with DOI service: $e"
        )
      case DuplicateDoiException =>
        respond.InternalServerError(
          "A dataset version has already been published with this DOI"
        )
      case MissingParameterException(parameter) =>
        respond.BadRequest(s"Missing parameter '$parameter'")
      case ForbiddenException(e) => respond.Forbidden(e)
      case PublishJobException(e) => respond.InternalServerError(e.toString)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  override def finalizeRelease(
    respond: GuardrailResource.FinalizeReleaseResponse.type
  )(
    sourceOrganizationId: Int,
    sourceDatasetId: Int,
    body: definitions.FinalizeReleaseRequest
  ): Future[FinalizeResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(sourceOrganizationId),
      datasetId = Some(sourceDatasetId)
    )
    ports.log.info(s"finalizeRelease() starting request: ${body}")

    withServiceOwnerAuthorization[FinalizeResponse](
      claim,
      sourceOrganizationId,
      sourceDatasetId
    ) { _ =>
      val query = for {
        publicDataset <- PublicDatasetsMapper
          .getDatasetFromSourceIds(sourceOrganizationId, sourceDatasetId)
        _ = ports.log.info(s"finalizeRelease() dataset: ${publicDataset}")

        version <- PublicDatasetVersionsMapper.getVersion(
          publicDataset.id,
          body.versionId
        )
        _ = ports.log.info(s"finalizeRelease() version: ${version}")

        updatedVersion <- PublicDatasetVersionsMapper.setStatus(
          id = publicDataset.id,
          version = version.version,
          status = body.publishSuccess match {
            case true =>
              PublishStatus.PublishSucceeded
            case false =>
              PublishStatus.PublishFailed
          }
        )
        _ = ports.log.info(
          s"finalizeRelease() updated version: ${updatedVersion}"
        )

        cleanupStage <- updatedVersion.status match {
          case PublishStatus.PublishSucceeded =>
            publishSucceeded(publicDataset, updatedVersion, body)
          case _ =>
            publishFailed(publicDataset, updatedVersion, body)

        }

        // invoke S3 Cleanup Lambda to delete publishing intermediate files
        _ = ports.log.info(
          s"finalizeRelease() [action] run S3 clean: ${cleanupStage}"
        )
        _ <- DBIOAction.from(
          ports.lambdaClient.runS3Clean(
            updatedVersion.s3Key.value,
            updatedVersion.s3Bucket.value,
            updatedVersion.s3Bucket.value,
            cleanupStage,
            updatedVersion.migrated
          )
        )

        response = definitions.ReleasePublishingResponse(
          name = publicDataset.name,
          sourceOrganizationName = publicDataset.sourceOrganizationName,
          sourceOrganizationId = publicDataset.sourceOrganizationId,
          sourceDatasetId = publicDataset.sourceDatasetId,
          publishedDatasetId = updatedVersion.datasetId,
          publishedVersionCount = updatedVersion.version,
          status = updatedVersion.status,
          lastPublishedDate = Some(updatedVersion.createdAt),
          sponsorship = None,
          publicId = updatedVersion.doi
        )
        _ = ports.log.info("finalizeRelease() finished [response]: $response")

      } yield respond.OK(response)

      ports.db
        .run(
          query.transactionally
            .withTransactionIsolation(TransactionIsolation.Serializable)
        )

    }.recover {
      case UnauthorizedException => respond.Unauthorized
      case DecodingFailure(msg, path) =>
        respond.InternalServerError(s"Failed to decode DOI: $msg [$path]")
      case MissingParameterException(parameter) =>
        respond.BadRequest(s"Missing parameter '$parameter'")
      case ForbiddenException(e) => respond.Forbidden(e)
      case PublishJobException(e) => respond.InternalServerError(e.toString)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  private def publishSucceeded(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    body: definitions.FinalizeReleaseRequest
  )(implicit
    logContext: DiscoverLogContext
  ): DBIOAction[
    String,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] =
    for {
      finalizedVersion <- PublicDatasetVersionsMapper.setResultMetadata(
        version = version,
        size = body.totalSize,
        fileCount = body.fileCount,
        banner = s3KeyFor(body.bannerKey),
        readme = s3KeyFor(body.readmeKey),
        changelog = s3KeyFor(body.changelogKey)
      )
      _ = ports.log.info(
        s"publishSucceeded() finalized version: ${finalizedVersion}"
      )

      release <- PublicDatasetReleaseMapper.get(dataset.id, version.version)
      _ = ports.log.info(s"publishSucceeded() release: ${release}")

      _ <- DBIO.from(release match {
        case Some(_) => Future.successful(())
        case None => Future.failed(new Throwable("no dataset release found"))
      })

      // load the metadata (manifest.json) file from S3
      metadata <- DBIO.from(
        ports.s3StreamClient
          .loadDatasetMetadata(finalizedVersion)
      )
      _ = ports.log.info(s"publishSucceeded() metadata: ${metadata}")

      _ <- DBIO.from(metadata match {
        case Some(_) => Future.successful(())
        case None =>
          Future.failed(new Throwable("dataset metadata not found"))
      })

      manifestFiles = metadata.get.files
      manifestFile = manifestFiles
        .filter(_.path.equals("manifest.json"))
        .head
      files = manifestFile.copy(s3VersionId = Some(body.manifestVersionId)) :: manifestFiles
        .filterNot(_.path.equals("manifest.json"))
      _ = ports.log.info(s"publishSucceeded() updated manifestFiles: ${files}")

      publicFileVersions <- DBIO.sequence(files.map { file =>
        PublicFileVersionsMapper.createOne(finalizedVersion, file)
      })
      _ = ports.log.info(
        s"publishSucceeded() publicFileVersions: ${publicFileVersions}"
      )

      publicFileVersionLinks <- DBIO.sequence(
        publicFileVersions
          .map { pfv =>
            PublicDatasetVersionFilesTableMapper
              .storeLink(finalizedVersion, pfv)
          }
      )
      _ = ports.log.info(
        s"publishSucceeded() publicFileVersionLinks: ${publicFileVersionLinks}"
      )

      // read release asset listing
      releaseAssetListing <- DBIO.from(
        ports.s3StreamClient
          .loadReleaseAssetListing(finalizedVersion)
      )

      _ = ports.log.info(
        s"publishSucceeded() releaseAssetListing: ${releaseAssetListing}"
      )

      _ <- releaseAssetListing match {
        case Some(releaseAssetListing) =>
          ports.log.info(
            s"publishSucceeded() storing ${releaseAssetListing.files.length} release asset items"
          )
          PublicDatasetReleaseAssetMapper.createMany(
            finalizedVersion,
            release.get,
            releaseAssetListing
          )
        case None =>
          ports.log.warn(
            s"publishSucceeded() release asset listing was not loaded"
          )
          DBIO.successful(akka.Done)
      }

      // queue message to make DOI visible
      _ = ports.log.info("publishSucceeded() [action] queue push DOI message")
      _ <- DBIOAction.from(
        SQSMessenger.queueMessage(
          ports.config.sqs.queueUrl,
          PushDoiRequest(
            jobType = SQSNotificationType.PUSH_DOI,
            datasetId = finalizedVersion.datasetId,
            version = finalizedVersion.version,
            doi = finalizedVersion.doi
          )
        )(executionContext, logContext, ports)
      )

    } yield (S3CleanupStage.Tidy)

  private def publishFailed(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    body: definitions.FinalizeReleaseRequest
  )(implicit
    logContext: DiscoverLogContext
  ): DBIOAction[String, NoStream, Effect] = {
    ports.log.warn(
      s"publishFailed() datasetId: ${dataset.id} version: ${version.version}"
    )
    DBIO.from(Future.successful(S3CleanupStage.Failure))
  }

  private def s3KeyFor(value: Option[String]): Option[S3Key.File] =
    value match {
      case Some(value) => Some(S3Key.File(value))
      case None => None
    }
}

object ReleaseHandler {
  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route = {
    logRequestAndResponse(ports) {
      authenticateJwt(system.name)(ports.jwt) { claim =>
        GuardrailResource.routes(new ReleaseHandler(ports, claim))
      }
    }
  }
}
