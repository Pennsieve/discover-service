// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import com.pennsieve.auth.middleware.AkkaDirective.authenticateJwt
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.utils.getOrCreateDoi
import com.pennsieve.discover.Authenticator.withServiceOwnerAuthorization
import com.pennsieve.discover.db.{
  PublicCollectionsMapper,
  PublicContributorsMapper,
  PublicDatasetReleaseAssetMapper,
  PublicDatasetReleaseMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicExternalPublicationsMapper,
  PublicFileVersionStore
}
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.logging.{
  logRequestAndResponse,
  DiscoverLogContext
}
import com.pennsieve.discover.models.{
  PennsieveSchemaVersion,
  PublicDatasetRelease,
  PublicDatasetVersion,
  PublishingWorkflow,
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
import com.pennsieve.models.PublishStatus.PublishSucceeded
import com.pennsieve.models.{ DatasetType, PublishStatus, RelationshipType }
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
    ports.log.info(s"publishRelease() starting [request]:$body")
    val bucketResolver = BucketResolver(ports)
    val (targetS3Bucket, _) =
      bucketResolver.resolveBucketConfig(
        body.bucketConfig,
        Some(PublishingWorkflow.Version5)
      )

    withServiceOwnerAuthorization[PublishResponse](
      claim,
      organizationId,
      datasetId
    ) { _ =>
      getOrCreateDoi(ports, organizationId, datasetId)
        .flatMap { doi =>
          ports.log.info(s"publishRelease() [stored] DOI: $doi")

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
            _ = ports.log.info(
              s"publishRelease() [stored] dataset: $publicDataset"
            )

            // get the latest published version
            _ <- PublicDatasetVersionsMapper
              .getLatestVisibleVersion(publicDataset)
              .flatMap {
                // if the previous publish job failed, then this will remove the record of the public
                // dataset version and release in preparation for attempting publishing (again).
                case Some(version) if version.status != PublishSucceeded =>
                  for {
                    _ <- PublicDatasetReleaseMapper.delete(
                      publicDataset.id,
                      version.version
                    )
                    _ <- PublicDatasetVersionsMapper.deleteVersion(version)
                  } yield ()
                case _ =>
                  DBIOAction.from(Future.successful(()))
              }

            version <- PublicDatasetVersionsMapper
              .create(
                id = publicDataset.id,
                status = PublishStatus.PublishInProgress,
                size = body.size,
                description = body.description,
                modelCount = Map.empty,
                fileCount = body.fileCount,
                s3Bucket = targetS3Bucket,
                embargoReleaseDate = None,
                doi = doi.doi,
                // TODO: we need to generate a new schema version and DatasetMetadata_V?
                schemaVersion = PennsieveSchemaVersion.`4.0`,
                migrated = true
              )
            _ = ports.log.info(s"publishRelease() [stored] version: $version")

            release <- PublicDatasetReleaseMapper.add(
              PublicDatasetRelease(
                datasetId = publicDataset.id,
                datasetVersion = version.version,
                origin = body.origin,
                label = body.label,
                marker = body.marker,
                repoUrl = body.repoUrl,
                labelUrl = body.labelUrl,
                markerUrl = body.markerUrl,
                releaseStatus = body.releaseStatus
              )
            )
            _ = ports.log.info(s"publishRelease() [stored] release: $release")

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
              lastPublishedDate = None,
              sponsorship = None,
              publicId = version.doi
            )

            _ = ports.log.info(
              s"publishRelease() finished [response]: $response"
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
    ports.log.info(s"finalizeRelease() starting [request]: $body")

    withServiceOwnerAuthorization[FinalizeResponse](
      claim,
      sourceOrganizationId,
      sourceDatasetId
    ) { _ =>
      val query = for {
        publicDataset <- PublicDatasetsMapper
          .getDatasetFromSourceIds(sourceOrganizationId, sourceDatasetId)
        _ = ports.log.info(
          s"finalizeRelease() [retrieved] dataset: ${publicDataset}"
        )

        version <- PublicDatasetVersionsMapper.getVersion(
          publicDataset.id,
          body.versionId
        )
        _ = ports.log.info(s"finalizeRelease() [retrieved] version: ${version}")

        updatedStatus <- PublicDatasetVersionsMapper.setStatus(
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
          s"finalizeRelease() [updated] status: ${updatedStatus}"
        )

        updatedVersion <- PublicDatasetVersionsMapper.setResultMetadata(
          version = updatedStatus,
          size = body.totalSize,
          fileCount = body.fileCount,
          banner = s3KeyFor(body.bannerKey),
          readme = s3KeyFor(body.readmeKey),
          changelog = s3KeyFor(body.changelogKey)
        )
        _ = ports.log.info(
          s"finalizeRelease() [updated] version: ${updatedVersion}"
        )

        release <- PublicDatasetReleaseMapper.get(
          publicDataset.id,
          updatedVersion.version
        )
        _ = ports.log.info(s"finalizeRelease() [retrieved] release: ${release}")

        _ <- DBIO.from(release match {
          case Some(_) => Future.successful(())
          case None => Future.failed(new Throwable("no dataset release found"))
        })

        // load the metadata (manifest.json) file from S3
        metadata <- DBIO.from(
          ports.s3StreamClient
            .loadDatasetMetadata(updatedVersion)
        )
        _ = ports.log.info(s"finalizeRelease() [loaded] metadata: ${metadata}")

        manifestFile = metadata.files
          .filter(_.path.equals("manifest.json"))
          .head
        files = manifestFile.copy(s3VersionId = Some(body.manifestVersionId)) :: metadata.files
          .filterNot(_.path.equals("manifest.json"))
        _ = ports.log.info(s"finalizeRelease() [updated] files: ${files}")

        _ <- DBIO.from(updatedVersion.version match {
          case 1 =>
            PublicFileVersionStore.publishFirstVersion(updatedVersion, files)(
              ec = executionContext,
              system = system,
              ports = ports,
              slickSession = ports.slickSession,
              logContext = logContext
            )
          case _ =>
            PublicFileVersionStore.publishNextVersion(updatedVersion, files)(
              ec = executionContext,
              system = system,
              ports = ports,
              slickSession = ports.slickSession,
              logContext = logContext
            )
        })

        // read release asset listing
        releaseAssetListing <- DBIO.from(
          ports.s3StreamClient
            .loadReleaseAssetListing(updatedVersion)
        )
        _ = ports.log.info(
          s"finalizeRelease() [loaded] assets: ${releaseAssetListing}"
        )

        // create PublicDatasetReleaseAssets
        _ <- PublicDatasetReleaseAssetMapper.createMany(
          updatedVersion,
          release.get,
          releaseAssetListing
        )

        // queue message to make DOI visible
        _ = ports.log.info("finalizeRelease() [action] queue push DOI message")
        _ <- DBIOAction.from(
          SQSMessenger.queueMessage(
            ports.config.sqs.queueUrl,
            PushDoiRequest(
              jobType = SQSNotificationType.PUSH_DOI,
              datasetId = updatedVersion.datasetId,
              version = updatedVersion.version,
              doi = updatedVersion.doi
            )
          )(executionContext, logContext, ports)
        )

        // invoke S3 Cleanup Lambda to delete publishing intermediate files
        _ = ports.log.info("finalizeRelease() [action] run S3 clean: TIDY")
        _ <- DBIOAction.from(
          ports.lambdaClient.runS3Clean(
            updatedVersion.s3Key.value,
            updatedVersion.s3Bucket.value,
            updatedVersion.s3Bucket.value,
            S3CleanupStage.Tidy,
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
          lastPublishedDate = None, // TODO: change this?
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

  def s3KeyFor(value: Option[String]): Option[S3Key.File] =
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
