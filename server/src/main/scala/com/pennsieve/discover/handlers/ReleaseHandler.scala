// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.utils.getOrCreateDoi
import com.pennsieve.discover.Authenticator.withServiceOwnerAuthorization
import com.pennsieve.discover.db.{
  PublicCollectionsMapper,
  PublicContributorsMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicExternalPublicationsMapper
}
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.logging.DiscoverLogContext
import com.pennsieve.discover.models.{ PennsieveSchemaVersion }
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
import com.pennsieve.models.{ PublishStatus, RelationshipType }
import io.circe.DecodingFailure
import slick.dbio.DBIO
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
  type PublishResponse = GuardrailResource.PublishResponse

  implicit val config: Config = ports.config

  override def publish(
    respond: GuardrailResource.PublishResponse.type
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
    ports.log.info("publish release starting")
    val bucketResolver = BucketResolver(ports)
    val (targetS3Bucket, _) =
      bucketResolver.resolveBucketConfig(body.bucketConfig, body.workflowId)

    withServiceOwnerAuthorization[PublishResponse](
      claim,
      organizationId,
      datasetId
    ) { _ =>
      getOrCreateDoi(ports, organizationId, datasetId)
        .flatMap { doi =>
          ports.log.info(s"DOI: $doi")

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
                tags = body.tags.toList
              )
            _ = ports.log.info(s"Public dataset: $publicDataset")

            version <- PublicDatasetVersionsMapper
              .create(
                id = publicDataset.id,
                status = PublishStatus.PublishInProgress,
                size = body.size,
                description = body.description,
                modelCount =
                  body.modelCount.map(o => o.modelName -> o.count).toMap,
                fileCount = body.fileCount,
                recordCount = body.recordCount,
                s3Bucket = targetS3Bucket,
                embargoReleaseDate = None,
                doi = doi.doi,
                schemaVersion = PennsieveSchemaVersion.`4.0`,
                migrated = true
              )
            _ = ports.log.info(s"Public dataset version : $version")

            // TODO: add PublicDatasetRelease

            _ = ports.log.info(s"Internal Contributors : ${body.contributors}")
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
            _ = ports.log.info(s"Public dataset contributors: $contributors")

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
            _ = ports.log.info(s"Public dataset collections: $collections")

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
              s"Public external publications: $externalPublications"
            )

            status <- PublicDatasetVersionsMapper.getDatasetStatus(
              publicDataset
            )

          } yield respond.Created(status)

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

}
