// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.model.{ HttpHeader, HttpResponse, Uri }
import akka.http.scaladsl.server.Route
import cats.implicits._
import com.pennsieve.auth.middleware.AkkaDirective.authenticateJwt
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.{
  withAuthorization,
  withOrganizationAccess,
  withServiceOwnerAuthorization
}
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.db._
import com.pennsieve.discover.logging.{
  logRequestAndResponse,
  DiscoverLogContext
}
import com.pennsieve.discover.models._
import com.pennsieve.discover.server.definitions
import com.pennsieve.discover.server.publish.{
  PublishHandler => GuardrailHandler,
  PublishResource => GuardrailResource
}
import com.pennsieve.discover._
import com.pennsieve.discover.search.Search
import com.pennsieve.doi.models.{ DoiDTO, DoiState }
import com.pennsieve.models.{
  FileManifest,
  License,
  PublishStatus,
  RelationshipType
}
import com.pennsieve.models.PublishStatus.Unpublished
import io.circe.{ DecodingFailure, Json }
import slick.dbio.DBIOAction
import slick.jdbc.TransactionIsolation
import software.amazon.awssdk.services.lambda.model.InvokeResponse
import com.pennsieve.discover.server.definitions.{
  BucketConfig,
  InternalContributor,
  SponsorshipRequest,
  SponsorshipResponse
}

import java.time.LocalDate
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import com.pennsieve.discover.db.PublicFilesMapper

class PublishHandler(
  ports: Ports,
  claim: Jwt.Claim
)(implicit
  system: ActorSystem,
  executionContext: ExecutionContext
) extends GuardrailHandler {

  implicit val config: Config = ports.config

  val defaultPublishBucket = ports.config.s3.publishBucket
  val defaultEmbargoBucket = ports.config.s3.embargoBucket

  type PublishResponse = GuardrailResource.PublishResponse

  private def resolveBucketConfig(
    bucketConfig: Option[BucketConfig]
  ): (S3Bucket, S3Bucket) = {
    (
      bucketConfig
        .map(c => S3Bucket(c.publish))
        .getOrElse(defaultPublishBucket),
      bucketConfig.map(c => S3Bucket(c.embargo)).getOrElse(defaultEmbargoBucket)
    )
  }

  override def publish(
    respond: GuardrailResource.PublishResponse.type
  )(
    organizationId: Int,
    datasetId: Int,
    embargo: Option[Boolean],
    embargoReleaseDate: Option[LocalDate],
    body: definitions.PublishRequest
  ): Future[PublishResponse] = {

    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(organizationId),
      datasetId = Some(datasetId),
      userId = Some(body.ownerId)
    )

    val shouldEmbargo = embargo.getOrElse(false)

    val (publishBucket, embargoBucket) = resolveBucketConfig(body.bucketConfig)

    val targetS3Bucket = if (shouldEmbargo) embargoBucket else publishBucket

    withServiceOwnerAuthorization[PublishResponse](
      claim,
      organizationId,
      datasetId
    ) { _ =>
      getOrCreateDoi(organizationId, datasetId)
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

            _ <- if (shouldEmbargo && embargoReleaseDate.isEmpty)
              DBIO.failed(MissingParameterException("embargoReleaseDate"))
            else
              DBIO.successful(())

            latest <- PublicDatasetVersionsMapper
              .getLatestVisibleVersion(publicDataset)
              .flatMap {
                case Some(version) =>
                  if (shouldEmbargo && !version.underEmbargo && version.status != Unpublished) {
                    DBIO.failed(
                      ForbiddenException(
                        s"Cannot embargo a dataset after successful publication. Found published version ${version.version}"
                      )
                    )
                  } else {
                    DBIO.successful(Some(version))
                  }
                case _ => DBIO.successful(None)
              }

            // If the previous publish job failed, or the previous version was embargoed, roll the dataset back to the
            // previous version
            _ <- PublicDatasetVersionsMapper.rollbackIfNeeded(
              publicDataset, { latestVersion: PublicDatasetVersion =>
                {
                  if (latestVersion.underEmbargo) {
                    ports.searchClient
                      .deleteDataset(publicDataset.id)
                      .map(_ => ())
                  } else {
                    Future.successful(())
                  }
                }
              }
            )

            requestedWorkflow = body.workflowId.getOrElse(
              PublishingWorkflow.Version4
            )

            // ensure we use a compatible workflow with the previous published version
            workflowVersion = latest match {
              case Some(version) if version.migrated =>
                PublishingWorkflow.Version5
              case Some(version) if !version.migrated =>
                PublishingWorkflow.Version4
              case _ => requestedWorkflow
            }

            version <- PublicDatasetVersionsMapper
              .create(
                id = publicDataset.id,
                status =
                  if (shouldEmbargo) PublishStatus.EmbargoInProgress
                  else PublishStatus.PublishInProgress,
                size = body.size,
                description = body.description,
                modelCount =
                  body.modelCount.map(o => o.modelName -> o.count).toMap,
                fileCount = body.fileCount,
                recordCount = body.recordCount,
                s3Bucket = targetS3Bucket,
                embargoReleaseDate = embargoReleaseDate,
                doi = doi.doi,
                schemaVersion = PennsieveSchemaVersion.`4.0`,
                migrated = workflowVersion == PublishingWorkflow.Version5
              )
            _ = ports.log.info(s"Public dataset version : $version")

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

            // Wrap the AWS call in a DBIOAction so that a failure will rollback
            // the database transaction
            sfnResponse <- DBIOAction.from(
              ports.stepFunctionsClient
                .startPublish(
                  PublishJob(
                    publicDataset,
                    version,
                    body,
                    doi,
                    contributors,
                    collections,
                    externalPublications,
                    publishBucket,
                    embargoBucket,
                    workflowId = workflowVersion
                  )
                )
            )

            // Store the Step Function ARN so that we can track down failed publish jobs
            _ <- PublicDatasetVersionsMapper.setExecutionArn(
              version,
              sfnResponse.executionArn()
            )

            _ = ports.log.info(s"Started step function ${sfnResponse.toString}")

            status <- PublicDatasetVersionsMapper.getDatasetStatus(
              publicDataset
            )

          } yield respond.Created(status)

          // Slick does not provide an easy way to do insertOrUpdate for non-primary
          // keys without using Serializable transactions. The performance costs
          // of Serializable should not be a problem since publishing is an infrequent
          // event and the table is tiny.
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

  type ReviseResponse = GuardrailResource.ReviseResponse

  override def revise(
    respond: GuardrailResource.ReviseResponse.type
  )(
    organizationId: Int,
    datasetId: Int,
    body: definitions.ReviseRequest
  ): Future[ReviseResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(organizationId),
      datasetId = Some(datasetId)
    )

    withServiceOwnerAuthorization[ReviseResponse](
      claim,
      organizationId,
      datasetId
    ) { _ =>
      val query = for {
        dataset <- PublicDatasetsMapper.getDatasetFromSourceIds(
          organizationId,
          datasetId
        )
        version <- PublicDatasetVersionsMapper
          .getLatestVersion(dataset.id)
          .flatMap {
            case None =>
              DBIO.failed(NoDatasetException(dataset.id))

            // Only allow revisions of successfully published versions.
            case Some(version)
                if (version.status != PublishStatus.PublishSucceeded) =>
              DBIO.failed(
                ForbiddenException(
                  s"Cannot revise dataset: last version has status ${version.status}. Expected ${PublishStatus.PublishSucceeded}"
                )
              )

            // Embargoed datasets cannot have revisions.
            case Some(version) if (version.underEmbargo) =>
              DBIO.failed(
                ForbiddenException(
                  s"Cannot revise dataset: dataset is under embargo. Please overwrite the embargoed dataset instead."
                )
              )

            case Some(version) => DBIO.successful(version)
          }

        bannerPresignedUrl <- DBIO.from(
          Either
            .catchNonFatal(Uri(body.bannerPresignedUrl))
            .fold(Future.failed(_), Future.successful(_))
        )
        readmePresignedUrl <- DBIO.from(
          Either
            .catchNonFatal(Uri(body.readmePresignedUrl))
            .fold(Future.failed(_), Future.successful(_))
        )

        revisedDataset <- PublicDatasetsMapper.updateDataset(
          dataset,
          name = body.name,
          ownerId = body.ownerId,
          ownerFirstName = body.ownerFirstName,
          ownerLastName = body.ownerLastName,
          ownerOrcid = body.ownerOrcid,
          license = body.license,
          tags = body.tags.toList
        )

        revisedVersion <- PublicDatasetVersionsMapper.updateVersion(
          version,
          description = body.description
        )

        revision <- RevisionsMapper.create(revisedVersion)

        _ <- PublicContributorsMapper.deleteContributorsByDatasetAndVersion(
          dataset,
          version
        )
        // TODO batch this
        contributors <- DBIO.sequence(body.contributors.map { c =>
          PublicContributorsMapper
            .create(
              firstName = c.firstName,
              middleInitial = c.middleInitial,
              lastName = c.lastName,
              degree = c.degree,
              orcid = c.orcid,
              datasetId = dataset.id,
              version = version.version,
              sourceContributorId = c.id,
              sourceUserId = c.userId
            )
        }.toList)

        _ <- PublicCollectionsMapper.deleteCollectionsByDatasetAndVersion(
          dataset,
          version
        )

        collections <- DBIO.sequence(
          body.collections
            .getOrElse(IndexedSeq())
            .map { c =>
              PublicCollectionsMapper
                .create(
                  name = c.name,
                  datasetId = revisedDataset.id,
                  version = version.version,
                  sourceCollectionId = c.id
                )
            }
            .toList
        )

        _ <- PublicExternalPublicationsMapper.deleteByDatasetAndVersion(
          dataset,
          version
        )

        externalPublications <- DBIO.sequence(
          body.externalPublications
            .getOrElse(IndexedSeq.empty)
            .map { p =>
              PublicExternalPublicationsMapper.create(
                doi = p.doi,
                relationshipType =
                  p.relationshipType.getOrElse(RelationshipType.References),
                datasetId = revisedDataset.id,
                version = version.version
              )
            }
            .toList
        )

        token = Authenticator.generateServiceToken(
          ports.jwt,
          organizationId = organizationId,
          datasetId = datasetId
        )
        headers = List(Authorization(OAuth2BearerToken(token.value)))

        _ <- DBIO.from(
          ports.doiClient
            .reviseDoi(
              doi = version.doi,
              name = body.name,
              contributors = contributors,
              owner = Some(
                InternalContributor(
                  id = body.ownerId,
                  firstName = body.ownerFirstName,
                  lastName = body.ownerLastName,
                  orcid = Some(body.ownerOrcid)
                )
              ),
              version = Some(revisedVersion.version),
              description = Some(revisedVersion.description),
              license = Some(revisedDataset.license),
              collections = collections,
              externalPublications = externalPublications,
              headers
            )
        )

        _ = ports.log.debug(
          s"writing dataset revision metadata for ${revisedDataset.id}"
        )
        newFiles <- DBIO.from(
          ports.s3StreamClient.writeDatasetRevisionMetadata(
            revisedDataset,
            revisedVersion,
            contributors,
            revision,
            collections,
            externalPublications,
            bannerPresignedUrl = bannerPresignedUrl,
            readmePresignedUrl = readmePresignedUrl
          )
        )

        _ <- revisedVersion.migrated match {
          case true =>
            PublicFileVersionsMapper.createAndLinkMany(
              revisedVersion,
              newFiles.asList
            )
          case false =>
            PublicFilesMapper.createMany(revisedVersion, newFiles.asList)
        }

        _ <- PublicDatasetVersionsMapper.setResultMetadata(
          version = revisedVersion,
          size = revisedVersion.size + newFiles.asList.map(_.size).sum,
          fileCount = revisedVersion.fileCount + newFiles.asList.length,
          readme = revisedVersion.s3Key / newFiles.readme.path,
          banner = revisedVersion.s3Key / newFiles.banner.path
        )
        sponsorship <- SponsorshipsMapper.maybeGetByDataset(dataset)

        _ = ports.log.debug(
          s"updating Elasticsearch for revision of dataset ${revisedDataset.id}"
        )
        // Update ElasticSearch
        _ <- DBIO.from(for {
          readme <- ports.s3StreamClient
            .readDatasetReadme(version, Some(revision))
          _ <- ports.searchClient.indexRevision(
            revisedDataset,
            revisedVersion,
            contributors,
            revision,
            collections,
            externalPublications,
            newFiles.asList,
            readme,
            sponsorship
          )
        } yield ())

        status <- PublicDatasetVersionsMapper.getDatasetStatus(dataset)

      } yield status

      ports.db
        .run(
          query.transactionally
            .withTransactionIsolation(TransactionIsolation.Serializable)
        )
        .map(respond.Created)

    }.recover {
      case UnauthorizedException => respond.Unauthorized
      case DecodingFailure(msg, path) =>
        respond.InternalServerError(s"Failed to decode DOI: $msg [$path]")
      case DoiServiceException(e) =>
        respond.InternalServerError(
          s"Failed to communicate with DOI service: $e"
        )
      case ForbiddenException(e) => respond.Forbidden(e)
      case PublishJobException(e) => respond.InternalServerError(e.toString)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  override def release(
    respond: GuardrailResource.ReleaseResponse.type
  )(
    organizationId: Int,
    datasetId: Int,
    body: definitions.ReleaseRequest
  ): Future[GuardrailResource.ReleaseResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(organizationId),
      datasetId = Some(datasetId)
    )
    withServiceOwnerAuthorization[GuardrailResource.ReleaseResponse](
      claim,
      organizationId,
      datasetId
    ) { _ =>
      val (publishBucket, _) =
        resolveBucketConfig(body.bucketConfig)
      // Ignoring embargo bucket in bucketConfig because it may not be where the dataset was embargoed.
      // eg., It may have been embargoed before custom buckets were configured for the organization.
      // The correct embargo bucket will be in the most current PublicDatasetVersion obtained below.

      val query = for {
        dataset <- PublicDatasetsMapper.getDatasetFromSourceIds(
          organizationId,
          datasetId
        )

        version <- PublicDatasetVersionsMapper
          .getLatestVersion(dataset.id)
          .flatMap {
            case Some(version)
                if version.status in Seq(
                  PublishStatus.EmbargoSucceeded,
                  PublishStatus.ReleaseFailed
                ) =>
              DBIO.successful(version)
            case Some(version) =>
              DBIO.failed(
                ForbiddenException("Can only release embargoed datasets")
              )
            case _ => DBIO.failed(NoDatasetException(dataset.id))
          }

        _ = ports.log.info(
          s"Releasing dataset ${dataset.id} version ${version.version}"
        )

        _ <- PublicDatasetVersionsMapper.setStatus(
          dataset.id,
          version.version,
          PublishStatus.ReleaseInProgress
        )

        sfnResponse <- DBIO.from(
          ports.stepFunctionsClient
            .startRelease(
              EmbargoReleaseJob(
                dataset,
                version,
                publishBucket = publishBucket,
                embargoBucket = version.s3Bucket
              )
            )
        )

        _ = ports.log.info(s"Started step function ${sfnResponse.toString}")

        // Store the Step Function ARN so that we can track down failed publish jobs
        _ <- PublicDatasetVersionsMapper.setReleaseExecutionArn(
          version,
          sfnResponse.executionArn()
        )

        // TODO: update DOI?

        status <- PublicDatasetVersionsMapper.getDatasetStatus(dataset)
      } yield status

      ports.db.run(query.transactionally).map(respond.Accepted)
    }.recover {
      case NoDatasetForSourcesException(_, _) => respond.NotFound
      case UnauthorizedException => respond.Unauthorized
      case DecodingFailure(msg, path) =>
        respond.InternalServerError(s"Failed to decode DOI: $msg [$path]")
      case DoiServiceException(e) =>
        respond.InternalServerError(
          s"Failed to communicate with DOI service: $e"
        )
      case ForbiddenException(e) => respond.Forbidden(e)
      case PublishJobException(e) => respond.InternalServerError(e.toString)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  override def unpublish(
    respond: GuardrailResource.UnpublishResponse.type
  )(
    organizationId: Int,
    datasetId: Int,
    body: definitions.UnpublishRequest
  ): Future[GuardrailResource.UnpublishResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(organizationId),
      datasetId = Some(datasetId)
    )
    withServiceOwnerAuthorization[GuardrailResource.UnpublishResponse](
      claim,
      organizationId,
      datasetId
    ) { _ =>
      val query = for {
        dataset <- PublicDatasetsMapper.getDatasetFromSourceIds(
          organizationId,
          datasetId
        )

        _ <- PublicDatasetVersionsMapper
          .isPublishing(dataset)
          .flatMap(
            if (_)
              DBIO.failed(
                ForbiddenException(
                  "Cannot unpublish a dataset that is being published"
                )
              )
            else
              DBIO.successful(())
          )

        versions <- PublicDatasetVersionsMapper
          .getSuccessfulVersions(dataset)
          .result
          .map(_.toList)

        _ = ports.log.info(
          s"Unpublishing dataset ${dataset.id} versions ${versions.map(_.version)}"
        )

        // If the last version is embargoed, or failed to publish, remove it entirely
        _ <- PublicDatasetVersionsMapper.rollbackIfNeeded(dataset)

        _ <- PublicDatasetVersionsMapper.setStatus(
          dataset.id,
          versions.filter(_.status == PublishStatus.PublishSucceeded),
          Unpublished
        )
        _ <- DBIO.from(ports.searchClient.removeDataset(dataset))

        token = Authenticator.generateServiceToken(
          ports.jwt,
          organizationId = organizationId,
          datasetId = datasetId
        )
        headers = List(Authorization(OAuth2BearerToken(token.value)))

        _ <- DBIO.from(
          versions.traverse(
            version =>
              ports.doiClient
                .hideDoi(version.doi, headers)
          )
        )
        _ <- DBIO.from(
          deleteAssetsMulti(
            s3Key = dataset.id.toString,
            versions.map(_.s3Bucket).toSet
          )
        )
        status <- PublicDatasetVersionsMapper.getDatasetStatus(dataset)
      } yield status

      ports.db.run(query.transactionally).map(respond.OK)
    }.recover {
      case NoDatasetForSourcesException(_, _) =>
        respond.NoContent
      case UnauthorizedException => respond.Unauthorized
      case DecodingFailure(msg, path) =>
        respond.InternalServerError(s"Failed to decode DOI: $msg [$path]")
      case DoiServiceException(e) =>
        respond.InternalServerError(
          s"Failed to communicate with DOI service: $e"
        )
      case ForbiddenException(e) => respond.Forbidden(e)
      case PublishJobException(e) => respond.InternalServerError(e.toString)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }

  }

  override def getStatus(
    respond: GuardrailResource.GetStatusResponse.type
  )(
    organizationId: Int,
    datasetId: Int
  ): Future[GuardrailResource.GetStatusResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(organizationId),
      datasetId = Some(datasetId)
    )
    withAuthorization[GuardrailResource.GetStatusResponse](
      claim,
      organizationId,
      datasetId
    ) { _ =>
      ports.db
        .run(
          PublicDatasetVersionsMapper
            .getDatasetStatus(organizationId, datasetId)
            .transactionally
        )
        .map { status =>
          GuardrailResource.GetStatusResponse
            .OK(status)
        }
    }.recover {
      case UnauthorizedException => respond.Unauthorized
      case ForbiddenException(e) => respond.Forbidden(e)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  override def getStatuses(
    respond: GuardrailResource.GetStatusesResponse.type
  )(
    organizationId: Int
  ): Future[GuardrailResource.GetStatusesResponse] = {
    implicit val logContext: DiscoverLogContext =
      DiscoverLogContext(
        organizationId = Some(organizationId),
        datasetId = None
      )

    withOrganizationAccess[GuardrailResource.GetStatusesResponse](
      claim,
      organizationId
    ) { _ =>
      ports.db
        .run(
          PublicDatasetVersionsMapper
            .getDatasetStatuses(organizationId)
        )
        .map { statuses =>
          GuardrailResource.GetStatusesResponse
            .OK(statuses.toVector)
        }
    }.recover {
      case UnauthorizedException => respond.Unauthorized
      case ForbiddenException(e) => respond.Forbidden(e)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  override def sponsorDataset(
    respond: GuardrailResource.SponsorDatasetResponse.type
  )(
    sourceOrganizationId: Int,
    sourceDatasetId: Int,
    body: SponsorshipRequest
  ): Future[GuardrailResource.SponsorDatasetResponse] = {

    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(sourceOrganizationId),
      datasetId = Some(sourceDatasetId)
    )

    withAuthorization[GuardrailResource.SponsorDatasetResponse](
      claim,
      sourceOrganizationId,
      sourceDatasetId
    ) { _ =>
      val query = for {
        sponsorship <- SponsorshipsMapper.createOrUpdate(
          sourceOrganizationId = sourceOrganizationId,
          sourceDatasetId = sourceDatasetId,
          title = body.title,
          imageUrl = body.imageUrl,
          markup = body.markup
        )
        dataset <- PublicDatasetsMapper.getDatasetFromSourceIds(
          sourceOrganizationId,
          sourceDatasetId
        )

        version <- PublicDatasetVersionsMapper
          .getLatestVisibleVersion(dataset)
          .flatMap {
            case Some(v) if v.status == PublishStatus.Unpublished =>
              DBIO.failed(DatasetUnpublishedException(dataset, v))
            case Some(v) => DBIO.successful(v)
            case None => DBIO.failed(NoDatasetException(dataset.id))
          }

        (contributors, collections, externalPublications, _, revision) <- PublicDatasetVersionsMapper
          .getDatasetDetails(dataset, version)

      } yield
        (
          dataset,
          version,
          contributors,
          sponsorship,
          revision,
          collections,
          externalPublications
        )

      for {
        (
          dataset,
          version,
          contributors,
          sponsorship,
          revision,
          collections,
          externalPublications
        ) <- ports.db
          .run(query.transactionally)

        readme <- ports.s3StreamClient.readDatasetReadme(version, revision)

        _ <- ports.searchClient.indexSponsoredDataset(
          dataset,
          version,
          contributors,
          readme,
          collections,
          externalPublications,
          revision,
          Some(sponsorship)
        )
      } yield
        respond.Created(
          SponsorshipResponse(
            datasetId = sponsorship.datasetId,
            sponsorshipId = sponsorship.id
          )
        )

    }.recover {
      case e @ NoDatasetForSourcesException(_, _) =>
        respond.NotFound(e.getMessage)
      case UnauthorizedException => respond.Unauthorized
      case ForbiddenException(e) => respond.Forbidden(e)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  override def removeDatasetSponsor(
    respond: GuardrailResource.RemoveDatasetSponsorResponse.type
  )(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  ): Future[GuardrailResource.RemoveDatasetSponsorResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(sourceOrganizationId),
      datasetId = Some(sourceDatasetId)
    )

    withAuthorization[GuardrailResource.RemoveDatasetSponsorResponse](
      claim,
      sourceOrganizationId,
      sourceDatasetId
    ) { _ =>
      val query = for {
        _ <- SponsorshipsMapper.delete(
          sourceOrganizationId = sourceOrganizationId,
          sourceDatasetId = sourceDatasetId
        )
        dataset <- PublicDatasetsMapper.getDatasetFromSourceIds(
          sourceOrganizationId,
          sourceDatasetId
        )
        version <- PublicDatasetVersionsMapper
          .getLatestVersion(dataset.id)
          .flatMap {
            case Some(latestVersion)
                if latestVersion.status == PublishStatus.Unpublished =>
              DBIO
                .failed(DatasetUnpublishedException(dataset, latestVersion))
            case Some(latestVersion) => DBIO.successful(latestVersion)
            case None => DBIO.failed(NoDatasetException(dataset.id))
          }

        (contributors, collections, externalPublications, _, revision) <- PublicDatasetVersionsMapper
          .getDatasetDetails(dataset, version)

      } yield
        (
          dataset,
          version,
          contributors,
          revision,
          collections,
          externalPublications
        )

      for {
        (
          dataset,
          version,
          contributors,
          revision,
          collections,
          externalPublications
        ) <- ports.db.run(query.transactionally)

        readme <- ports.s3StreamClient.readDatasetReadme(version, revision)

        _ <- ports.searchClient.indexSponsoredDataset(
          dataset,
          version,
          contributors,
          readme,
          collections,
          externalPublications,
          revision
        )
      } yield respond.NoContent

    }.recover {
      case e @ NoDatasetForSourcesException(_, _) =>
        respond.NotFound(e.getMessage)
      case UnauthorizedException => respond.Unauthorized
      case ForbiddenException(e) => respond.Forbidden(e)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  def deleteAssets(
    s3Key: String,
    publishBucket: String,
    embargoBucket: String
  ): Future[InvokeResponse] = {
    ports.lambdaClient.runS3Clean(s3Key, publishBucket, embargoBucket)
  }

  private def deleteAssetsMulti(
    s3Key: String,
    buckets: Set[S3Bucket]
  ): Future[Iterator[InvokeResponse]] = {
    val atMostTwoAtATime = buckets.grouped(2)
    Future.sequence(
      atMostTwoAtATime
        .map(_.toList match {
          case List(S3Bucket(b1), S3Bucket(b2)) => deleteAssets(s3Key, b1, b2)
          case List(S3Bucket(b)) => deleteAssets(s3Key, b, b)
          case _ =>
            throw new AssertionError(
              s"${atMostTwoAtATime} shouldn't produce lists with more than two elements!"
            )
        })
    )
  }

  def getOrCreateDoi(
    organizationId: Int,
    datasetId: Int
  )(implicit
    logContext: DiscoverLogContext
  ): Future[DoiDTO] = {
    val token = Authenticator.generateServiceToken(
      ports.jwt,
      organizationId = organizationId,
      datasetId = datasetId
    )
    val headers = List(Authorization(OAuth2BearerToken(token.value)))

    for {
      latestDoi <- ports.doiClient
        .getLatestDoi(organizationId, datasetId, headers)
        .recoverWith {
          case NoDoiException => {
            // no DOI exists for the dataset, so create a new one
            ports.log.info("creating new DOI: no existing DOI found")
            ports.doiClient.createDraftDoi(organizationId, datasetId, headers)
          }
        }
      isDuplicateDoi <- ports.db.run(
        PublicDatasetVersionsMapper.isDuplicateDoi(latestDoi.doi)
      )
      isFindable = latestDoi.state.contains(DoiState.Findable)
      validDoi <- if (isFindable || isDuplicateDoi) {
        // create a new draft DOI if the latest DOI is Findable, or if the latest DOI is already associated with a dataset version
        ports.log.info(
          s"creating new DOI: existing DOI ${latestDoi.doi} is not usable (isFindable: ${isFindable}, isDuplicateDoi: ${isDuplicateDoi})"
        )
        ports.doiClient.createDraftDoi(organizationId, datasetId, headers)
      } else Future.successful(latestDoi)
    } yield validDoi
  }
}

object PublishHandler {
  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route = {
    logRequestAndResponse(ports) {
      authenticateJwt(system.name)(ports.jwt) { claim =>
        GuardrailResource.routes(new PublishHandler(ports, claim))
      }
    }
  }
}
