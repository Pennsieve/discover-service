// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import cats.implicits._
import com.pennsieve.auth.middleware.AkkaDirective.authenticateJwt
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.withServiceOwnerAuthorization
import com.pennsieve.discover.db.{
  PublicContributorsMapper,
  PublicDatasetDoiCollectionDoisMapper,
  PublicDatasetDoiCollectionsMapper,
  PublicDatasetVersionFilesTableMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicFileVersionsMapper
}
import com.pennsieve.discover.logging.{
  logRequestAndResponse,
  DiscoverLogContext
}
import com.pennsieve.discover.models.{
  PennsieveSchemaVersion,
  PublicDataset,
  PublicDatasetDoiCollection,
  PublicDatasetVersion,
  PublishingWorkflow
}
import com.pennsieve.discover.{
  utils,
  Authenticator,
  DoiCreationException,
  DoiServiceException,
  DuplicateDoiException,
  ForbiddenException,
  MissingParameterException,
  NoDatasetForSourcesException,
  Ports,
  PublishJobException,
  UnauthorizedException
}
import com.pennsieve.discover.server.collection.{
  CollectionHandler => GuardrailHandler,
  CollectionResource => GuardrailResource
}
import com.pennsieve.discover.server.definitions.{
  FinalizeDoiCollectionRequest,
  PublishDoiCollectionRequest
}
import com.pennsieve.discover.utils.{ getOrCreateDoi, BucketResolver }
import com.pennsieve.models.PublishStatus
import io.circe.DecodingFailure
import slick.jdbc.TransactionIsolation
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.notifications.{
  PushDoiRequest,
  SQSMessenger,
  SQSNotificationType
}
import com.pennsieve.models.DatasetType.Collection
import com.pennsieve.models.PublishStatus.{
  PublishFailed,
  PublishInProgress,
  Unpublished
}
import slick.dbio.{ DBIO, DBIOAction }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

class DoiCollectionHandler(
  ports: Ports,
  claim: Jwt.Claim
)(implicit
  system: ActorSystem,
  executionContext: ExecutionContext
) extends GuardrailHandler {
  type PublishDoiCollectionResponse =
    GuardrailResource.PublishDoiCollectionResponse
  type FinalizeDoiCollectionResponse =
    GuardrailResource.FinalizeDoiCollectionResponse
  private val pennsieveDoiPrefix =
    ports.config.doiCollections.pennsieveDoiPrefix

  private val collectionOrgId = ports.config.doiCollections.idSpace.id
  private val collectionOrgName = ports.config.doiCollections.idSpace.name

  override def publishDoiCollection(
    respond: GuardrailResource.PublishDoiCollectionResponse.type
  )(
    collectionId: Int,
    body: PublishDoiCollectionRequest
  ): Future[PublishDoiCollectionResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      datasetId = Some(collectionId),
      userId = Some(body.ownerId)
    )

    ports.log.info(s"publishDoiCollection() starting request: ${body}")
    val bucketResolver = BucketResolver(ports)
    val (targetS3Bucket, _) =
      bucketResolver.resolveBucketConfig(
        body.bucketConfig,
        Some(PublishingWorkflow.Version5)
      )

    withServiceOwnerAuthorization[PublishDoiCollectionResponse](
      claim,
      collectionOrgId,
      collectionId
    ) { _ =>
      validateBody(body).flatMap { _ =>
        getOrCreateDoi(ports, collectionOrgId, collectionId)
          .flatMap { doi =>
            ports.log.info(s"publishDoiCollection() DOI: $doi")

            val query = for {
              publicDataset <- PublicDatasetsMapper
                .createOrUpdate(
                  name = body.name,
                  sourceOrganizationId = collectionOrgId,
                  sourceOrganizationName = collectionOrgName,
                  sourceDatasetId = collectionId,
                  ownerId = body.ownerId,
                  ownerFirstName = body.ownerFirstName,
                  ownerLastName = body.ownerLastName,
                  ownerOrcid = body.ownerOrcid,
                  license = body.license,
                  tags = body.tags.toList,
                  datasetType = Collection
                )

              _ = ports.log.info(
                s"publishDoiCollection() public dataset: $publicDataset"
              )

              // If the previous publish job failed, roll the dataset back to the
              // previous version
              _ <- PublicDatasetVersionsMapper.rollbackIfNeeded(publicDataset)

              version <- PublicDatasetVersionsMapper
                .create(
                  id = publicDataset.id,
                  status = PublishStatus.PublishInProgress,
                  description = body.description,
                  fileCount = 1,
                  s3Bucket = targetS3Bucket,
                  doi = doi.doi,
                  schemaVersion = PennsieveSchemaVersion.`5.0`,
                  migrated = true
                )
              _ = ports.log.info(
                s"publishDoiCollection() public dataset version : $version"
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
                s"publishDoiCollection() [stored] contributors: $contributors"
              )

              _ <- PublicDatasetDoiCollectionsMapper.add(
                PublicDatasetDoiCollection(
                  datasetId = version.datasetId,
                  datasetVersion = version.version,
                  banners = body.banners.toList
                )
              )
              _ <- PublicDatasetDoiCollectionDoisMapper.addDOIs(
                datasetId = version.datasetId,
                datasetVersion = version.version,
                dois = body.dois.toList
              )
              response = com.pennsieve.discover.server.definitions
                .PublishDoiCollectionResponse(
                  name = publicDataset.name,
                  sourceCollectionId = publicDataset.sourceDatasetId,
                  publishedDatasetId = version.datasetId,
                  publishedVersion = version.version,
                  status = version.status,
                  lastPublishedDate = Some(version.createdAt),
                  sponsorship = None,
                  publicId = version.doi
                )

            } yield respond.Created(response)

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
      }
    }.recover {
      case UnauthorizedException => respond.Unauthorized
      case DecodingFailure(msg, path) =>
        respond.InternalServerError(s"Decoding error: $msg [$path]")
      case DoiCreationException(e) =>
        respond.BadRequest(s"Failed to create a DOI for the collection: $e")
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
      case PublishDoiCollectionRequestValidationError(msg) =>
        respond.BadRequest(msg)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }

  }

  private def findNonPennsieveDois(dois: Seq[String]): Seq[String] =
    dois.filterNot(_.startsWith(s"$pennsieveDoiPrefix/"))

  private def lookupPennsieveDois(dois: Seq[String]): Future[
    (
      Map[String, PublicDatasetVersionsMapper.DatasetDetails],
      Map[String, (PublicDataset, PublicDatasetVersion)]
    )
  ] =
    ports.db
      .run(PublicDatasetVersionsMapper.getDatasetsByDoi(dois.toList))
      .map(x => (x.published, x.unpublished))

  private def validateBody(body: PublishDoiCollectionRequest): Future[Unit] = {
    val dois = Option(body.dois).getOrElse(Seq.empty)

    for {
      _ <- dois match {
        case Nil =>
          Future.failed(
            PublishDoiCollectionRequestValidationError("no DOIs in request")
          )
        case _ => Future.successful(())
      }

      // For now, non-Pennsieve DOIs are not allowed. May be allowed in future.
      _ <- findNonPennsieveDois(dois) match {
        case Nil => Future.successful(())
        case nonPenn =>
          Future.failed(
            PublishDoiCollectionRequestValidationError(
              s"Collection contains non-Pennsieve DOIs: ${nonPenn.mkString(", ")}"
            )
          )
      }
      (pennsievePublished, pennsieveUnpublished) <- lookupPennsieveDois(dois)

      // Don't allow publication of a DOI collection that contains unpublished DOIs
      _ <- pennsieveUnpublished match {
        case m if m.isEmpty => Future.successful(())
        case _ =>
          val nonPenn = pennsieveUnpublished.view.mapValues(_._2.status).toList
          Future.failed(
            PublishDoiCollectionRequestValidationError(
              s"Collection contains unpublished DOIs: ${nonPenn.mkString(", ")}"
            )
          )
      }

      // Don't allow publication of a DOI collection that contains another DOI collection
      _ <- pennsievePublished.view
        .filter(_._2.dataset.datasetType == Collection)
        .keys
        .toList match {
        case l if l.isEmpty => Future.successful(())
        case collectionDois =>
          Future.failed(
            PublishDoiCollectionRequestValidationError(
              s"Collection contains collection DOIs: ${collectionDois.mkString(", ")}"
            )
          )
      }
    } yield ()
  }

  override def finalizeDoiCollection(
    respond: GuardrailResource.FinalizeDoiCollectionResponse.type
  )(
    collectionId: Int,
    body: FinalizeDoiCollectionRequest
  ): Future[FinalizeDoiCollectionResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      datasetId = Some(collectionId),
      publicDatasetId = Some(body.publishedDatasetId),
      publicDatasetVersion = Some(body.publishedVersion)
    )

    withServiceOwnerAuthorization[FinalizeDoiCollectionResponse](
      claim,
      collectionOrgId,
      collectionId
    ) { _ =>
      val query = for {
        publicDataset <- PublicDatasetsMapper
          .getDatasetFromSourceIds(collectionOrgId, collectionId)

        _ = ports.log.info(s"finalizeDoiCollection() dataset: ${publicDataset}")

        version <- PublicDatasetVersionsMapper.getVersion(
          publicDataset.id,
          body.publishedVersion
        )
        _ = ports.log.info(s"finalizeDoiCollection() version: ${version}")

        _ <- checkVersionStatusForFinalize(version)

        finalStatus <- body.publishSuccess match {
          case true => publishSucceeded(publicDataset, version, body)
          case false => publishFailed(publicDataset, version)
        }

        updatedVersion <- PublicDatasetVersionsMapper.setStatus(
          id = publicDataset.id,
          version = version.version,
          status = finalStatus
        )
        _ = ports.log.info(
          s"finalizeDoiCollection() updated version: ${updatedVersion}"
        )
        response = com.pennsieve.discover.server.definitions
          .FinalizeDoiCollectionResponse(status = updatedVersion.status)
        _ = ports.log.info(
          s"finalizeDoiCollection() finished [response]: ${response}"
        )

      } yield respond.OK(response)
      ports.db
        .run(
          query.transactionally
            .withTransactionIsolation(TransactionIsolation.Serializable)
        )
    }.recover {
      case UnauthorizedException => respond.Unauthorized
      case DecodingFailure(msg, path) =>
        respond.InternalServerError(s"Decoding error: $msg [$path]")
      case MissingParameterException(parameter) =>
        respond.BadRequest(s"Missing parameter '$parameter'")
      case FinalizeDoiCollectionStatusError(msg) => respond.BadRequest(msg)
      case ForbiddenException(e) => respond.Forbidden(e)
      case NonFatal(e) => respond.InternalServerError(e.toString)
    }
  }

  private def checkVersionStatusForFinalize(
    version: PublicDatasetVersion
  )(implicit
    logContext: DiscoverLogContext
  ): DBIOAction[Unit, NoStream, Effect] = {
    version.status match {
      case PublishInProgress => DBIOAction.from(Future.successful(()))
      case _ => {
        ports.log.warn(
          s"DoiCollectionHandler.checkVersionStatusForFinalize() datasetId: ${version.datasetId} version: ${version.version} is in state ${version.status}"
        )
        DBIO.from(
          Future.failed(
            FinalizeDoiCollectionStatusError(
              s"no DOICollection publish in progress; status is ${version.status}"
            )
          )
        )
      }

    }
  }

  private def publishSucceeded(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    body: FinalizeDoiCollectionRequest
  )(implicit
    logContext: DiscoverLogContext
  ): DBIOAction[
    PublishStatus,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] =
    for {
      finalizedVersion <- PublicDatasetVersionsMapper.setResultMetadata(
        version = version,
        size = body.totalSize,
        fileCount = body.fileCount
      )
      _ = ports.log.info(
        s"DoiCollectionHandler.publishSucceeded() finalized version: ${finalizedVersion}"
      )

      // load the metadata (manifest.json) file from S3
      metadata <- DBIO.from(
        ports.s3StreamClient
          .loadDatasetMetadata(finalizedVersion)
      )
      _ = ports.log.info(
        s"DoiCollectionHandler.publishSucceeded() metadata: ${metadata}"
      )

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
      _ = ports.log.info(
        s"DoiCollectionHandler.publishSucceeded() updated manifestFiles: ${files}"
      )

      publicFileVersions <- DBIO.sequence(files.map { file =>
        PublicFileVersionsMapper.createOne(finalizedVersion, file)
      })
      _ = ports.log.info(
        s"DoiCollectionHandler.publishSucceeded() publicFileVersions: ${publicFileVersions}"
      )

      publicFileVersionLinks <- DBIO.sequence(
        publicFileVersions
          .map { pfv =>
            PublicDatasetVersionFilesTableMapper
              .storeLink(finalizedVersion, pfv)
          }
      )
      _ = ports.log.info(
        s"DoiCollectionHandler.publishSucceeded() publicFileVersionLinks: ${publicFileVersionLinks}"
      )

      _ = ports.log.info(
        "DoiCollectionHandler.publishSucceeded() [action] queue push DOI message"
      )
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
    } yield PublishStatus.PublishSucceeded

  private def publishFailed(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    logContext: DiscoverLogContext
  ): DBIOAction[PublishStatus, NoStream, Effect] = {
    ports.log.warn(
      s"DoiCollectionHandler.publishFailed() datasetId: ${dataset.id} version: ${version.version}"
    )
    DBIO.from(Future.successful(PublishFailed))
  }

  override def unpublishDoiCollection(
    respond: GuardrailResource.UnpublishDoiCollectionResponse.type
  )(
    collectionId: Int
  ): Future[GuardrailResource.UnpublishDoiCollectionResponse] = {
    implicit val logContext: DiscoverLogContext = DiscoverLogContext(
      organizationId = Some(collectionOrgId),
      datasetId = Some(collectionId)
    )
    withServiceOwnerAuthorization[
      GuardrailResource.UnpublishDoiCollectionResponse
    ](claim, collectionOrgId, collectionId) { _ =>
      val query = for {

        dataset <- PublicDatasetsMapper.getDatasetFromSourceIds(
          collectionOrgId,
          collectionId
        )

        _ <- PublicDatasetVersionsMapper
          .isPublishing(dataset)
          .flatMap(
            if (_)
              DBIO.failed(
                ForbiddenException(
                  "Cannot unpublish a DOI Collection that is being published"
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
          s"Unpublishing DOI Collection ${dataset.id} versions ${versions.map(_.version)}"
        )

        migrated = versions.get(0) match {
          case Some(version) => version.migrated
          case None => false
        }

        // If the last version failed to publish, remove it entirely
        _ <- PublicDatasetVersionsMapper.rollbackIfNeeded(dataset)

        _ <- PublicDatasetVersionsMapper.setStatus(
          dataset.id,
          versions.filter(_.status == PublishStatus.PublishSucceeded),
          Unpublished
        )
        _ <- DBIO.from(ports.searchClient.removeDataset(dataset))

        token = Authenticator.generateServiceToken(
          ports.jwt,
          organizationId = collectionOrgId,
          datasetId = collectionId
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
          utils.deleteAssetsMultiForUnpublish(
            ports.lambdaClient,
            s3KeyPrefix = dataset.id.toString,
            publishedDatasetId = dataset.id,
            versions.map(_.s3Bucket).toSet,
            migrated
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

}

object DoiCollectionHandler {
  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route = {
    logRequestAndResponse(ports) {
      authenticateJwt(system.name)(ports.jwt) { claim =>
        GuardrailResource.routes(new DoiCollectionHandler(ports, claim))
      }
    }
  }
}

case class PublishDoiCollectionRequestValidationError(msg: String)
    extends Throwable {
  override def getMessage: String = msg
}

case class FinalizeDoiCollectionStatusError(msg: String) extends Throwable {
  override def getMessage: String = msg
}
