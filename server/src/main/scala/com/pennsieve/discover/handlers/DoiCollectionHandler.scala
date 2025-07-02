// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import com.pennsieve.auth.middleware.AkkaDirective.authenticateJwt
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.withServiceOwnerAuthorization
import com.pennsieve.discover.db.{
  PublicDatasetDoiCollectionDoisMapper,
  PublicDatasetDoiCollectionsMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper
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
  DoiCreationException,
  DoiServiceException,
  DuplicateDoiException,
  ForbiddenException,
  MissingParameterException,
  Ports,
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
import com.pennsieve.discover.handlers.DoiCollectionHandler.{
  collectionOrgId,
  collectionOrgName
}
import com.pennsieve.models.DatasetType.Collection
import com.pennsieve.models.PublishStatus.PublishFailed

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
          "finalizeDoiCollection() finished [response]: $response"
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
      case ForbiddenException(e) => respond.Forbidden(e)
      case NonFatal(e) => respond.InternalServerError(e.toString)
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
  ] = ???

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
}

object DoiCollectionHandler {
  val collectionOrgId: Int = PublicDatasetDoiCollection.collectionOrgId
  val collectionOrgName: String = PublicDatasetDoiCollection.collectionOrgName

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
