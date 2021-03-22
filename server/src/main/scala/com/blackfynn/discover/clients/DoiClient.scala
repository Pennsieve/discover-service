// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.stream.Materializer
import cats.data._
import cats.implicits._
import com.pennsieve.discover.server.definitions.InternalContributor
import com.pennsieve.doi.client.doi.{ DoiClient => DoiServiceClient }
import com.pennsieve.doi.client.definitions._
import com.pennsieve.doi.client.doi.{
  CreateDraftDoiResponse,
  GetLatestDoiResponse,
  HideDoiResponse,
  PublishDoiResponse,
  ReviseDoiResponse
}
import com.pennsieve.doi.models.{ DoiDTO, DoiState }
import com.pennsieve.discover.{
  Authenticator,
  DoiCreationException,
  DoiServiceException,
  ForbiddenException,
  NoDoiException,
  Ports,
  UnauthorizedException
}
import com.pennsieve.discover.models._
import com.pennsieve.models.License
import io.circe.{ DecodingFailure, Json }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Thin wrapper around the Guardrail DOI service client
  */
class DoiClient(
  doiServiceHost: String
)(implicit
  httpClient: HttpRequest => Future[HttpResponse],
  ec: ExecutionContext,
  mat: Materializer
) {

  val client = DoiServiceClient(doiServiceHost)

  def getLatestDoi(
    organizationId: Int,
    datasetId: Int,
    headers: List[HttpHeader]
  ): Future[DoiDTO] = {
    client
      .getLatestDoi(organizationId, datasetId, headers)
      .leftSemiflatMap(handleGuardrailError)
      .value
      .flatMap {
        case Right(response) =>
          response match {
            case GetLatestDoiResponse.OK(dto) =>
              decodeDoi(dto).fold(Future.failed(_), Future.successful(_))
            case GetLatestDoiResponse.NotFound(_) =>
              Future.failed(NoDoiException)
            case GetLatestDoiResponse.Forbidden(e) =>
              Future.failed(ForbiddenException(e))
            case GetLatestDoiResponse.Unauthorized =>
              Future.failed(UnauthorizedException)
            case GetLatestDoiResponse.InternalServerError(e) =>
              Future.failed(DoiServiceException(HttpError(500, e)))
          }
        case Left(e) => Future.failed(e)
      }
  }

  def createDraftDoi(
    organizationId: Int,
    datasetId: Int,
    headers: List[HttpHeader]
  ): Future[DoiDTO] = {

    client
      .createDraftDoi(
        organizationId,
        datasetId,
        body =
          CreateDraftDoiRequest(None, None, None, None, None, None, None, None),
        headers
      )
      .leftSemiflatMap(handleGuardrailError)
      .value
      .flatMap {
        case Right(response) =>
          response match {
            case CreateDraftDoiResponse.Created(dto) =>
              decodeDoi(dto).fold(Future.failed(_), Future.successful(_))
            case CreateDraftDoiResponse.Forbidden(e) =>
              Future.failed(ForbiddenException(e))
            case CreateDraftDoiResponse.Unauthorized =>
              Future.failed(UnauthorizedException)
            case CreateDraftDoiResponse.BadRequest(e) =>
              Future.failed(DoiCreationException(e))
            case CreateDraftDoiResponse.InternalServerError(e) =>
              Future.failed(DoiServiceException(HttpError(500, e)))
          }
        case Left(e) => Future.failed(e)
      }
  }

  def publishDoi(
    doi: String,
    name: String,
    publicationYear: Int,
    contributors: List[PublicContributor],
    url: String,
    owner: Option[InternalContributor] = None,
    version: Option[Int] = None,
    description: Option[String] = None,
    license: Option[License] = None,
    collections: List[PublicCollection] = List.empty,
    externalPublications: List[PublicExternalPublication] = List.empty,
    headers: List[HttpHeader]
  ): Future[DoiDTO] = {
    client
      .publishDoi(
        doi,
        PublishDoiRequest(
          title = name,
          creators = contributors
            .map(
              c =>
                CreatorDTO(
                  firstName = c.firstName,
                  lastName = c.lastName,
                  middleInitial = c.middleInitial,
                  orcid = c.orcid
                )
            )
            .toIndexedSeq,
          publicationYear = publicationYear,
          url = url,
          owner = owner.map(
            o =>
              CreatorDTO(
                firstName = o.firstName,
                lastName = o.lastName,
                middleInitial = o.middleInitial,
                orcid = o.orcid
              )
          ),
          version = version,
          description = description,
          collections = Some(
            collections
              .map(c => CollectionDTO(c.name, c.sourceCollectionId))
              .toIndexedSeq
          ),
          externalPublications = Some(
            externalPublications
              .map(
                p =>
                  ExternalPublicationDTO(
                    p.doi,
                    Some(p.relationshipType.entryName)
                  )
              )
              .toIndexedSeq
          ),
          licenses = license
            .map(
              l =>
                IndexedSeq(
                  LicenseDTO(
                    l.entryName,
                    License.licenseUri.get(l).getOrElse("")
                  )
                )
            )
        ),
        headers
      )
      .leftSemiflatMap(handleGuardrailError)
      .value
      .flatMap {
        case Right(response) =>
          response match {
            case PublishDoiResponse.OK(dto) =>
              decodeDoi(dto).fold(Future.failed(_), Future.successful(_))

            case PublishDoiResponse.BadRequest(e) =>
              Future.failed(DoiServiceException(HttpError(400, e)))
            case PublishDoiResponse.NotFound(_) =>
              Future.failed(NoDoiException)
            case PublishDoiResponse.Forbidden(e) =>
              Future.failed(ForbiddenException(e))
            case PublishDoiResponse.Unauthorized =>
              Future.failed(UnauthorizedException)
            case PublishDoiResponse.InternalServerError(e) =>
              Future.failed(DoiServiceException(HttpError(500, e)))
          }
        case Left(e) => Future.failed(e)
      }
  }

  def reviseDoi(
    doi: String,
    name: String,
    contributors: List[PublicContributor],
    owner: Option[InternalContributor] = None,
    version: Option[Int] = None,
    description: Option[String] = None,
    license: Option[License] = None,
    collections: List[PublicCollection] = List.empty,
    externalPublications: List[PublicExternalPublication] = List.empty,
    headers: List[HttpHeader]
  ): Future[DoiDTO] = {
    client
      .reviseDoi(
        doi,
        ReviseDoiRequest(
          title = name,
          creators = contributors
            .map(
              c =>
                CreatorDTO(
                  firstName = c.firstName,
                  lastName = c.lastName,
                  middleInitial = c.middleInitial,
                  orcid = c.orcid
                )
            )
            .toIndexedSeq,
          owner = owner.map(
            o =>
              CreatorDTO(
                firstName = o.firstName,
                lastName = o.lastName,
                middleInitial = o.middleInitial,
                orcid = o.orcid
              )
          ),
          version = version,
          description = description,
          collections = Some(
            collections
              .map(c => CollectionDTO(c.name, c.sourceCollectionId))
              .toIndexedSeq
          ),
          externalPublications = Some(
            externalPublications
              .map(
                p =>
                  ExternalPublicationDTO(
                    p.doi,
                    Some(p.relationshipType.entryName)
                  )
              )
              .toIndexedSeq
          ),
          licenses = license.map(
            l =>
              IndexedSeq(
                LicenseDTO(l.entryName, License.licenseUri.get(l).getOrElse(""))
              )
          )
        ),
        headers
      )
      .leftSemiflatMap(handleGuardrailError)
      .value
      .flatMap {
        case Right(response) =>
          response match {
            case ReviseDoiResponse.OK(dto) =>
              decodeDoi(dto).fold(Future.failed(_), Future.successful(_))
            case ReviseDoiResponse.BadRequest(e) =>
              Future.failed(DoiServiceException(HttpError(400, e)))
            case ReviseDoiResponse.NotFound(_) =>
              Future.failed(NoDoiException)
            case ReviseDoiResponse.Forbidden(e) =>
              Future.failed(ForbiddenException(e))
            case ReviseDoiResponse.Unauthorized =>
              Future.failed(UnauthorizedException)
            case ReviseDoiResponse.InternalServerError(e) =>
              Future.failed(DoiServiceException(HttpError(500, e)))
          }
        case Left(e) => Future.failed(e)
      }
  }

  def hideDoi(doi: String, headers: List[HttpHeader]): Future[DoiDTO] = {
    client
      .hideDoi(doi, headers)
      .leftSemiflatMap(handleGuardrailError)
      .value
      .flatMap {
        case Right(response) =>
          response match {
            case HideDoiResponse.OK(dto) =>
              decodeDoi(dto).fold(Future.failed(_), Future.successful(_))
            case HideDoiResponse.BadRequest(e) =>
              Future.failed(DoiServiceException(HttpError(400, e)))
            case HideDoiResponse.NotFound(_) =>
              Future.failed(NoDoiException)
            case HideDoiResponse.Forbidden(e) =>
              Future.failed(ForbiddenException(e))
            case HideDoiResponse.Unauthorized =>
              Future.failed(UnauthorizedException)
            case HideDoiResponse.InternalServerError(e) =>
              Future.failed(DoiServiceException(HttpError(500, e)))
          }
        case Left(e) => Future.failed(e)
      }
  }

  private def decodeDoi(dto: Json): Either[DecodingFailure, DoiDTO] =
    dto.as[DoiDTO] match {
      case Right(decodedDoi) => Right(decodedDoi)
      case Left(decodingFailure) => Left(decodingFailure)
    }

  /**
    * Handle errors from the Guardrail client.
    *
    * These are either HTTP responses that are not documented in the Swagger
    * file, or the error thrown by a failed Future during the request.
    */
  private def handleGuardrailError
    : Either[Throwable, HttpResponse] => Future[Throwable] =
    _.fold(
      error => Future.successful(DoiServiceException(error)),
      resp =>
        resp.entity.toStrict(5.seconds).map { entity =>
          DoiServiceException(HttpError(resp.status, entity.data.utf8String))
        }
    )

}
