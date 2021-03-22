// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.clients

import java.time.OffsetDateTime

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{
  Authorization,
  OAuth2BearerToken,
  RawHeader
}
import cats.data.EitherT
import cats.implicits._
import com.blackfynn.auth.middleware.Jwt
import com.blackfynn.discover.Authenticator
import com.blackfynn.discover.clients.HttpClient.HttpClient
import com.blackfynn.discover.server.definitions.DatasetPublishStatus
import com.blackfynn.models.PublishStatus
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{ Decoder, Encoder }

import scala.concurrent.{ ExecutionContext, Future }

trait PennsieveApiClient {

  /**
    * Notify API that a publish job has finished.
    */
  def putPublishComplete(
    publishStatus: DatasetPublishStatus,
    error: Option[String] = None
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit]

  /**
    * Request preview access to a dataset.
    */
  def requestPreview(
    organizationId: Int,
    previewRequest: APIPreviewAccessRequest
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit]

  /**
    * Tell API that an embargoed dataset is ready to be released.
    */
  def startRelease(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit]

  def getDatasetPreviewers(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, List[DatasetPreview]]

  def getDataUseAgreement(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Option[DataUseAgreement]]

}

class PennsieveApiClientImpl(
  host: String,
  jwt: Jwt.Config,
  httpClient: HttpClient
) extends PennsieveApiClient {

  /**
    * Add required Pennsieve API headers to the request
    */
  private def withApiHeaders(
    req: HttpRequest,
    organizationId: Int,
    datasetId: Int
  ): HttpRequest =
    req.withHeaders(
      Authorization(
        OAuth2BearerToken(
          Authenticator
            .generateServiceToken(jwt, organizationId, datasetId)
            .value
        )
      ),
      RawHeader("X-ORGANIZATION-INT-ID", organizationId.toString)
    )

  def putPublishComplete(
    publishStatus: DatasetPublishStatus,
    error: Option[String] = None
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit] = {
    val success = publishStatus.status in Seq(
      PublishStatus.PublishSucceeded,
      PublishStatus.EmbargoSucceeded
    )
    val organizationId = publishStatus.sourceOrganizationId
    val datasetId = publishStatus.sourceDatasetId

    val req =
      HttpRequest(
        uri = s"${host}/datasets/$datasetId/publication/complete",
        method = HttpMethods.PUT,
        entity = HttpEntity(
          ContentType(MediaTypes.`application/json`),
          PublishCompleteRequest(
            publishedDatasetId = publishStatus.publishedDatasetId,
            publishedVersionCount = publishStatus.publishedVersionCount,
            lastPublishedDate = publishStatus.lastPublishedDate,
            status = publishStatus.status,
            success,
            error
          ).asJson.toString
        )
      )
    httpClient(withApiHeaders(req, organizationId, datasetId)).map(_ => ())
  }

  def requestPreview(
    organizationId: Int,
    previewRequest: APIPreviewAccessRequest
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit] = {
    val req =
      HttpRequest(
        uri = s"${host}/datasets/publication/preview/request",
        method = HttpMethods.POST,
        entity = HttpEntity(
          ContentType(MediaTypes.`application/json`),
          previewRequest.asJson.toString
        )
      )

    httpClient(withApiHeaders(req, organizationId, previewRequest.datasetId))
      .map(_ => ())
  }

  def getDatasetPreviewers(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, List[DatasetPreview]] = {
    val request =
      HttpRequest(
        uri = s"${host}/datasets/$sourceDatasetId/publication/preview",
        method = HttpMethods.GET
      )
    httpClient(withApiHeaders(request, sourceOrganizationId, sourceDatasetId))
      .subflatMap { payload =>
        decode[List[DatasetPreview]](payload)
      }
      .leftMap(e => HttpError(500, e.getMessage))
  }

  def startRelease(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit] = {
    val request =
      HttpRequest(
        uri = s"${host}/datasets/$sourceDatasetId/publication/release",
        method = HttpMethods.POST
      )
    httpClient(withApiHeaders(request, sourceOrganizationId, sourceDatasetId))
      .map(_ => ())
  }

  def getDataUseAgreement(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Option[DataUseAgreement]] = {
    val request =
      HttpRequest(
        uri =
          s"${host}/datasets/$sourceDatasetId/publication/data-use-agreement",
        method = HttpMethods.GET
      )
    httpClient(withApiHeaders(request, sourceOrganizationId, sourceDatasetId))
      .subflatMap {
        case "" => Right(None)
        case s => decode[DataUseAgreement](s).map(_.some)
      }
      .leftMap(e => HttpError(500, e.getMessage))
  }
}

case class User(id: String, email: String, intId: Int)

object User {
  implicit val encoder: Encoder[User] =
    deriveEncoder[User]
  implicit val decoder: Decoder[User] =
    deriveDecoder[User]
}

case class DatasetPreview(user: User, embargoAccess: String)

object DatasetPreview {
  implicit val encoder: Encoder[DatasetPreview] =
    deriveEncoder[DatasetPreview]
  implicit val decoder: Decoder[DatasetPreview] =
    deriveDecoder[DatasetPreview]
}

case class PublishCompleteRequest(
  publishedDatasetId: Option[Int],
  publishedVersionCount: Int,
  lastPublishedDate: Option[OffsetDateTime],
  status: PublishStatus,
  success: Boolean,
  error: Option[String]
)

object PublishCompleteRequest {
  implicit val encoder: Encoder[PublishCompleteRequest] =
    deriveEncoder[PublishCompleteRequest]
  implicit val decoder: Decoder[PublishCompleteRequest] =
    deriveDecoder[PublishCompleteRequest]
}

case class DataUseAgreement(id: Int, name: String, body: String)

object DataUseAgreement {
  implicit val encoder: Encoder[DataUseAgreement] =
    deriveEncoder[DataUseAgreement]
  implicit val decoder: Decoder[DataUseAgreement] =
    deriveDecoder[DataUseAgreement]
}

case class APIPreviewAccessRequest(
  datasetId: Int,
  userId: Int,
  dataUseAgreementId: Option[Int] = None
)

object APIPreviewAccessRequest {
  implicit val encoder: Encoder[APIPreviewAccessRequest] =
    deriveEncoder[APIPreviewAccessRequest]
  implicit val decoder: Decoder[APIPreviewAccessRequest] =
    deriveDecoder[APIPreviewAccessRequest]
}
