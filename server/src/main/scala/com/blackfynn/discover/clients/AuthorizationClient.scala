// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.clients

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{
  Authorization,
  OAuth2BearerToken,
  RawHeader
}
import cats.data._
import cats.implicits._
import com.blackfynn.discover.clients.HttpClient.HttpClient

import scala.concurrent.{ ExecutionContext, Future }
import java.net.URL

trait AuthorizationClient {

  import AuthorizationClient._

  def authorizeEmbargoPreview(
    organizationId: Int,
    datasetId: Int,
    authorization: Authorization
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, EmbargoAuthorization]

}

object AuthorizationClient {
  sealed trait EmbargoAuthorization

  object EmbargoAuthorization {
    case object OK extends EmbargoAuthorization
    case object Unauthorized extends EmbargoAuthorization
    case object Forbidden extends EmbargoAuthorization
  }
}

class AuthorizationClientImpl(host: String, httpClient: HttpClient)
    extends AuthorizationClient {

  import AuthorizationClient._

  def authorizeEmbargoPreview(
    organizationId: Int,
    datasetId: Int,
    authorization: Authorization
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, EmbargoAuthorization] = {
    val request =
      HttpRequest(
        uri =
          s"${host}/authorization/organizations/$organizationId/datasets/$datasetId/discover/preview",
        method = HttpMethods.GET
      ).withHeaders(authorization)

    httpClient(request)
      .map[EmbargoAuthorization](_ => EmbargoAuthorization.OK)
      .recover {
        case HttpError(StatusCodes.Unauthorized, _) =>
          EmbargoAuthorization.Unauthorized
        case HttpError(StatusCodes.Forbidden, _) =>
          EmbargoAuthorization.Forbidden
      }
  }
}
