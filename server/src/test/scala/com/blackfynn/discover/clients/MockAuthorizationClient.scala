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
import com.blackfynn.discover.Authenticator
import com.blackfynn.auth.middleware.Jwt

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

class MockAuthorizationClient(jwtKey: String) extends AuthorizationClient {

  // TODO: pass this in
  val jwt: Jwt.Config = new Jwt.Config {
    val key: String = jwtKey
  }

  import AuthorizationClient._

  val authorizedHeader =
    Authorization(
      OAuth2BearerToken(
        Authenticator.generateUserToken(jwt, 1, 1, Some(1), 1.hour).value
      )
    )

  val forbiddenHeader =
    Authorization(
      OAuth2BearerToken(
        Authenticator.generateUserToken(jwt, 9, 9, Some(9), 1.hour).value
      )
    )

  def authorizeEmbargoPreview(
    organizationId: Int,
    datasetId: Int,
    authorization: Authorization
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, EmbargoAuthorization] =
    EitherT.rightT[Future, HttpError] {
      if (authorization == authorizedHeader) EmbargoAuthorization.OK
      else if (authorization == forbiddenHeader) EmbargoAuthorization.Forbidden
      else EmbargoAuthorization.Unauthorized
    }
}
