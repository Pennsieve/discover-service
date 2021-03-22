// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpRequest, StatusCode }
import akka.stream.Materializer
import cats.data.EitherT
import cats.implicits._

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

case class HttpError(statusCode: StatusCode, body: String) extends Throwable {
  override def getMessage: String = s"HTTP $statusCode: $body"
}

object HttpClient {
  type HttpClient =
    HttpRequest => EitherT[Future, HttpError, String]

  def sendHttpRequest(
    req: HttpRequest
  )(implicit
    system: ActorSystem,
    mat: Materializer,
    ec: ExecutionContext
  ): EitherT[Future, HttpError, String] =
    EitherT[Future, HttpError, String] {
      for {
        resp <- Http().singleRequest(req)
        body <- resp.entity.toStrict(5.seconds)
      } yield
        if (resp.status.isSuccess()) body.data.utf8String.asRight
        else HttpError(resp.status, body.data.utf8String).asLeft
    }

  def apply(
  )(implicit
    system: ActorSystem,
    mat: Materializer,
    ec: ExecutionContext
  ): HttpClient =
    sendHttpRequest(_)
}
