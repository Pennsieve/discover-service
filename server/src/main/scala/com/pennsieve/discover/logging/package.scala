// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import akka.http.scaladsl.server.{ Directive, RouteResult }
import akka.http.scaladsl.server.directives.{
  DebuggingDirectives,
  LoggingMagnet
}
import akka.http.scaladsl.model.HttpRequest

package object logging {
  def logRequestAndResponse(ports: Ports): Directive[Unit] =
    DebuggingDirectives.logRequestResult(
      LoggingMagnet(
        _ =>
          req => {
            case RouteResult.Complete(resp) =>
              ports.logger.noContext
                .info(s"${req.method} ${req.uri} ${resp.status}")
            case _ => ()
          }
      )
    )
}
