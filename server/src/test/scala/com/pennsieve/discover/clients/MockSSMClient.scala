// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

class MockSSMClient(defaults: Map[String, String] = Map.empty)
    extends SSMClient {

  // Store parameter values that can be configured per test
  val parameters: mutable.Map[String, String] = mutable.Map.from(defaults)

  // Track which parameters were requested
  val requestedParameters: ListBuffer[String] = ListBuffer.empty

  def setParameter(name: String, value: String): Unit = {
    parameters(name) = value
  }

  private var failNextWith: Option[Throwable] = None

  /** Cause the next call to getParameter or getBooleanParameter to return Future.failed(t). */
  def failNext(t: Throwable): Unit = failNextWith = Some(t)

  def clear(): Unit = {
    parameters.clear()
    parameters ++= defaults
    requestedParameters.clear()
    failNextWith = None
  }

  override def getParameter(
    parameterName: String
  )(implicit
    ec: ExecutionContext
  ): Future[String] = {
    requestedParameters += parameterName
    failNextWith match {
      case Some(t) =>
        failNextWith = None
        Future.failed(t)
      case None =>
        parameters.get(parameterName) match {
          case Some(value) => Future.successful(value)
          case None =>
            Future.failed(
              new RuntimeException(s"Parameter not found: $parameterName")
            )
        }
    }
  }

  override def getBooleanParameter(
    parameterName: String,
    defaultValue: Boolean = false
  )(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    requestedParameters += parameterName
    failNextWith match {
      case Some(t) =>
        failNextWith = None
        Future.failed(t)
      case None =>
        Future.successful(
          parameters
            .get(parameterName)
            .map(_.toLowerCase.trim == "true")
            .getOrElse(defaultValue)
        )
    }

  }
}
