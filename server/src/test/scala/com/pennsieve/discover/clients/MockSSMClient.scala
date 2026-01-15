// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

class MockSSMClient extends SSMClient {

  // Store parameter values that can be configured per test
  val parameters: mutable.Map[String, String] = mutable.Map.empty

  // Track which parameters were requested
  val requestedParameters: ListBuffer[String] = ListBuffer.empty

  def setParameter(name: String, value: String): Unit = {
    parameters(name) = value
  }

  def clear(): Unit = {
    parameters.clear()
    requestedParameters.clear()
  }

  override def getParameter(
    parameterName: String
  )(implicit
    ec: ExecutionContext
  ): Future[String] = {
    requestedParameters += parameterName
    parameters.get(parameterName) match {
      case Some(value) => Future.successful(value)
      case None =>
        Future.failed(
          new RuntimeException(s"Parameter not found: $parameterName")
        )
    }
  }

  override def getBooleanParameter(
    parameterName: String,
    defaultValue: Boolean = false
  )(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    requestedParameters += parameterName
    Future.successful(
      parameters
        .get(parameterName)
        .map(_.toLowerCase.trim == "true")
        .getOrElse(defaultValue)
    )
  }
}
