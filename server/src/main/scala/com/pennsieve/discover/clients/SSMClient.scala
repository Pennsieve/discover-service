// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ssm.SsmAsyncClient
import software.amazon.awssdk.services.ssm.model.{
  GetParameterRequest,
  ParameterNotFoundException
}

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait SSMClient {

  /**
    * Fetches a parameter value from SSM Parameter Store.
    * @param parameterName The name of the parameter (will be combined with the configured path prefix)
    * @return The parameter value as a String
    */
  def getParameter(
    parameterName: String
  )(implicit
    ec: ExecutionContext
  ): Future[String]

  /**
    * Fetches a boolean parameter value from SSM Parameter Store.
    * @param parameterName The name of the parameter
    * @param defaultValue The default value if parameter doesn't exist or can't be parsed
    * @return The parameter value as a Boolean
    */
  def getBooleanParameter(
    parameterName: String,
    defaultValue: Boolean = false
  )(implicit
    ec: ExecutionContext
  ): Future[Boolean]
}

class AwsSSMClient(
  parameterPathPrefix: String,
  region: Region
) extends SSMClient
    with StrictLogging {

  private lazy val client: SsmAsyncClient = SsmAsyncClient
    .builder()
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region(region)
    .build()

  override def getParameter(
    parameterName: String
  )(implicit
    ec: ExecutionContext
  ): Future[String] = {
    val fullParameterPath = s"$parameterPathPrefix/$parameterName"

    val request = GetParameterRequest
      .builder()
      .name(fullParameterPath)
      .withDecryption(true)
      .build()

    client
      .getParameter(request)
      .toScala
      .map(_.parameter().value())
  }

  override def getBooleanParameter(
    parameterName: String,
    defaultValue: Boolean = false
  )(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    getParameter(parameterName)
      .map(value => value.toLowerCase.trim == "true")
      .recover {
        case _: ParameterNotFoundException =>
          logger.warn(
            s"SSM parameter $parameterName not found, using default value: $defaultValue"
          )
          defaultValue
        case e: Exception =>
          logger.error(
            s"Error fetching SSM parameter $parameterName, using default value: $defaultValue",
            e
          )
          defaultValue
      }
  }
}
