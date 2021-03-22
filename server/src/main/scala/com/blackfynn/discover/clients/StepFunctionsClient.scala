// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import cats.implicits._
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sfn.SfnAsyncClient
import software.amazon.awssdk.services.sfn.model.{
  StartExecutionRequest,
  StartExecutionResponse
}
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient

import com.pennsieve.discover.models.{ EmbargoReleaseJob, PublishJob }
import io.circe.syntax._

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }

trait StepFunctionsClient {
  def startPublish(
    job: PublishJob
  )(implicit
    ec: ExecutionContext
  ): Future[StartExecutionResponse]

  def startRelease(
    job: EmbargoReleaseJob
  )(implicit
    ec: ExecutionContext
  ): Future[StartExecutionResponse]
}

class AwsStepFunctionsClient(
  publishStateMachineArn: String,
  releaseStateMachineArn: String,
  region: Region
) extends StepFunctionsClient {

  private lazy val client: SfnAsyncClient = SfnAsyncClient
    .builder()
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region(region)
    .build()

  def startPublish(
    job: PublishJob
  )(implicit
    ec: ExecutionContext
  ): Future[StartExecutionResponse] = {

    val request = StartExecutionRequest
      .builder()
      .stateMachineArn(publishStateMachineArn)
      .input(job.asJson.toString)
      .build()

    client.startExecution(request).toScala
  }

  def startRelease(
    job: EmbargoReleaseJob
  )(implicit
    ec: ExecutionContext
  ): Future[StartExecutionResponse] = {

    val request = StartExecutionRequest
      .builder()
      .stateMachineArn(releaseStateMachineArn)
      .input(job.asJson.toString)
      .build()

    client.startExecution(request).toScala
  }
}
