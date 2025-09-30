// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.actor.ActorSystem
import software.amazon.awssdk.services.lambda.model.InvokeResponse

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

case class LambdaRequest(
  s3KeyPrefix: String,
  publishedDatasetId: Int,
  publishedDatasetVersion: Option[Int],
  publishBucket: String,
  embargoBucket: String,
  cleanupStage: String,
  migrated: Boolean
)

class MockLambdaClient extends LambdaClient {

  val requests: ListBuffer[LambdaRequest] = ListBuffer.empty

  def runS3Clean(
    s3KeyPrefix: String,
    publishedDatasetId: Int,
    publishedDatasetVersion: Option[Int],
    publishBucket: String,
    embargoBucket: String,
    cleanupStage: String,
    migrated: Boolean
  )(implicit
    system: ActorSystem,
    ec: ExecutionContext
  ): Future[InvokeResponse] = {
    this.requests += LambdaRequest(
      s3KeyPrefix,
      publishedDatasetId,
      publishedDatasetVersion,
      publishBucket,
      embargoBucket,
      cleanupStage,
      migrated
    )
    Future.successful(InvokeResponse.builder().statusCode(200).build())
  }

}
