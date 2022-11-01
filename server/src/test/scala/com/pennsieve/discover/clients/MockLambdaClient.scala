// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.actor.ActorSystem
import software.amazon.awssdk.services.lambda.model.InvokeResponse

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

case class LambdaRequest(
  s3KeyPrefix: String,
  publishBucket: String,
  embargoBucket: String
)

class MockLambdaClient extends LambdaClient {

  val requests: ListBuffer[LambdaRequest] = ListBuffer.empty

  def runS3Clean(
    s3KeyPrefix: String,
    publishBucket: String,
    embargoBucket: String
  )(implicit
    system: ActorSystem,
    ec: ExecutionContext
  ): Future[InvokeResponse] = {
    this.requests += LambdaRequest(s3KeyPrefix, publishBucket, embargoBucket)
    Future.successful(InvokeResponse.builder().statusCode(200).build())
  }

}
