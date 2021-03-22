// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.stream.ActorMaterializer
import software.amazon.awssdk.services.lambda.model.InvokeResponse

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

class MockLambdaClient extends LambdaClient {

  val s3Keys: ListBuffer[String] =
    ListBuffer.empty[String]

  def runS3Clean(
    s3KeyPrefix: String
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[InvokeResponse] = {
    this.s3Keys += s3KeyPrefix
    Future.successful(InvokeResponse.builder().statusCode(200).build())
  }

}
