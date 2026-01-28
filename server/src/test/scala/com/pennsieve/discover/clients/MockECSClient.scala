// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import software.amazon.awssdk.services.ecs.model.{ RunTaskResponse, Task }

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

case class S3StorageCleanupTaskRequest(
  sourceDatasetId: Int,
  publicDatasetId: Int,
  version: Int,
  organizationId: Int,
  publishSuccess: Boolean,
  s3Bucket: String,
  s3Key: String
)

class MockECSClient extends ECSClient {

  val requests: ListBuffer[S3StorageCleanupTaskRequest] = ListBuffer.empty

  def clear(): Unit = {
    requests.clear()
  }

  override def runS3StorageCleanupTask(
    sourceDatasetId: Int,
    publicDatasetId: Int,
    version: Int,
    organizationId: Int,
    publishSuccess: Boolean,
    s3Bucket: String,
    s3Key: String
  )(implicit
    ec: ExecutionContext
  ): Future[RunTaskResponse] = {
    requests += S3StorageCleanupTaskRequest(
      sourceDatasetId,
      publicDatasetId,
      version,
      organizationId,
      publishSuccess,
      s3Bucket,
      s3Key
    )
    Future.successful(
      RunTaskResponse
        .builder()
        .tasks(Task.builder().taskArn("arn:aws:ecs:us-east-1:123456789:task/test-task").build())
        .build()
    )
  }
}
