// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import software.amazon.awssdk.services.ecs.model.{ RunTaskResponse, Task }

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

case class S3StorageCleanupTaskRequest(
  datasetId: Int,
  version: Int,
  organizationId: Int,
  publishSuccess: Boolean
)

class MockECSClient extends ECSClient {

  val requests: ListBuffer[S3StorageCleanupTaskRequest] = ListBuffer.empty

  def clear(): Unit = {
    requests.clear()
  }

  override def runS3StorageCleanupTask(
    datasetId: Int,
    version: Int,
    organizationId: Int,
    publishSuccess: Boolean
  )(implicit
    ec: ExecutionContext
  ): Future[RunTaskResponse] = {
    requests += S3StorageCleanupTaskRequest(
      datasetId,
      version,
      organizationId,
      publishSuccess
    )
    Future.successful(
      RunTaskResponse
        .builder()
        .tasks(Task.builder().taskArn("arn:aws:ecs:us-east-1:123456789:task/test-task").build())
        .build()
    )
  }
}
