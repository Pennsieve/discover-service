// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import com.pennsieve.discover.PublishJobException
import com.pennsieve.discover.models._
import com.pennsieve.discover.TestUtilities

import scala.collection.mutable.ListBuffer
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse

import scala.concurrent.{ ExecutionContext, Future }

class MockStepFunctionsClient extends StepFunctionsClient {
  val startedJobs = ListBuffer.empty[PublishJob]

  def startPublish(
    job: PublishJob
  )(implicit
    ec: ExecutionContext
  ): Future[StartExecutionResponse] = {
    this.startedJobs += job
    Future.successful(
      StartExecutionResponse
        .builder()
        .executionArn(TestUtilities.randomString())
        .build()
    )
  }

  val startedReleaseJobs = ListBuffer.empty[EmbargoReleaseJob]

  def startRelease(
    job: EmbargoReleaseJob
  )(implicit
    ec: ExecutionContext
  ): Future[StartExecutionResponse] = {
    this.startedReleaseJobs += job
    Future.successful(
      StartExecutionResponse
        .builder()
        .executionArn(TestUtilities.randomString())
        .build()
    )
  }

  def clear(): Unit = {
    this.startedJobs.clear()
    this.startedReleaseJobs.clear()
  }
}
