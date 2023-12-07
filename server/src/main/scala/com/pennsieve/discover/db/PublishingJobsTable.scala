// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.PublishingJob
import slick.dbio.{ DBIOAction, Effect }

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext

final class PublishingJobsTable(tag: Tag)
    extends Table[PublishingJob](tag, "publishing_jobs") {

  def datasetId = column[Int]("dataset_id")
  def version = column[Int]("version")
  def jobType = column[String]("job_type")
  def executionArn = column[String]("execution_arn")
  def createdAt = column[OffsetDateTime]("created_at")

  def * =
    (datasetId, version, jobType, executionArn, createdAt).mapTo[PublishingJob]
}

object PublishingJobsMapper extends TableQuery(new PublishingJobsTable(_)) {
  def create(
    datasetId: Int,
    version: Int,
    jobType: String,
    executionArn: String
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublishingJob, NoStream, Effect.Write with Effect] =
    (this returning this) +=
      PublishingJob(
        datasetId = datasetId,
        version = version,
        jobType = jobType,
        executionArn = executionArn
      )
}
