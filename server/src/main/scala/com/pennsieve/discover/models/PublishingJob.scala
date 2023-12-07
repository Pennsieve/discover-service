// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

case class PublishingJob(
  datasetId: Int,
  version: Int,
  jobType: String,
  executionArn: String,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object PublishingJobType {
  def PUBLISH = "PUBLISH"
  def RELEASE = "RELEASE"
}
