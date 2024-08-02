// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.utils

import com.pennsieve.discover.Ports
import com.pennsieve.discover.models.S3Bucket
import com.pennsieve.discover.server.definitions.BucketConfig

object BucketResolver {
  def apply(ports: Ports): BucketResolver = new BucketResolver(ports)
}

class BucketResolver(ports: Ports) {

  val defaultPublishBucket = ports.config.s3.publishBucket
  val defaultEmbargoBucket = ports.config.s3.embargoBucket
  val defaultPublish50Bucket = ports.config.s3.publish50Bucket
  val defaultEmbargo50Bucket = ports.config.s3.embargo50Bucket

  private def getDefaultBucket(
    workflowId: Option[Long],
    bucket4: S3Bucket,
    bucket5: S3Bucket
  ) =
    workflowId match {
      case Some(workflowId) =>
        workflowId match {
          case 4 => bucket4
          case _ => bucket5
        }
      case None => bucket5
    }

  def resolveBucketConfig(
    bucketConfig: Option[BucketConfig],
    workflowId: Option[Long]
  ): (S3Bucket, S3Bucket) = {
    (
      bucketConfig
        .map(c => S3Bucket(c.publish))
        .getOrElse(
          getDefaultBucket(
            workflowId,
            defaultPublishBucket,
            defaultPublish50Bucket
          )
        ),
      bucketConfig
        .map(c => S3Bucket(c.embargo))
        .getOrElse(
          getDefaultBucket(
            workflowId,
            defaultEmbargoBucket,
            defaultEmbargo50Bucket
          )
        )
    )
  }
}
