// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import cats.implicits._

import io.circe.{ CursorOp, Decoder, DecodingFailure, Encoder, HCursor, Json }
import io.circe.syntax._
import io.circe.parser.decode

case class EmbargoReleaseJob(
  organizationId: Int,
  datasetId: Int,
  version: Int,
  s3Key: S3Key.Version,
  publishBucket: S3Bucket,
  embargoBucket: S3Bucket
)

object EmbargoReleaseJob {

  def apply(
    publicDataset: PublicDataset,
    version: PublicDatasetVersion,
    publishBucket: S3Bucket,
    embargoBucket: S3Bucket
  ): EmbargoReleaseJob =
    EmbargoReleaseJob(
      publicDataset.sourceOrganizationId,
      publicDataset.sourceDatasetId,
      version.version,
      version.s3Key,
      publishBucket,
      embargoBucket
    )

  /**
    * Same with PublishJob, need custom encoder to convert all fields to strings.
    */
  implicit val encoder: Encoder[EmbargoReleaseJob] =
    Encoder.forProduct6(
      "organization_id",
      "dataset_id",
      "version",
      "s3_key",
      "publish_bucket",
      "embargo_bucket"
    )(
      j =>
        (
          j.organizationId.toString,
          j.datasetId.toString,
          j.version.toString,
          j.s3Key.value,
          j.publishBucket.value,
          j.embargoBucket.value
        )
    )
}
