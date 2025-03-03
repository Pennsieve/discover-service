// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.syntax._

/**
  * Metadata produced by the publish job that discover-service needs to know
  * about in order to display correct information.
  *
  * Fargate tasks cannot return output to their caller like a Lambda can, so the
  * publish job writes an "outputs.json" file that is read and then deleted by
  * discover-service
  *
  * A cleaner solution might be to add a Lambda to the Step Function that reads
  * this data and adds it to the PublishNotificatiion.
  */
case class PublishJobOutput(
  readmeKey: S3Key.File,
  bannerKey: S3Key.File,
  changelogKey: S3Key.File,
  totalSize: Long,
  manifestVersion: Option[String] = None
)

object PublishJobOutput {
  implicit val encoder: Encoder[PublishJobOutput] =
    deriveEncoder[PublishJobOutput]
  implicit val decoder: Decoder[PublishJobOutput] =
    deriveDecoder[PublishJobOutput]
}

case class ReleaseAction(
  sourceBucket: String,
  sourceKey: String,
  sourceVersion: String,
  targetBucket: String,
  targetKey: String,
  targetVersion: String
)

object ReleaseAction {
  implicit val encoder: Encoder[ReleaseAction] =
    deriveEncoder[ReleaseAction]
  implicit val decoder: Decoder[ReleaseAction] =
    deriveDecoder[ReleaseAction]
}

case class ReleaseActionV50(
  sourceBucket: String,
  sourceKey: String,
  sourceSize: String,
  sourceVersionId: String,
  sourceEtag: String,
  sourceSha256: String,
  targetBucket: String,
  targetKey: String,
  targetSize: String,
  targetVersionId: String,
  targetEtag: String,
  targetSha256: String
)

object ReleaseActionV50 {
  implicit val encoder: Encoder[ReleaseActionV50] = deriveEncoder[ReleaseActionV50]
  implicit val decoder: Decoder[ReleaseActionV50] = Decoder.forProduct12(
    "source_bucket",
    "source_key",
    "source_size",
    "source_version_id",
    "source_etag",
    "source_sha256",
    "target_bucket",
    "target_key",
    "target_size",
    "target_version_id",
    "target_etag",
    "target_sha256"
  )(ReleaseActionV50.apply)
}
