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
  totalSize: Long
)

object PublishJobOutput {
  implicit val encoder: Encoder[PublishJobOutput] =
    deriveEncoder[PublishJobOutput]
  implicit val decoder: Decoder[PublishJobOutput] =
    deriveDecoder[PublishJobOutput]
}
