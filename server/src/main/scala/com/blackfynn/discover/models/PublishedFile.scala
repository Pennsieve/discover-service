// Copyright (c) 2019 Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class PublishedFile(
  s3Key: S3Key.File,
  s3VersionId: Option[String] = None,
  sourceFileId: Option[String] = None
)

object PublishedFile {
  implicit val encoder: Encoder[PublishedFile] = deriveEncoder[PublishedFile]
  implicit val decoder: Decoder[PublishedFile] = deriveDecoder[PublishedFile]

}
