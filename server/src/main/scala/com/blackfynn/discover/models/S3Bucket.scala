// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import io.circe.{ Decoder, Encoder }

case class S3Bucket(value: String) extends AnyVal {
  override def toString: String = value
}

object S3Bucket {
  implicit val encodeFile: Encoder[S3Bucket] =
    Encoder.encodeString.contramap[S3Bucket](_.value)

  implicit val decodeFile: Decoder[S3Bucket] = Decoder.decodeString.emap {
    str =>
      Right(S3Bucket(str))
  }
}
