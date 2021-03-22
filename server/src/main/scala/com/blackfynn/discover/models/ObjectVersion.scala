// Copyright (c) 2019 Blackfynn, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import io.circe.{ Decoder, Encoder }

/**
  * Key of a versioned object in S3, example: "R6Gl.CtHEEr05ZrQwzSDt_7qcaCAMTKH"
  */
case class ObjectVersion(value: String) extends AnyVal {
  override def toString: String = value
}

object ObjectVersion {
  implicit val encode: Encoder[ObjectVersion] =
    Encoder.encodeString.contramap[ObjectVersion](_.value)

  implicit val decode: Decoder[ObjectVersion] = Decoder.decodeString.emap {
    str =>
      Right(ObjectVersion(str))
  }
}
