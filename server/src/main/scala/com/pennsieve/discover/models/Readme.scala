// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import io.circe.{ Decoder, Encoder }

case class Readme(value: String) extends AnyVal

object Readme {

  implicit val encodeReadme: Encoder[Readme] =
    Encoder.encodeString.contramap[Readme](_.value)

  implicit val decodeReadme: Decoder[Readme] =
    Decoder.decodeString.emap(value => Right(Readme(value)))
}
