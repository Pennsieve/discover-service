// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import io.circe.{ Decoder, Encoder, Json }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

// this is essentially the same as the FileChecksum in Pennsieve Core Models
final case class FileChecksum(chunkSize: Long, checksum: String)

object FileChecksum {
  // TODO: make these constructors work for real
  def fromString(s: String): FileChecksum = new FileChecksum(0, "")
  def fromJson(j: Json): FileChecksum = new FileChecksum(0, "")
  implicit val encoder: Encoder[FileChecksum] = deriveEncoder[FileChecksum]
  implicit val decoder: Decoder[FileChecksum] = deriveDecoder[FileChecksum]
}
