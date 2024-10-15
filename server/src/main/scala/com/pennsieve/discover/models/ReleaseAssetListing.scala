// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import enumeratum._
import enumeratum.EnumEntry.Snakecase
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

sealed trait ReleaseAssetFileType extends EnumEntry with Snakecase
object ReleaseAssetFileType
    extends Enum[ReleaseAssetFileType]
    with CirceEnum[ReleaseAssetFileType] {
  val values = findValues
  case object File extends ReleaseAssetFileType
  case object Folder extends ReleaseAssetFileType
}

/**
  * ReleaseAssetFile
  * @param file the full path to the file (src/main/scala/main.scala)
  * @param name the file name (main.scala)
  * @param `type` 'file' or 'folder'
  * @param size the size of the file in bytes
  */
case class ReleaseAssetFile(
  file: String,
  name: String,
  `type`: ReleaseAssetFileType,
  size: Long
)

object ReleaseAssetFile {
  implicit val encoder: Encoder[ReleaseAssetFile] =
    deriveEncoder[ReleaseAssetFile]
  implicit val decoder: Decoder[ReleaseAssetFile] =
    deriveDecoder[ReleaseAssetFile]
}

/**
  * ReleaseAssetListing
  * @param files the list of files in the release asset (Zip file)
  */
case class ReleaseAssetListing(files: Seq[ReleaseAssetFile])

object ReleaseAssetListing {
  implicit val encoder: Encoder[ReleaseAssetListing] =
    deriveEncoder[ReleaseAssetListing]
  implicit val decoder: Decoder[ReleaseAssetListing] =
    deriveDecoder[ReleaseAssetListing]
}
