// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import com.blackfynn.discover.utils.joinPath
import io.circe.{ Decoder, Encoder, HCursor, Json }

sealed trait S3Key extends Any

object S3Key {

  /**
    * Key of a fully-qualified file in S3.
    */
  case class File(value: String) extends AnyVal with S3Key {
    override def toString: String = value

    // TODO: should this return an Either to capture failures when the string
    // does not start with the version?
    def removeDatasetPrefix(prefix: S3Key.Dataset): String =
      value.stripPrefix(prefix.toString)
  }

  object File {
    implicit val encodeFile: Encoder[File] =
      Encoder.encodeString.contramap[File](_.value)

    implicit val decodeFile: Decoder[File] = Decoder.decodeString.emap { str =>
      Right(File(str))
    }
  }

  /**
    * Base key of a dataset in S3 versioning-enabled, which is versioned/:datasetId
    */
  case class Dataset(value: String) extends AnyVal with S3Key {

    /**
      * Add a suffix to this base key.
      */
    def /(suffix: String): File =
      S3Key.File(joinPath(value, suffix))

    override def toString: String = value
  }

  object Dataset {
    def apply(datasetId: Int): Dataset =
      Dataset(s"versioned/$datasetId/")

    implicit val encodeVersion: Encoder[Dataset] =
      Encoder.encodeString.contramap[Dataset](_.value)

    implicit val decodeVersion: Decoder[Dataset] = Decoder.decodeString.emap {
      str =>
        Right(Dataset(str))
    }
  }

  /**
    * Base key of a revision to a dataset version in S3, which is
    * :datasetId/:version/revisions/:revision
    */
  case class Revision(value: String) extends AnyVal with S3Key {

    /**
      * Add a suffix to this base key.
      */
    def /(suffix: String): File =
      S3Key.File(joinPath(value, suffix))

    override def toString: String = value
  }

  object Revision {
    def apply(datasetId: Int, revision: Int): Revision =
      Revision(s"versioned/$datasetId/revisions/$revision/")

    implicit val encodeRevision: Encoder[Revision] =
      Encoder.encodeString.contramap[Revision](_.value)

    implicit val decodeRevision: Decoder[Revision] = Decoder.decodeString.emap {
      str =>
        Right(Revision(str))
    }
  }
}
