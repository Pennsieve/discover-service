// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models
import java.time.{ OffsetDateTime, ZoneOffset }

import com.blackfynn.models.License
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.java8.time._

final case class Revision(
  datasetId: Int,
  version: Int,
  revision: Int,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  id: Int = 0
) {
  def s3Key: S3Key.Revision = S3Key.Revision(datasetId, revision)
}

object Revision {
  implicit val encoder: Encoder[Revision] = deriveEncoder[Revision]
  implicit val decoder: Decoder[Revision] = deriveDecoder[Revision]

  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled
}
