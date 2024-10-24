// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models
import java.time.{ OffsetDateTime, ZoneOffset }
import com.pennsieve.models.{ DatasetType, License }
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

final case class PublicDataset(
  name: String,
  sourceOrganizationId: Int,
  sourceOrganizationName: String,
  sourceDatasetId: Int,
  ownerId: Int,
  ownerFirstName: String,
  ownerLastName: String,
  ownerOrcid: String,
  license: License,
  tags: List[String] = List.empty,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  id: Int = 0,
  datasetType: DatasetType = DatasetType.Research
)

object PublicDataset {
  implicit val encoder: Encoder[PublicDataset] = deriveEncoder[PublicDataset]
  implicit val decoder: Decoder[PublicDataset] = deriveDecoder[PublicDataset]

  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled
}
