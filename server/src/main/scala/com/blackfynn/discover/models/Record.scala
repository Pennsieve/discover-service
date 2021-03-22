// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class Record(
  model: String,
  datasetId: Int,
  version: Int,
  organizationName: String,
  properties: Map[String, String]
)

object Record {
  implicit val encoder: Encoder[Record] = deriveEncoder[Record]
  implicit val decoder: Decoder[Record] = deriveDecoder[Record]

  def apply(
    model: String,
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    properties: Map[String, String]
  ): Record =
    Record(
      model = model,
      datasetId = dataset.id,
      version = version.version,
      organizationName = dataset.sourceOrganizationName,
      properties = properties
    )
}
