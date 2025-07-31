// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto._
import enumeratum.{ CirceEnum, Enum, EnumEntry }

sealed trait DOIData

object DOIData {
  case class FromPublicDTO(
    value: com.pennsieve.discover.client.definitions.PublicDatasetDto
  ) extends DOIData
  case class FromTombstoneDTO(
    value: com.pennsieve.discover.client.definitions.TombstoneDto
  ) extends DOIData
  case class FromExternalDoiData(
    value: com.pennsieve.discover.client.definitions.ExternalDoiData
  ) extends DOIData

  implicit val encoder: Encoder[DOIData] = deriveEncoder
  implicit val decoder: Decoder[DOIData] = deriveDecoder
}

sealed trait DOIInformationSource extends EnumEntry

object DOIInformationSource
    extends Enum[DOIInformationSource]
    with CirceEnum[DOIInformationSource] {
  val values: IndexedSeq[DOIInformationSource] = findValues
  case object Pennsieve extends DOIInformationSource
  case object PennsieveUnpublished extends DOIInformationSource
  case object External extends DOIInformationSource
}

// Defining this ourselves instead of letting Guardrail create it because the
// version we are using cannot handle polymorphism via OpenAPI oneOf
case class DOIInformation(source: DOIInformationSource, data: DOIData)

object DOIInformation {
  implicit val encoder: Encoder[DOIInformation] = deriveEncoder
  implicit val decoder: Decoder[DOIInformation] = deriveDecoder
}
