// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.discover.models.DOIData.{
  FromExternalDoiData,
  FromPublicDTO,
  FromTombstoneDTO
}
import com.pennsieve.discover.models.DOIInformationSource.Pennsieve
import enumeratum.{ CirceEnum, Enum, EnumEntry }
import io.circe.{ Decoder, Encoder, Json }
import io.circe.generic.semiauto._
import io.circe.syntax.EncoderOps

sealed trait DOIData

object DOIData {
  case class FromPublicDTO(
    value: com.pennsieve.discover.server.definitions.PublicDatasetDto
  ) extends DOIData
  case class FromTombstoneDTO(
    value: com.pennsieve.discover.server.definitions.TombstoneDto
  ) extends DOIData
  case class FromExternalDoiData(
    value: com.pennsieve.discover.server.definitions.ExternalDoiData
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
  // Custom encoder to avoid nested Json DOIData objects
  implicit val encoder: Encoder[DOIInformation] = Encoder.instance { outer =>
    Json.obj("source" -> outer.source.asJson, "data" -> {
      outer.data match {
        case FromPublicDTO(publicDTO) => publicDTO.asJson
        case FromTombstoneDTO(tombstoneDTO) => tombstoneDTO.asJson
        case FromExternalDoiData(externalDTO) => externalDTO.asJson
      }
    })
  }
  implicit val decoder: Decoder[DOIInformation] = Decoder.instance { cursor =>
    for {
      source <- cursor.get[DOIInformationSource]("source")
      data <- source match {
        case DOIInformationSource.Pennsieve =>
          cursor
            .get[com.pennsieve.discover.server.definitions.PublicDatasetDto](
              "data"
            )
            .map(FromPublicDTO)
        case DOIInformationSource.PennsieveUnpublished =>
          cursor
            .get[com.pennsieve.discover.server.definitions.TombstoneDto]("data")
            .map(FromTombstoneDTO)
        case DOIInformationSource.External =>
          cursor
            .get[com.pennsieve.discover.server.definitions.ExternalDoiData](
              "data"
            )
            .map(FromExternalDoiData)
      }
    } yield DOIInformation(source, data)
  }
}
