// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models
import com.pennsieve.discover.server.definitions.SponsorshipDTO
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

final case class Sponsorship(
  datasetId: Int,
  title: Option[String],
  imageUrl: Option[String],
  markup: Option[String],
  id: Int = 0
) {
  def toDTO: SponsorshipDTO = SponsorshipDTO(title, imageUrl, markup)
}

object Sponsorship {
  implicit val encoder: Encoder[Sponsorship] = deriveEncoder[Sponsorship]
  implicit val decoder: Decoder[Sponsorship] = deriveDecoder[Sponsorship]

  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled
}
