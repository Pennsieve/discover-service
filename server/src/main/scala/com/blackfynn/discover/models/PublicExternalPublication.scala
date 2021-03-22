// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }
import com.blackfynn.models.RelationshipType

final case class PublicExternalPublication(
  doi: String,
  relationshipType: RelationshipType,
  datasetId: Int,
  version: Int,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object PublicExternalPublication {
  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled
}
